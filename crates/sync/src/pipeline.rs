//! Bounded producer tasks supervised by the ordered ingestion coordinator.

use crate::{
    bulk,
    config::{RAW_PER_DOCUMENT, RsyncMode},
    ingest::Scope,
    metrics::Stage,
    parse::{Budget, BudgetExceeded, DOCUMENT_BYTES, OwnedText},
    upstream::{Capture, Download, RsyncBatch, Source, Upstream},
    xml::{self, XmlReferences},
};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter},
    postgres::Result,
};
use std::{io::Read, sync::Arc, time::Instant};
use tokio::{
    sync::{Semaphore, watch},
    task::{self, JoinSet},
};

#[derive(Clone)]
pub(crate) struct Shutdown(watch::Sender<bool>);

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self(watch::channel(false).0)
    }

    pub(crate) fn stop(&self) {
        self.0.send_replace(true);
    }

    pub(crate) fn check(&self) -> Result<()> {
        if *self.0.borrow() {
            Err("reconciliation cancelled".into())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn cancelled(&self) {
        let _ = self.0.subscribe().wait_for(|cancelled| *cancelled).await;
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PriorTxt {
    pub digest: BlobDigest,
    pub observation: BlobDigest,
    pub rsync_only: bool,
}

pub(crate) struct Prepared {
    pub bytes: Arc<Vec<u8>>,
    pub digest: BlobDigest,
    pub txt_evidence: TxtEvidence,
    pub xml: Result<Download>,
    pub queued_at: Instant,
    pub parsed: Option<Parsed>,
}

pub(crate) struct Parsed {
    pub txt: OwnedText,
    pub references: Option<Result<XmlReferences>>,
}

pub(crate) enum TxtEvidence {
    Prior(PriorTxt),
    HttpsMatch(Capture),
    Rsync(bulk::Proof),
    Mismatch(Capture),
}

pub(crate) enum Event {
    Staged(Result<StagedTxt>),
    Document(RfcNumber, Result<Box<Prepared>>),
}

pub(crate) enum StagedTxt {
    Batch(RsyncBatch),
    Bulk(bulk::Snapshot),
}

pub(crate) struct StagedBody {
    bytes: Vec<u8>,
    rsync: Option<bulk::Proof>,
}

impl StagedTxt {
    pub(crate) fn take(&self, store: &BlobWriter, id: RfcNumber) -> Result<StagedBody> {
        match self {
            Self::Batch(batch) => Ok(StagedBody {
                bytes: batch.take(store, id)?,
                rsync: None,
            }),
            Self::Bulk(snapshot) => {
                let (bytes, rsync) = snapshot.take(store, id)?;
                Ok(StagedBody {
                    bytes,
                    rsync: Some(rsync),
                })
            }
        }
    }

    pub(crate) fn close(self) -> Result<()> {
        match self {
            Self::Batch(batch) => batch.close(),
            Self::Bulk(_) => Ok(()),
        }
    }
}

pub(crate) struct Pipeline {
    tasks: JoinSet<Event>,
    upstream: Upstream,
    shutdown: Shutdown,
    parse_slots: Arc<Semaphore>,
    parse_bytes: usize,
}

impl Pipeline {
    pub(crate) fn new(upstream: Upstream) -> Result<Self> {
        Ok(Self {
            tasks: JoinSet::new(),
            shutdown: Shutdown::new(),
            parse_slots: Arc::new(Semaphore::new(
                upstream.config().ingestion.pipeline.parser_workers,
            )),
            parse_bytes: parse_bytes(&upstream)?,
            upstream,
        })
    }

    pub(crate) fn stage(&mut self, scope: Scope, ids: Vec<RfcNumber>, accepted_remaining: u64) {
        let upstream = self.upstream.clone();
        let shutdown = self.shutdown.clone();
        self.tasks.spawn(async move {
            let result = match upstream.config().ingestion.rsync.mode {
                RsyncMode::Batched => upstream.stage(&ids, &shutdown).await.map(StagedTxt::Batch),
                RsyncMode::PersistentBulk => {
                    bulk::stage(&upstream, scope, &ids, accepted_remaining, &shutdown)
                        .await
                        .map(StagedTxt::Bulk)
                }
            };
            Event::Staged(result)
        });
    }

    pub(crate) fn prepare(
        &mut self,
        id: RfcNumber,
        body: StagedBody,
        prior: Option<PriorTxt>,
        rsync_bootstrap: bool,
        reader: Arc<BlobReader>,
        admitted: usize,
    ) {
        let upstream = self.upstream.clone();
        let shutdown = self.shutdown.clone();
        let metrics = upstream.metrics();
        metrics.admitted(admitted, admitted * RAW_PER_DOCUMENT);
        let slots = self.parse_slots.clone();
        let parse_bytes = self.parse_bytes;
        self.tasks.spawn(async move {
            let result = async {
                let mut prepared = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => Err("document verification cancelled".into()),
                    result = prepare_document(&upstream, id, body, prior, rsync_bootstrap, &reader) => result,
                }?;
                if !matches!(prepared.txt_evidence, TxtEvidence::Mismatch(_)) {
                    let queue = metrics.span(Stage::Queue);
                    let slot = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Err("document parsing cancelled".into()),
                        result = slots.acquire_owned() => result?,
                    };
                    drop(queue);
                    // Keep the join handle until completion. Dropping it would
                    // detach a running blocking parser during graceful shutdown.
                    prepared = task::spawn_blocking(move || {
                        let _slot = slot;
                        let _parse = metrics.parse_start();
                        let budget = Budget {
                            bytes: parse_bytes,
                            shutdown: &shutdown,
                        };
                        let txt = OwnedText::parse(&prepared.bytes, &budget)?;
                        let references = if let Ok(Download::Captured(capture)) = &prepared.xml {
                            let budget = Budget {
                                bytes: budget.bytes - txt.data.retained_bytes,
                                ..budget
                            };
                            let result = xml::references(&capture.bytes, id, &budget);
                            budget.shutdown.check()?;
                            if result
                                .as_ref()
                                .is_err_and(|error| error.is::<BudgetExceeded>())
                            {
                                return Err(BudgetExceeded.into());
                            }
                            Some(result)
                        } else {
                            None
                        };
                        prepared.parsed = Some(Parsed { txt, references });
                        prepared.queued_at = Instant::now();
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(prepared)
                    })
                    .await??;
                }
                Ok(prepared)
            }
            .await;
            Event::Document(id, result.map(Box::new))
        });
    }

    pub(crate) async fn next(&mut self) -> Result<Event> {
        Ok(self
            .tasks
            .join_next()
            .await
            .ok_or("ingestion pipeline stalled")??)
    }

    pub(crate) async fn drain(&mut self) {
        self.shutdown.stop();
        while self.tasks.join_next().await.is_some() {}
    }
}

async fn prepare_document(
    upstream: &Upstream,
    id: RfcNumber,
    body: StagedBody,
    prior: Option<PriorTxt>,
    rsync_bootstrap: bool,
    reader: &BlobReader,
) -> Result<Prepared> {
    let StagedBody { bytes, rsync } = body;
    let digest = BlobDigest::hash(&bytes);
    let txt_evidence = if rsync_bootstrap {
        TxtEvidence::Rsync(rsync.ok_or("rsync bootstrap requires a completed bulk snapshot")?)
    } else if let Some(prior) = prior.filter(|prior| prior.digest == digest)
        && revalidate(reader, &bytes, digest)?
    {
        TxtEvidence::Prior(prior)
    } else {
        let Download::Captured(mut capture) = upstream.fetch(Source::Txt(id)).await? else {
            return Err("required HTTPS TXT is unavailable".into());
        };
        if bytes.as_slice() != capture.bytes.as_slice() {
            return Ok(Prepared {
                bytes: Arc::new(bytes),
                digest,
                txt_evidence: TxtEvidence::Mismatch(capture),
                xml: Ok(Download::Unavailable),
                queued_at: Instant::now(),
                parsed: None,
            });
        }
        capture.bytes = Arc::default();
        TxtEvidence::HttpsMatch(capture)
    };
    let xml = upstream.fetch(Source::Xml(id)).await;
    Ok(Prepared {
        bytes: Arc::new(bytes),
        digest,
        txt_evidence,
        xml,
        queued_at: Instant::now(),
        parsed: None,
    })
}

fn parse_bytes(_upstream: &Upstream) -> Result<usize> {
    #[cfg(feature = "phase2-fixtures")]
    if _upstream.is_fixture()
        && let Ok(value) = std::env::var("RFC_PHASE2_PARSE_BUDGET")
    {
        let bytes: usize = value.parse()?;
        if (1..=DOCUMENT_BYTES).contains(&bytes) {
            return Ok(bytes);
        }
        return Err("invalid fixture parse budget".into());
    }
    Ok(DOCUMENT_BYTES)
}

// The staged bytes already hash to the observation's digest. Compare the entire
// canonical file through a fixed buffer, including its length, before reuse.
fn revalidate(reader: &BlobReader, bytes: &[u8], digest: BlobDigest) -> Result<bool> {
    let mut file = match reader.open_blob(digest) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if file.metadata()?.len() != bytes.len() as u64 {
        return Err("canonical TXT differs from verified baseline".into());
    }
    let mut buffer = [0; 16 * 1024];
    for slice in bytes.chunks(buffer.len()) {
        let read = &mut buffer[..slice.len()];
        file.read_exact(read)?;
        if read != slice {
            return Err("canonical TXT differs from verified baseline".into());
        }
    }
    if file.read(&mut buffer[..1])? != 0 {
        return Err("canonical TXT differs from verified baseline".into());
    }
    Ok(true)
}
