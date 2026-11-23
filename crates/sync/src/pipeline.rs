//! Bounded producer tasks supervised by the ordered ingestion coordinator.

use crate::upstream::{
    Capture, Download, RsyncBatch, Source, TXT_LIMIT, Upstream, XML_LIMIT, concurrency,
};
use crate::{
    metrics::Stage,
    parse::{Budget, BudgetExceeded, DOCUMENT_BYTES, OwnedText},
    xml::{self, XmlReferences},
};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::{blob::BlobReader, postgres::Result};
use std::{io::Read, sync::Arc, time::Instant};
use tokio::{
    sync::{Semaphore, watch},
    task::{self, JoinSet},
};

pub(crate) const DOCUMENT_SLOTS: usize = 3;
const RAW_PER_DOCUMENT: usize = 2 * TXT_LIMIT + XML_LIMIT;

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
}

pub(crate) struct Prepared {
    pub bytes: Arc<Vec<u8>>,
    pub digest: BlobDigest,
    pub verified: VerifiedTxt,
    pub xml: Result<Download>,
    pub queued_at: Instant,
    pub parsed: Option<Parsed>,
}

pub(crate) struct Parsed {
    pub txt: OwnedText,
    pub references: Option<Result<XmlReferences>>,
}

pub(crate) enum VerifiedTxt {
    Prior(BlobDigest),
    Fresh(Capture),
    Mismatch(Capture),
}

pub(crate) enum Event {
    Staged(Result<RsyncBatch>),
    Document(RfcNumber, Result<Box<Prepared>>),
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
            parse_slots: Arc::new(Semaphore::new(concurrency(
                "RFC_SYNC_PARSE_CONCURRENCY",
                1,
            )?)),
            parse_bytes: parse_bytes(&upstream)?,
            upstream,
        })
    }

    pub(crate) fn stage(&mut self, ids: Vec<RfcNumber>) {
        let upstream = self.upstream.clone();
        let shutdown = self.shutdown.clone();
        self.tasks
            .spawn(async move { Event::Staged(upstream.stage(&ids, &shutdown).await) });
    }

    pub(crate) fn prepare(
        &mut self,
        id: RfcNumber,
        bytes: Vec<u8>,
        prior: Option<PriorTxt>,
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
                    result = verify(&upstream, id, bytes, prior, &reader) => result,
                }?;
                if !matches!(prepared.verified, VerifiedTxt::Mismatch(_)) {
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

async fn verify(
    upstream: &Upstream,
    id: RfcNumber,
    bytes: Vec<u8>,
    prior: Option<PriorTxt>,
    reader: &BlobReader,
) -> Result<Prepared> {
    let digest = BlobDigest::hash(&bytes);
    let prior = match prior.filter(|prior| prior.digest == digest) {
        Some(prior) if revalidate(reader, &bytes, digest)? => Some(prior),
        _ => None,
    };
    let verified = if let Some(prior) = prior {
        VerifiedTxt::Prior(prior.observation)
    } else {
        let Download::Captured(mut capture) = upstream.fetch(Source::Txt(id)).await? else {
            return Err("required HTTPS TXT is unavailable".into());
        };
        if bytes.as_slice() != capture.bytes.as_slice() {
            return Ok(Prepared {
                bytes: Arc::new(bytes),
                digest,
                verified: VerifiedTxt::Mismatch(capture),
                xml: Ok(Download::Unavailable),
                queued_at: Instant::now(),
                parsed: None,
            });
        }
        capture.bytes = Arc::default();
        VerifiedTxt::Fresh(capture)
    };
    let xml = upstream.fetch(Source::Xml(id)).await;
    Ok(Prepared {
        bytes: Arc::new(bytes),
        digest,
        verified,
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
