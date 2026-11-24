//! Bounded producer tasks supervised by the ordered ingestion coordinator.

use crate::{
    bulk,
    config::{RAW_PER_DOCUMENT, RsyncMode},
    ingest::Scope,
    metrics::{Metrics, Stage},
    parse::{Budget, BudgetExceeded, DOCUMENT_BYTES, OwnedText},
    upstream::{Capture, Download, MissingXml, RsyncBatch, Source, Upstream, XmlFailure},
    xml::{self, XmlReferences},
};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter},
    postgres::Result,
};
use std::{collections::VecDeque, io::Read, sync::Arc, time::Instant};
use tokio::{
    sync::{Semaphore, oneshot, watch},
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
    pub queued_at: Instant,
    pub parsed: Option<Parsed>,
}

pub(crate) struct Parsed {
    pub txt: OwnedText,
    pub xml: PreparedXml,
}

pub(crate) enum PreparedXml {
    Captured(Capture, XmlReferences),
    Malformed(Capture, Box<dyn std::error::Error + Send + Sync>),
    Missing(MissingXml),
    Failed(XmlFailure),
}

pub(crate) enum TxtEvidence {
    Prior(PriorTxt),
    HttpsMatch(Capture),
    Rsync(bulk::Proof),
    Mismatch(Capture),
}

pub(crate) enum Event {
    Staged(Result<StagedTxt>),
    XmlFetched,
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
    xml_tasks: JoinSet<()>,
    upstream: Upstream,
    shutdown: Shutdown,
    parser: Parser,
    xml_enqueued: usize,
    xml_waiting: VecDeque<(RfcNumber, oneshot::Receiver<XmlFetch>)>,
}

type XmlFetch = std::result::Result<Download, XmlFailure>;

#[derive(Clone)]
struct Parser {
    slots: Arc<Semaphore>,
    bytes: usize,
    shutdown: Shutdown,
    metrics: Metrics,
}

impl Parser {
    async fn parse<T: Send + 'static>(
        &self,
        parse: impl FnOnce(&Budget<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let queue = self.metrics.span(Stage::Queue);
        let slot = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return Err("document parsing cancelled".into()),
            result = self.slots.clone().acquire_owned() => result?,
        };
        drop(queue);
        let parser = self.clone();
        // Await even during shutdown so a blocking parser cannot outlive the fence.
        task::spawn_blocking(move || {
            let _slot = slot;
            let _parse = parser.metrics.parse_start();
            let budget = Budget {
                bytes: parser.bytes,
                shutdown: &parser.shutdown,
            };
            parse(&budget)
        })
        .await?
    }
}

impl Pipeline {
    pub(crate) fn new(upstream: Upstream) -> Result<Self> {
        let shutdown = Shutdown::new();
        Ok(Self {
            tasks: JoinSet::new(),
            xml_tasks: JoinSet::new(),
            parser: Parser {
                slots: Arc::new(Semaphore::new(
                    upstream.config().ingestion.pipeline.parser_workers,
                )),
                bytes: parse_bytes(&upstream)?,
                shutdown: shutdown.clone(),
                metrics: upstream.metrics(),
            },
            upstream,
            shutdown,
            xml_enqueued: 0,
            xml_waiting: VecDeque::new(),
        })
    }

    pub(crate) fn prefetch_xml(&mut self, ids: impl Iterator<Item = RfcNumber>, committed: usize) {
        let limit = committed
            + self
                .upstream
                .config()
                .ingestion
                .pipeline
                .max_pending_documents;
        for id in ids.skip(self.xml_enqueued).take(limit - self.xml_enqueued) {
            let (send, receive) = oneshot::channel();
            self.xml_waiting.push_back((id, receive));
            self.xml_enqueued += 1;
            let upstream = self.upstream.clone();
            let shutdown = self.shutdown.clone();
            self.xml_tasks.spawn(async move {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {}
                    result = upstream.fetch_xml(id) => { let _ = send.send(result); }
                }
            });
        }
        let reserved = self.xml_enqueued - committed;
        self.upstream
            .metrics()
            .xml_reserved(reserved, reserved * RAW_PER_DOCUMENT);
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
    ) -> Result<()> {
        let (_, xml_fetch) = self
            .xml_waiting
            .pop_front()
            .filter(|(scheduled, _)| *scheduled == id)
            .ok_or("missing scheduled RFCXML job")?;
        let upstream = self.upstream.clone();
        let shutdown = self.shutdown.clone();
        let parser = self.parser.clone();
        upstream.metrics().admitted(admitted);
        self.tasks.spawn(async move {
            let result = async {
                let mut prepared = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => Err("document verification cancelled".into()),
                    result = prepare_document(&upstream, id, body, prior, rsync_bootstrap, &reader) => result,
                }?;
                if !matches!(prepared.txt_evidence, TxtEvidence::Mismatch(_)) {
                    let bytes = prepared.bytes.clone();
                    let txt = parser.parse(move |budget| OwnedText::parse(&bytes, budget)).await?;
                    let retained = txt.data.retained_bytes;
                    #[cfg(feature = "phase2-fixtures")]
                    if upstream.is_fixture()
                        && xml_fetch.is_empty()
                        && std::env::var("RFC_PHASE2_TRACE_TXT_PARSE").as_deref() == Ok("1")
                    {
                        eprintln!("fixture TXT parsed before XML for RFC {}", id.get());
                    }
                    let fetched = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return Err("RFCXML join cancelled".into()),
                        result = xml_fetch => result?,
                    };
                    let xml = match fetched {
                        Ok(Download::Captured(capture)) => parser.parse(move |budget| {
                            let budget = Budget {
                                bytes: budget.bytes.checked_sub(retained).ok_or(BudgetExceeded)?,
                                shutdown: budget.shutdown,
                            };
                            let result = xml::references(&capture.bytes, id, &budget);
                            budget.shutdown.check()?;
                            match result {
                                Ok(references) => Ok(PreparedXml::Captured(capture, references)),
                                Err(error) if error.is::<BudgetExceeded>() => Err(error),
                                Err(error) => Ok(PreparedXml::Malformed(capture, error)),
                            }
                        }).await?,
                        Ok(Download::Missing(status)) => PreparedXml::Missing(status),
                        Err(failure) => PreparedXml::Failed(failure),
                    };
                    prepared.parsed = Some(Parsed { txt, xml });
                    prepared.queued_at = Instant::now();
                }
                Ok(prepared)
            }
            .await;
            Event::Document(id, result.map(Box::new))
        });
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Result<Event> {
        tokio::select! {
            biased;
            Some(result) = self.xml_tasks.join_next() => {
                result?;
                Ok(Event::XmlFetched)
            }
            result = self.tasks.join_next(), if !self.tasks.is_empty() => Ok(
                result.ok_or("ingestion pipeline stalled")??
            ),
            else => Err("ingestion pipeline stalled".into()),
        }
    }

    pub(crate) async fn drain(&mut self) {
        self.shutdown.stop();
        while self.tasks.join_next().await.is_some() {}
        while self.xml_tasks.join_next().await.is_some() {}
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
                queued_at: Instant::now(),
                parsed: None,
            });
        }
        capture.bytes = Arc::default();
        TxtEvidence::HttpsMatch(capture)
    };
    Ok(Prepared {
        bytes: Arc::new(bytes),
        digest,
        txt_evidence,
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
