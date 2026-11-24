//! Bounded producer tasks supervised by the ingestion coordinator.

use crate::{
    bulk,
    checkpoint::{Kind, Saved, SavedSources},
    config::{RAW_PER_DOCUMENT, RsyncMode},
    ingest::Scope,
    metrics::{Metrics, Stage},
    parse::{Budget, BudgetExceeded, DOCUMENT_BYTES, OwnedText},
    upstream::{Capture, Download, MissingXml, RsyncBatch, Source, Upstream, XmlFailure},
    xml::{self, XmlReferences},
};
use rfc_core::{BlobDigest, GenerationId, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter},
    postgres::Result,
};
use std::{collections::VecDeque, io::Read, sync::Arc, time::Instant};
use tokio::{
    sync::{Semaphore, mpsc, oneshot, watch},
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

    pub(crate) async fn until<T>(
        &self,
        deadline: tokio::time::Instant,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.cancelled() => Err("reconciliation cancelled".into()),
            _ = tokio::time::sleep_until(deadline) => Err("reconciliation deadline exceeded".into()),
            result = work => result,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PriorTxt {
    pub digest: BlobDigest,
    pub observation: BlobDigest,
    pub rsync_only: bool,
    pub legacy: bool,
}

pub(crate) struct TxtVerification {
    pub baseline: Option<PriorTxt>,
    pub checkpoint: Option<Saved>,
    pub bootstrap: bool,
}

pub(crate) struct Prepared {
    pub bytes: Arc<Vec<u8>>,
    pub digest: BlobDigest,
    pub txt_evidence: TxtEvidence,
    pub queued_at: Instant,
    pub parsed: Option<OwnedText>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Selection {
    pub generation: GenerationId,
    pub catalog: BlobDigest,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkId {
    pub selection: Selection,
    pub rfc: RfcNumber,
}

pub(crate) enum PreparedXml {
    Captured(Capture, XmlReferences),
    Reused(Saved, XmlReferences),
    Malformed(Capture, Box<dyn std::error::Error + Send + Sync>),
    Missing(MissingXml),
    Failed(XmlFailure),
}

pub(crate) enum TxtEvidence {
    Prior(PriorTxt),
    Checkpoint(Saved),
    HttpsMatch(Capture, bulk::Proof),
    Rsync(bulk::Proof),
    Mismatch(Capture),
}

pub(crate) enum Event {
    Staged(Result<StagedTxt>),
    Txt(WorkId, Box<Prepared>),
    Xml(WorkId, PreparedXml),
    Invalidated(WorkId, Kind),
    Failed(WorkId, Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) enum StagedTxt {
    Batch(RsyncBatch),
    Bulk(bulk::Snapshot),
}

pub(crate) struct StagedBody {
    bytes: Vec<u8>,
    rsync: bulk::Proof,
}

impl StagedTxt {
    pub(crate) fn take(&self, store: &BlobWriter, id: RfcNumber) -> Result<StagedBody> {
        match self {
            Self::Batch(batch) => Ok(StagedBody {
                bytes: batch.take(store, id)?,
                rsync: bulk::Proof {
                    observed_us: batch.observed_us,
                    fetched_us: Some(batch.observed_us),
                },
            }),
            Self::Bulk(snapshot) => {
                let (bytes, rsync) = snapshot.take(store, id)?;
                Ok(StagedBody { bytes, rsync })
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
    tasks: JoinSet<Option<Event>>,
    results: mpsc::Receiver<Event>,
    send_result: mpsc::Sender<Event>,
    upstream: Upstream,
    shutdown: Shutdown,
    parser: Parser,
    xml_enqueued: usize,
    xml_waiting: VecDeque<(WorkId, oneshot::Receiver<XmlFetch>)>,
}

enum XmlInput {
    Upstream(std::result::Result<Download, XmlFailure>),
    Saved(Saved, Vec<u8>),
}

type XmlFetch = Result<XmlInput>;

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
        let (send_result, results) =
            mpsc::channel(2 * upstream.config().ingestion.pipeline.max_pending_documents);
        Ok(Self {
            tasks: JoinSet::new(),
            results,
            send_result,
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

    pub(crate) fn prefetch_xml(
        &mut self,
        selection: Selection,
        ordered: &[RfcNumber],
        committed: usize,
        checkpoints: &SavedSources,
        reader: &Arc<BlobReader>,
    ) {
        let capacity = self
            .upstream
            .config()
            .ingestion
            .pipeline
            .max_pending_documents;
        let limit = (committed + capacity).min(ordered.len());
        for &rfc in &ordered[self.xml_enqueued..limit] {
            let id = WorkId { selection, rfc };
            let (send, receive) = oneshot::channel();
            self.xml_waiting.push_back((id, receive));
            self.xml_enqueued += 1;
            let upstream = self.upstream.clone();
            let shutdown = self.shutdown.clone();
            let events = self.send_result.clone();
            let cached = checkpoints.xml.get(&rfc).copied();
            let reader = reader.clone();
            self.tasks.spawn(async move {
                let result = async {
                    if let Some(saved) = cached {
                        let body = task::spawn_blocking(move || saved.read_xml(&reader))
                            .await?
                            .map_err(|error| Kind::Xml.invalid(saved, error))?;
                        if let Some(bytes) = body {
                            return Ok(XmlInput::Saved(saved, bytes));
                        }
                        events.send(Event::Invalidated(id, Kind::Xml)).await?;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => Err("RFCXML acquisition cancelled".into()),
                        result = upstream.fetch_xml(id.rfc) => Ok(XmlInput::Upstream(result)),
                    }
                }
                .await;
                let _ = send.send(result);
                None
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
            Some(Event::Staged(result))
        });
    }

    pub(crate) fn prepare(
        &mut self,
        id: WorkId,
        body: StagedBody,
        verification: TxtVerification,
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
        let send = self.send_result.clone();
        upstream.metrics().admitted(admitted);
        self.tasks.spawn(async move {
            let result = async {
                let mut prepared = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => Err("document verification cancelled".into()),
                    result = verify_txt(&upstream, id, body, verification, &reader, &send) => result,
                }?;
                if matches!(prepared.txt_evidence, TxtEvidence::Mismatch(_)) {
                    send.send(Event::Txt(id, Box::new(prepared))).await?;
                    return Ok(());
                }
                let bytes = prepared.bytes.clone();
                let txt = parser.parse(move |budget| OwnedText::parse(&bytes, budget)).await?;
                let retained = txt.data.retained_bytes;
                prepared.parsed = Some(txt);
                prepared.queued_at = Instant::now();
                send.send(Event::Txt(id, Box::new(prepared))).await?;

                // Delivery of TXT precedes XML for this key. The CPU slot and
                // the TXT payload have both been handed back before the wait.
                let fetched = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Err("RFCXML join cancelled".into()),
                    result = xml_fetch => result??,
                };
                let fetched = match fetched {
                    XmlInput::Saved(saved, _) if !saved.fresh()? => {
                        send.send(Event::Invalidated(id, Kind::Xml)).await?;
                        let result = tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => return Err("RFCXML acquisition cancelled".into()),
                            result = upstream.fetch_xml(id.rfc) => result,
                        };
                        XmlInput::Upstream(result)
                    }
                    input => input,
                };
                let xml = match fetched {
                    XmlInput::Upstream(Ok(Download::Missing(status))) => PreparedXml::Missing(status),
                    XmlInput::Upstream(Err(failure)) => PreparedXml::Failed(failure),
                    body => parser.parse(move |budget| {
                        let budget = Budget {
                            bytes: budget.bytes.checked_sub(retained).ok_or(BudgetExceeded)?,
                            shutdown: budget.shutdown,
                        };
                        #[cfg(feature = "phase2-fixtures")]
                        if upstream.is_fixture()
                            && std::env::var("RFC_PHASE2_PANIC_XML")
                                .ok()
                                .and_then(|value| value.parse().ok()) == Some(id.rfc.get())
                        {
                            panic!("fixture RFCXML parser panic");
                        }
                        let bytes = match &body {
                            XmlInput::Upstream(Ok(Download::Captured(capture))) => capture.bytes.as_slice(),
                            XmlInput::Saved(_, bytes) => bytes.as_slice(),
                            _ => unreachable!("only captured RFCXML bodies reach the parser"),
                        };
                        let result = xml::references(bytes, id.rfc, &budget);
                        budget.shutdown.check()?;
                        match (body, result) {
                            (_, Err(error)) if error.is::<BudgetExceeded>() => Err(error),
                            (XmlInput::Upstream(Ok(Download::Captured(capture))), Ok(references)) =>
                                Ok(PreparedXml::Captured(capture, references)),
                            (XmlInput::Upstream(Ok(Download::Captured(capture))), Err(error)) =>
                                Ok(PreparedXml::Malformed(capture, error)),
                            (XmlInput::Saved(saved, _), Ok(references)) => Ok(PreparedXml::Reused(saved, references)),
                            (XmlInput::Saved(saved, _), Err(error)) => Err(Kind::Xml.invalid(saved, error).into()),
                            _ => unreachable!("only captured RFCXML bodies reach the parser"),
                        }
                    }).await?,
                };
                send.send(Event::Xml(id, xml)).await?;
                Ok(())
            }
            .await;
            result.err().map(|error| Event::Failed(id, error))
        });
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Result<Event> {
        loop {
            if self.tasks.is_empty() && self.results.is_empty() {
                return Err("ingestion pipeline stalled".into());
            }
            tokio::select! {
                biased;
                Some(result) = self.tasks.join_next() => {
                    if let Some(event) = result? {
                        return Ok(event);
                    }
                }
                Some(event) = self.results.recv() => return Ok(event),
            }
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<()> {
        while let Some(result) = self.tasks.join_next().await {
            if result?.is_some() {
                return Err("unexpected task result after ingestion".into());
            }
        }
        if !self.results.is_empty() || !self.xml_waiting.is_empty() {
            return Err("unexpected document result after ingestion".into());
        }
        Ok(())
    }

    pub(crate) async fn drain(&mut self) {
        self.shutdown.stop();
        self.results.close();
        self.xml_waiting.clear();
        while self.tasks.join_next().await.is_some() {}
        while self.results.try_recv().is_ok() {}
    }
}

async fn verify_txt(
    upstream: &Upstream,
    id: WorkId,
    body: StagedBody,
    verification: TxtVerification,
    reader: &BlobReader,
    events: &mpsc::Sender<Event>,
) -> Result<Prepared> {
    let StagedBody { bytes, rsync } = body;
    let digest = BlobDigest::hash(&bytes);
    let prior = if let Some(prior) = verification
        .baseline
        .filter(|prior| !verification.bootstrap && prior.digest == digest)
        && revalidate(reader, &bytes, digest)?
    {
        Some(TxtEvidence::Prior(prior))
    } else {
        None
    };
    let mut evidence = prior;
    if let Some(saved) = verification
        .checkpoint
        .filter(|saved| evidence.is_none() && saved.rsync_only == verification.bootstrap)
    {
        if saved.digest == digest
            && saved.fresh()?
            && revalidate(reader, &bytes, digest)
                .map_err(|error| Kind::Txt.invalid(saved, error))?
        {
            evidence = Some(TxtEvidence::Checkpoint(saved));
        } else {
            events.send(Event::Invalidated(id, Kind::Txt)).await?;
        }
    }
    let txt_evidence = match evidence {
        Some(evidence) => evidence,
        None if verification.bootstrap => TxtEvidence::Rsync(rsync),
        None => {
            let Download::Captured(mut capture) = upstream.fetch(Source::Txt(id.rfc)).await? else {
                return Err("required HTTPS TXT is unavailable".into());
            };
            if bytes.as_slice() == capture.bytes.as_slice() {
                capture.bytes = Arc::default();
                TxtEvidence::HttpsMatch(capture, rsync)
            } else {
                TxtEvidence::Mismatch(capture)
            }
        }
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
