//! Reconciliation aggregates. Source labels and storage classes are fixed.

use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Catalog,
    Txt,
    Xml,
    Feed,
    Rsync,
    Parse,
    Install,
    Cas,
    TxtQueue,
    XmlQueue,
    ParserQueue,
    Database,
    #[cfg(feature = "phase2-fixtures")]
    Validation,
    Queue,
}

#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct Timing {
    calls: u64,
    elapsed_us: u64,
}

#[derive(Clone, Default)]
pub(crate) struct Metrics(Arc<Mutex<Record>>);

#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct Progress {
    selected: usize,
    txt_completed: usize,
    xml_completed: usize,
    txt_remaining: usize,
    xml_remaining: usize,
    admitted: usize,
    xml_reserved: usize,
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct Record {
    pub catalog: Timing,
    pub txt: Timing,
    pub rfcxml: Timing,
    pub feed: Timing,
    pub rsync: Timing,
    pub parse: Timing,
    pub install: Timing,
    pub cas: Timing,
    pub txt_queue: Timing,
    pub rfcxml_queue: Timing,
    pub parser_queue: Timing,
    pub parser_cpu_us: u64,
    pub txt_received_bytes: u64,
    pub rfcxml_received_bytes: u64,
    pub database: Timing,
    pub validation: Timing,
    pub queue: Timing,
    pub wall_us: u64,
    pub cpu_us: Option<u64>,
    pub memory_peak: Option<u64>,
    pub received_bytes: u64,
    pub rsync_transferred_payload_bytes: u64,
    pub https_requests: u64,
    pub https_peak: u64,
    pub minimum_start_spacing_us: Option<u64>,
    pub rsync_peak_bytes: u64,
    pub rsync_transferred_files: usize,
    pub rsync_checksum_hits: usize,
    pub scratch_peak_bytes: Option<u64>,
    pub out_of_order: u64,
    pub admitted_peak: u64,
    pub xml_window_peak: u64,
    pub raw_reserved_peak: u64,
    pub parse_peak: u64,
    pub staged_peak: u64,
    pub retries: u64,
    pub progress: Progress,
    #[cfg(feature = "phase2-fixtures")]
    pub rsync_files: usize,
    #[cfg(feature = "phase2-fixtures")]
    pub rsync_payload_bytes: u64,
    #[cfg(feature = "phase2-fixtures")]
    pub peak_rss_bytes: Option<u64>,
    #[cfg(feature = "phase2-fixtures")]
    pub https_start_window_us: Option<u64>,
    #[cfg(feature = "phase2-fixtures")]
    #[serde(skip)]
    first_start: Option<Instant>,
    #[serde(skip)]
    active: u64,
    #[serde(skip)]
    parsing: u64,
    #[cfg(feature = "phase2-fixtures")]
    #[serde(skip)]
    staging: u64,
    #[serde(skip)]
    last_start: Option<Instant>,
}

impl Metrics {
    pub(crate) fn span(&self, stage: Stage) -> Span {
        Span {
            metrics: self.clone(),
            stage,
            start: Instant::now(),
            http: false,
            parser: false,
            thread_cpu: None,
        }
    }

    pub(crate) fn http_start(&self, stage: Stage) -> Span {
        let mut span = self.span(stage);
        span.http = true;
        let mut record = self.0.lock().unwrap();
        record.https_requests += 1;
        record.active += 1;
        record.https_peak = record.https_peak.max(record.active);
        #[cfg(feature = "phase2-fixtures")]
        {
            let first = *record.first_start.get_or_insert(span.start);
            record.https_start_window_us = Some(micros(span.start.duration_since(first)));
        }
        if let Some(last) = record.last_start.replace(span.start) {
            let interval = micros(span.start.duration_since(last));
            record.minimum_start_spacing_us = Some(
                record
                    .minimum_start_spacing_us
                    .map_or(interval, |old| old.min(interval)),
            );
        }
        span
    }

    pub(crate) fn parse_start(&self) -> Span {
        let mut span = self.span(Stage::Parse);
        span.parser = true;
        let mut record = self.0.lock().unwrap();
        record.parsing += 1;
        record.parse_peak = record.parse_peak.max(record.parsing);
        span
    }

    pub(crate) fn parse_on_thread(&self) -> Span {
        let mut span = self.parse_start();
        span.thread_cpu = usage(libc::RUSAGE_THREAD).as_ref().and_then(cpu_micros);
        span
    }

    pub(crate) fn received(&self, stage: Stage, bytes: usize) {
        let mut record = self.0.lock().unwrap();
        record.received_bytes += bytes as u64;
        match stage {
            Stage::Txt => record.txt_received_bytes += bytes as u64,
            Stage::Xml => record.rfcxml_received_bytes += bytes as u64,
            _ => {}
        }
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn stage_start(&self) -> StageLease {
        let mut record = self.0.lock().unwrap();
        record.staging += 1;
        record.staged_peak = record.staged_peak.max(record.staging);
        StageLease(self.clone())
    }

    pub(crate) fn staged(
        &self,
        bytes: u64,
        transferred_payload: u64,
        scratch: Option<u64>,
        transferred: usize,
        unchanged: usize,
    ) {
        let mut record = self.0.lock().unwrap();
        record.rsync_peak_bytes = record.rsync_peak_bytes.max(bytes);
        record.rsync_transferred_payload_bytes += transferred_payload;
        record.scratch_peak_bytes = record.scratch_peak_bytes.max(scratch);
        record.rsync_transferred_files += transferred;
        record.rsync_checksum_hits += unchanged;
        #[cfg(feature = "phase2-fixtures")]
        {
            record.rsync_files += transferred + unchanged;
            record.rsync_payload_bytes += bytes;
        }
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn out_of_order(&self) {
        self.0.lock().unwrap().out_of_order += 1;
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn admitted(&self, entries: usize) {
        let mut record = self.0.lock().unwrap();
        record.progress.admitted = entries;
        record.admitted_peak = record.admitted_peak.max(entries as u64);
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn xml_reserved(&self, entries: usize, raw_bytes: usize) {
        let mut record = self.0.lock().unwrap();
        record.progress.xml_reserved = entries;
        record.xml_window_peak = record.xml_window_peak.max(entries as u64);
        record.raw_reserved_peak = record.raw_reserved_peak.max(raw_bytes as u64);
    }

    pub(crate) fn retry(&self) {
        self.0.lock().unwrap().retries += 1;
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn selected(&self, count: usize) {
        let mut record = self.0.lock().unwrap();
        record.progress.selected = count;
        record.progress.txt_remaining = count;
        record.progress.xml_remaining = count;
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn txt_completed(&self) {
        let mut record = self.0.lock().unwrap();
        record.progress.txt_completed += 1;
        record.progress.txt_remaining -= 1;
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn xml_completed(&self, admitted: usize) {
        let mut record = self.0.lock().unwrap();
        record.progress.xml_completed += 1;
        record.progress.xml_remaining -= 1;
        record.progress.xml_reserved -= 1;
        record.progress.admitted = admitted;
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn progress(&self) -> Progress {
        self.0.lock().unwrap().progress
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn queue_wait(&self, elapsed: Duration) {
        let mut record = self.0.lock().unwrap();
        record.queue.calls += 1;
        record.queue.elapsed_us += micros(elapsed);
    }

    pub(crate) fn snapshot(&self, elapsed: Duration) -> Record {
        let mut record = self.0.lock().unwrap().clone();
        record.wall_us = micros(elapsed);
        record.memory_peak = std::fs::read_to_string("/sys/fs/cgroup/memory.peak")
            .ok()
            .and_then(|text| text.trim().parse().ok());
        let sync_usage = usage(libc::RUSAGE_SELF);
        record.cpu_us = sync_usage
            .as_ref()
            .and_then(cpu_micros)
            .zip(usage(libc::RUSAGE_CHILDREN).as_ref().and_then(cpu_micros))
            .map(|(this, children)| this + children);
        #[cfg(feature = "phase2-fixtures")]
        {
            record.peak_rss_bytes = sync_usage
                .and_then(|usage| u64::try_from(usage.ru_maxrss).ok())
                .and_then(|kib| kib.checked_mul(1024));
        }
        record
    }
}

#[cfg(feature = "phase2-fixtures")]
pub(crate) struct StageLease(Metrics);

#[cfg(feature = "phase2-fixtures")]
impl Drop for StageLease {
    fn drop(&mut self) {
        self.0.0.lock().unwrap().staging -= 1;
    }
}

pub(crate) struct Span {
    metrics: Metrics,
    stage: Stage,
    start: Instant,
    http: bool,
    parser: bool,
    thread_cpu: Option<u64>,
}

impl Drop for Span {
    fn drop(&mut self) {
        let mut record = self.metrics.0.lock().unwrap();
        if self.http {
            record.active -= 1;
        }
        if self.parser {
            record.parsing -= 1;
        }
        if let Some(start) = self.thread_cpu
            && let Some(end) = usage(libc::RUSAGE_THREAD).as_ref().and_then(cpu_micros)
        {
            record.parser_cpu_us += end.saturating_sub(start);
        }
        let timing = match self.stage {
            Stage::Catalog => &mut record.catalog,
            Stage::Txt => &mut record.txt,
            Stage::Feed => &mut record.feed,
            Stage::Xml => &mut record.rfcxml,
            Stage::Rsync => &mut record.rsync,
            Stage::Parse => &mut record.parse,
            Stage::Install => &mut record.install,
            Stage::Cas => &mut record.cas,
            Stage::TxtQueue => &mut record.txt_queue,
            Stage::XmlQueue => &mut record.rfcxml_queue,
            Stage::ParserQueue => &mut record.parser_queue,
            Stage::Database => &mut record.database,
            #[cfg(feature = "phase2-fixtures")]
            Stage::Validation => &mut record.validation,
            Stage::Queue => &mut record.queue,
        };
        timing.calls += 1;
        timing.elapsed_us += micros(self.start.elapsed());
    }
}

fn usage(who: libc::c_int) -> Option<libc::rusage> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(who, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    Some(unsafe { usage.assume_init() })
}

fn cpu_micros(usage: &libc::rusage) -> Option<u64> {
    [usage.ru_utime, usage.ru_stime]
        .into_iter()
        .try_fold(0, |sum, value| {
            Some(
                sum + u64::try_from(value.tv_sec).ok()? * 1_000_000
                    + u64::try_from(value.tv_usec).ok()?,
            )
        })
}

fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}
