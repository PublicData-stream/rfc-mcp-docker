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
    Rsync,
    Parse,
    Install,
    Database,
    Queue,
}

#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct Timing {
    calls: u64,
    elapsed_us: u64,
}

#[derive(Clone, Default)]
pub(crate) struct Metrics(Arc<Mutex<Record>>);

#[derive(Clone, Default, Serialize)]
pub(crate) struct Record {
    pub catalog: Timing,
    pub txt: Timing,
    pub rfcxml: Timing,
    pub rsync: Timing,
    pub parse: Timing,
    pub install: Timing,
    pub database: Timing,
    pub queue: Timing,
    pub wall_us: u64,
    pub cpu_us: Option<u64>,
    pub memory_peak: Option<u64>,
    pub received_bytes: u64,
    pub https_requests: u64,
    pub https_peak: u64,
    pub minimum_start_spacing_us: Option<u64>,
    pub rsync_peak_bytes: u64,
    pub admitted_peak: u64,
    pub raw_reserved_peak: u64,
    pub parse_peak: u64,
    pub staged_peak: u64,
    pub retries: u64,
    #[serde(skip)]
    active: u64,
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
        }
    }

    pub(crate) fn http_start(&self, stage: Stage) -> Span {
        let mut span = self.span(stage);
        span.http = true;
        let mut record = self.0.lock().unwrap();
        record.https_requests += 1;
        record.active += 1;
        record.https_peak = record.https_peak.max(record.active);
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

    pub(crate) fn received(&self, bytes: usize) {
        self.0.lock().unwrap().received_bytes += bytes as u64;
    }

    pub(crate) fn staged(&self, bytes: u64) {
        let mut record = self.0.lock().unwrap();
        record.rsync_peak_bytes = record.rsync_peak_bytes.max(bytes);
        record.staged_peak = 1;
    }

    pub(crate) fn admitted(&self, entries: usize, raw_bytes: usize) {
        let mut record = self.0.lock().unwrap();
        record.admitted_peak = record.admitted_peak.max(entries as u64);
        record.raw_reserved_peak = record.raw_reserved_peak.max(raw_bytes as u64);
    }

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
        record.cpu_us = usage(libc::RUSAGE_SELF)
            .zip(usage(libc::RUSAGE_CHILDREN))
            .map(|(this, children)| this + children);
        record
    }
}

pub(crate) struct Span {
    metrics: Metrics,
    stage: Stage,
    start: Instant,
    http: bool,
}

impl Drop for Span {
    fn drop(&mut self) {
        let mut record = self.metrics.0.lock().unwrap();
        if self.http {
            record.active -= 1;
        }
        let timing = match self.stage {
            Stage::Catalog => &mut record.catalog,
            Stage::Txt => &mut record.txt,
            Stage::Xml => &mut record.rfcxml,
            Stage::Rsync => &mut record.rsync,
            Stage::Parse => &mut record.parse,
            Stage::Install => &mut record.install,
            Stage::Database => &mut record.database,
            Stage::Queue => &mut record.queue,
        };
        timing.calls += 1;
        timing.elapsed_us += micros(self.start.elapsed());
    }
}

fn usage(who: libc::c_int) -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(who, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
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
