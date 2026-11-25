//! Required TXT revalidation under the acquisition coordinator.

mod journal;
mod transfer;

use crate::{
    acquisition::{Accepted, LocalFailure, Original},
    config::Revalidation,
    diagnostics::{self, Code, Failure, Level},
    shutdown::Shutdown,
    upstream::{TXT_LIMIT, Upstream, now_us},
};
use journal::Journal;
use rfc_core::{BlobDigest, RfcNumber};
use rfc_mcp_docker::control::jitter;
use rfc_storage::postgres::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    mem::MaybeUninit,
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

const ROOT: &str = "/rfc-rsync-audit";
const MIB: u64 = 1024 * 1024;
const BATCH_BYTES: u64 = 256 * MIB;
const QUARANTINE_BYTES: u64 = 256 * MIB;
const QUARANTINE_PAIRS: u64 = 128;
const QUARANTINE_AGE_US: i64 = 7 * 24 * 60 * 60 * 1_000_000;
const REPAIR_BYTES: u64 = 2 * TXT_LIMIT as u64 + 16 * 1024;
pub(crate) const MEMORY_RESERVATION: u64 = 32 * MIB;

#[derive(Debug, Serialize)]
pub(crate) struct Capacity {
    available_bytes: u64,
    available_inodes: u64,
}

pub(crate) fn preflight() -> Result<Capacity> {
    let root = Path::new(ROOT);
    let file = open_private_dir(root)?;
    let mounts = fs::read_to_string("/proc/self/mountinfo")?;
    let mut entries = mounts.lines().filter_map(|line| {
        let (mount, filesystem) = line.split_once(" - ")?;
        let mut fields = mount.split_ascii_whitespace().skip(2);
        Some((
            fields.next()?,
            Path::new(fields.next()?),
            fields.next()?,
            filesystem.split_ascii_whitespace().next()?,
        ))
    });
    let audit_mount = entries
        .clone()
        .find(|entry| entry.2 == ROOT)
        .filter(|entry| !matches!(entry.3, "tmpfs" | "ramfs" | "hugetlbfs"))
        .ok_or("rsync audit requires a separate, disk-backed /rfc-rsync-audit mount")?;
    let mirror = rfc_storage::postgres::mirror_root();
    let overlaps = entries.any(|(device, underlying, at, _)| {
        (at == crate::egress::ROOT || Path::new(at) == mirror || at == "/tmp")
            && device == audit_mount.0
            && (underlying.starts_with(audit_mount.1) || audit_mount.1.starts_with(underlying))
    });
    let own = file.metadata()?;
    let aliases = [
        PathBuf::from("/"),
        PathBuf::from("/tmp"),
        PathBuf::from(crate::egress::ROOT),
        rfc_storage::postgres::mirror_root(),
    ]
    .into_iter()
    .filter_map(|path| fs::metadata(path).ok())
    .any(|other| (other.dev(), other.ino()) == (own.dev(), own.ino()));
    if overlaps || aliases {
        return Err("rsync audit requires a separate, disk-backed /rfc-rsync-audit mount".into());
    }
    crate::rsync::landlock_abi()?;
    capacity(&file, BATCH_BYTES + REPAIR_BYTES, 1024)
}

fn capacity(dir: &File, bytes: u64, inodes: u64) -> Result<Capacity> {
    let mut stats = MaybeUninit::<libc::statvfs>::uninit();
    // fstatvfs writes the buffer on success. The open descriptor pins the mount.
    let status = unsafe { libc::fstatvfs(dir.as_raw_fd(), stats.as_mut_ptr()) };
    if status != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let stats = unsafe { stats.assume_init() };
    let available_bytes = stats
        .f_bavail
        .checked_mul(stats.f_frsize)
        .ok_or("audit filesystem capacity overflow")?;
    if stats.f_flag & libc::ST_RDONLY != 0 || available_bytes < bytes || stats.f_favail < inodes {
        return Err("rsync audit volume lacks writable bytes or inodes".into());
    }
    Ok(Capacity {
        available_bytes,
        available_inodes: stats.f_favail,
    })
}

fn open_private_dir(path: &Path) -> Result<File> {
    let file = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| format!("cannot open private rsync audit directory: {error}"))?;
    let meta = file.metadata()?;
    // Both uids are local container identities, including under user namespaces.
    if meta.uid() != unsafe { libc::geteuid() }
        || meta.gid() != unsafe { libc::getegid() }
        || meta.mode() & 0o7777 != 0o700
    {
        return Err("rsync audit directory must be owned by Sync with mode 0700".into());
    }
    Ok(file)
}

fn private_dir(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => (),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error.into()),
    }
    open_private_dir(path)?;
    Ok(())
}

fn remove_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            open_private_dir(path)?;
            fs::remove_dir_all(path)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn remove_regular(path: &Path) -> Result<()> {
    match transfer::open_regular(path) {
        Ok(_) => fs::remove_file(path)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct Input {
    pub rfc: u32,
    pub length: u64,
    pub digest: String,
    pub observation: String,
}

impl Input {
    pub fn new(rfc: u32, original: &Original) -> Result<Self> {
        Ok(Self {
            rfc,
            length: original.length,
            digest: original.digest.to_string(),
            observation: original
                .observation
                .clone()
                .ok_or("audit requires a sealed HTTPS observation")?,
        })
    }

    fn id(&self) -> Result<RfcNumber> {
        RfcNumber::new(i32::try_from(self.rfc)?).ok_or_else(|| "invalid audit RFC".into())
    }

    fn filename(&self) -> String {
        format!("rfc{}.txt", self.rfc)
    }

    fn digest(&self) -> Result<BlobDigest> {
        BlobDigest::from_hex(&self.digest).ok_or_else(|| "invalid audit digest".into())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct BatchReceipt {
    pub id: String,
    pub started_us: i64,
    pub observed_us: i64,
    pub checksum_choice: String,
    pub algorithm: Option<String>,
    module: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Equal {
    pub original: Original,
    https_url: String,
    pub rsync_observed_us: i64,
    rsync_url: String,
    pub rsync_digest: String,
    pub rsync_length: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum RepairReason {
    Interrupted,
    ByteMismatch,
    CaptureFailed,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Repair {
    attempts: usize,
    not_before_us: i64,
    reason: Option<RepairReason>,
    equal: Option<Equal>,
    sealed: Option<Input>,
}

enum State {
    Matched(Input, BatchReceipt),
    Repair(Input, Box<Repair>),
}

impl State {
    fn rfc(&self) -> u32 {
        match self {
            Self::Matched(input, _) | Self::Repair(input, _) => input.rfc,
        }
    }
}

pub(crate) struct Resolved {
    pub input: Input,
    pub original: Original,
    pub path: PathBuf,
    pub acknowledge: oneshot::Sender<Result<Input>>,
}

#[derive(Default, Serialize)]
pub(crate) struct Report {
    pub matched: usize,
    pub repaired: usize,
    pub reused: usize,
    pub unresolved: usize,
    sessions: usize,
    failed_sessions: usize,
    session_ms: u64,
    repair_attempts: usize,
    quarantine_bytes: u64,
    quarantine_peak_bytes: u64,
    snapshot_peak_bytes: u64,
}

impl Report {
    fn record_quarantine(&mut self, bytes: u64) {
        self.quarantine_bytes = bytes;
        self.quarantine_peak_bytes = self.quarantine_peak_bytes.max(bytes);
    }
}

pub(crate) enum Proof<'a> {
    Checksum(&'a BatchReceipt),
    Repair(&'a Equal),
}

pub(crate) struct Completion(Journal, Instant, i64);

impl Completion {
    pub fn window(&self) -> (i64, i64) {
        (self.0.started_us, self.2)
    }

    pub fn proof(&self, input: &Input) -> Result<Proof<'_>> {
        match self.0.states.get(&input.rfc) {
            Some(State::Matched(saved, receipt)) if saved == input => Ok(Proof::Checksum(receipt)),
            Some(State::Repair(_, repair)) if repair.sealed.as_ref() == Some(input) => {
                Ok(Proof::Repair(
                    repair
                        .equal
                        .as_ref()
                        .ok_or("sealed audit repair has no proof")?,
                ))
            }
            _ => Err("audit proof does not bind the selected HTTPS original".into()),
        }
    }

    pub fn prepare(&mut self, generation: i64) -> Result<()> {
        if Instant::now() >= self.1 {
            return Err("audit receipts expired before candidate readiness".into());
        }
        self.0.prepare(generation)
    }

    pub fn commit(mut self) -> Result<()> {
        self.0.complete()
    }

    pub fn invalidate(self) -> Result<()> {
        self.0.invalidate()
    }
}

pub(crate) struct Audit {
    root: PathBuf,
    quarantine: PathBuf,
    journal: Journal,
    deadline: Instant,
    options: Revalidation,
    report: Report,
    expires_us: i64,
}

impl Audit {
    pub async fn open(
        scope: &str,
        key: &str,
        config: &Revalidation,
        writer: &crate::Writer,
    ) -> Result<Self> {
        let volume = Path::new(ROOT);
        let root = volume.join(scope);
        let quarantine = volume.join("quarantine");
        private_dir(&root)?;
        private_dir(&quarantine)?;
        File::open(volume)?.sync_all()?;
        // A batch may have been interrupted before its receipt was committed.
        remove_directory(&root.join("snapshot"))?;
        let now = now_us()?;
        let lifetime_us = i64::try_from(config.interval_seconds * 1_000_000)?;
        let mut journal = Journal::open(&root, key, now, lifetime_us)?;
        if let Some(generation) = journal.prepared
            && writer.client.query_one(
                "SELECT EXISTS (SELECT FROM rfc.generations WHERE id = $1 AND manifest_format = 3)",
                &[&generation],
            ).await?.try_get::<_, bool>(0)?
        {
            journal.invalidate()?;
            journal = Journal::open(&root, key, now, lifetime_us)?;
        }
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(rfc) = name.to_str().and_then(|name| name.strip_prefix("repair-")) {
                let id = rfc.parse::<u32>()?;
                let pending = matches!(
                    journal.states.get(&id),
                    Some(State::Repair(_, repair)) if repair.equal.is_some() && repair.sealed.is_none()
                );
                if !pending {
                    remove_directory(&entry.path())?;
                }
            }
        }
        let expires_us = journal.started_us + lifetime_us;
        let deadline = Instant::now() + Duration::from_micros((expires_us - now) as u64);
        let mut audit = Self {
            root,
            quarantine,
            journal,
            deadline,
            options: config.clone(),
            report: Report::default(),
            expires_us,
        };
        let used = audit.quarantine_usage(now)?.0;
        audit.report.record_quarantine(used);
        Ok(audit)
    }

    pub async fn run(
        mut self,
        mut input: mpsc::Receiver<Input>,
        resolved: mpsc::Sender<Resolved>,
        upstream: Upstream,
        shutdown: Shutdown,
    ) -> (Report, Result<Completion>) {
        let result = self
            .process(&mut input, &resolved, &upstream, &shutdown)
            .await;
        if result.is_err() {
            self.report.unresolved += 1;
        }
        (
            self.report,
            result
                .map(|()| Completion(self.journal, self.deadline, self.expires_us))
                .map_err(|error| Failure::new(Code::AuditFailed, error).into()),
        )
    }

    async fn process(
        &mut self,
        input: &mut mpsc::Receiver<Input>,
        resolved: &mpsc::Sender<Resolved>,
        upstream: &Upstream,
        stop: &Shutdown,
    ) -> Result<()> {
        let mut batch = Vec::new();
        let mut bytes = 0;
        while let Some(item) = stop
            .until(self.deadline, async { Ok(input.recv().await) })
            .await?
        {
            match self.journal.states.get(&item.rfc) {
                Some(State::Matched(saved, _)) if saved == &item => {
                    self.report.matched += 1;
                    self.report.reused += 1;
                    upstream
                        .run()
                        .progress(|progress| progress.audit_matched += 1);
                }
                Some(State::Repair(_, repair)) if repair.sealed.as_ref() == Some(&item) => {
                    self.report.repaired += 1;
                    self.report.reused += 1;
                    upstream
                        .run()
                        .progress(|progress| progress.audit_repaired += 1);
                }
                Some(State::Repair(_, repair)) if repair.sealed.is_none() => {
                    // A fresh HTTPS capture does not clear a confirmed mismatch.
                    // If a durable pair already exists, verify and replay it first.
                    self.repair(item, repair.as_ref().clone(), resolved, upstream, stop)
                        .await
                        .map_err(|error| Failure::new(Code::RepairFailed, error))?;
                }
                _ => {
                    if !(1..=TXT_LIMIT as u64).contains(&item.length) {
                        return Err("TXT is outside the audit byte budget".into());
                    }
                    if bytes + item.length > BATCH_BYTES {
                        self.batch(&batch, resolved, upstream, stop).await?;
                        batch.clear();
                        bytes = 0;
                    }
                    bytes += item.length;
                    batch.push(item);
                    if batch.len() == self.options.batch_size as usize {
                        self.batch(&batch, resolved, upstream, stop).await?;
                        batch.clear();
                        bytes = 0;
                    }
                }
            }
        }
        if !batch.is_empty() {
            self.batch(&batch, resolved, upstream, stop).await?;
        }
        stop.check()
    }

    fn timeout(&self) -> Duration {
        self.deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(self.options.session_timeout_seconds))
    }

    async fn batch(
        &mut self,
        inputs: &[Input],
        resolved: &mpsc::Sender<Resolved>,
        upstream: &Upstream,
        stop: &Shutdown,
    ) -> Result<()> {
        let path = self.root.join("snapshot");
        private_dir(&path)?;
        let total = inputs.iter().map(|item| item.length).sum::<u64>();
        self.report.snapshot_peak_bytes = self.report.snapshot_peak_bytes.max(total);
        let result = self.compare(&path, inputs, upstream, stop).await;
        // Keep a completed snapshot until the committed receipt has captured
        // its identity. No snapshot is ever used after startup recovery.
        let cleanup = remove_directory(&path);
        result?;
        cleanup?;
        let matched = inputs
            .iter()
            .filter(|item| matches!(self.journal.states.get(&item.rfc), Some(State::Matched(..))))
            .count();
        self.report.matched += matched;
        upstream
            .run()
            .progress(|progress| progress.audit_matched += matched);
        for item in inputs {
            if matches!(
                self.journal.states.get(&item.rfc),
                Some(State::Repair(_, _))
            ) {
                self.repair(item.clone(), Repair::default(), resolved, upstream, stop)
                    .await
                    .map_err(|error| Failure::new(Code::RepairFailed, error))?;
            }
        }
        Ok(())
    }

    async fn compare(
        &mut self,
        path: &Path,
        inputs: &[Input],
        upstream: &Upstream,
        stop: &Shutdown,
    ) -> Result<()> {
        capacity(
            &open_private_dir(path)?,
            inputs.iter().map(|i| i.length + 4096).sum(),
            inputs.len() as u64 + 2,
        )?;
        let copy = (path.to_owned(), inputs.to_vec(), stop.clone());
        tokio::task::spawn_blocking(move || transfer::snapshot(&copy.0, &copy.1, &copy.2))
            .await??;
        for attempt in 0..self.options.max_attempts {
            let start = Instant::now();
            self.report.sessions += 1;
            let result = transfer::compare(path, inputs, upstream, stop, self.timeout()).await;
            self.report.session_ms +=
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
            match result {
                Ok((session, outcomes)) => {
                    stop.check()?;
                    if self.timeout().is_zero() {
                        return Err("rsync audit expired before committing its receipt".into());
                    }
                    let check = (path.to_owned(), inputs.to_vec(), stop.clone());
                    tokio::task::spawn_blocking(move || {
                        transfer::verify(&check.0, &check.1, &check.2)
                    })
                    .await??;
                    let entries = inputs
                        .iter()
                        .map(|item| (item.clone(), outcomes[&item.rfc]))
                        .collect();
                    self.journal.batch(session, entries)?;
                    let bytes = inputs.iter().map(|item| item.length).sum();
                    let matches = outcomes.values().filter(|&&matched| matched).count();
                    upstream.run().event(Level::Debug, "audit_batch", || json!({
                        "attempt": attempt + 1, "items": inputs.len(), "matches": matches,
                        "outcome": "committed", "duration_ms": diagnostics::millis(start.elapsed()),
                    }));
                    upstream.metrics().staged(bytes, 0, None, 0, matches);
                    rfc_storage::fixture_boundary("after_rsync_audit_receipt");
                    return Ok(());
                }
                Err(error) => {
                    self.report.failed_sessions += 1;
                    upstream.run().event(Level::Debug, "audit_batch", || json!({
                        "attempt": attempt + 1, "items": inputs.len(),
                        "outcome": "failed", "duration_ms": diagnostics::millis(start.elapsed()),
                    }));
                    stop.check()?;
                    if attempt + 1 == self.options.max_attempts {
                        self.report.unresolved += inputs.len() - 1;
                        return Err(error);
                    }
                    self.delay(0, attempt, stop).await?;
                }
            }
        }
        unreachable!("validated audit attempts are nonzero")
    }

    async fn delay(&self, at_us: i64, attempt: usize, stop: &Shutdown) -> Result<()> {
        let delay = if at_us == 0 {
            jitter(Duration::from_secs(
                self.options.retry_backoff_seconds[attempt],
            ))
        } else {
            Duration::from_micros(at_us.saturating_sub(now_us()?).max(0) as u64)
        };
        stop.until(self.deadline, async {
            tokio::time::sleep(delay).await;
            Ok(())
        })
        .await
    }

    async fn repair(
        &mut self,
        input: Input,
        mut repair: Repair,
        resolved: &mpsc::Sender<Resolved>,
        upstream: &Upstream,
        stop: &Shutdown,
    ) -> Result<()> {
        let path = self.root.join(format!("repair-{}", input.rfc));
        loop {
            stop.check()?;
            if let Some(proof) = &repair.equal {
                if input.digest != proof.original.digest.to_string()
                    && self.journal.states.get(&input.rfc).is_some_and(
                        |state| matches!(state, State::Repair(saved, _) if saved != &input),
                    )
                {
                    return Err("current HTTPS capture changed after repair was recorded".into());
                }
                let https = path.join("https");
                let rsync = path.join("rsync").join(input.filename());
                let (same, digest, length) = compare_pair(&https, &rsync, &proof.original)?;
                if !same || digest.to_string() != proof.rsync_digest || length != proof.rsync_length
                {
                    return Err("previously equal repair bytes changed".into());
                }
                let (acknowledge, ack) = oneshot::channel();
                let message = Resolved {
                    input: input.clone(),
                    original: proof.original.clone(),
                    path: https,
                    acknowledge,
                };
                let sealed = stop
                    .until(self.deadline, async {
                        resolved
                            .send(message)
                            .await
                            .map_err(|_| "repair coordinator stopped")?;
                        ack.await?
                    })
                    .await?;
                repair.sealed = Some(sealed);
                self.journal.repair(input, repair)?;
                remove_directory(&path)?;
                File::open(&self.root)?.sync_all()?;
                self.report.repaired += 1;
                upstream
                    .run()
                    .progress(|progress| progress.audit_repaired += 1);
                return Ok(());
            }
            if repair.attempts >= self.options.max_attempts {
                return Err(format!(
                    "RFC {} TXT repair exhausted ({:?})",
                    input.rfc, repair.reason
                )
                .into());
            }
            if repair.not_before_us != 0 {
                self.delay(repair.not_before_us, 0, stop).await?;
            }
            stop.until(self.deadline, upstream.wait_repair_egress(stop))
                .await?;
            let now = now_us()?;
            let (bytes, pairs) = self.quarantine_usage(now)?;
            self.report.record_quarantine(bytes);
            if pairs >= QUARANTINE_PAIRS || bytes + REPAIR_BYTES > QUARANTINE_BYTES {
                return Err("TXT audit quarantine is full".into());
            }
            capacity(&open_private_dir(&self.root)?, REPAIR_BYTES, 8)?;
            remove_directory(&path)?;
            private_dir(&path)?;
            private_dir(&path.join("rsync"))?;
            repair.attempts += 1;
            repair.not_before_us = self.next_retry(repair.attempts)?;
            repair.reason = Some(RepairReason::Interrupted);
            self.journal.repair(input.clone(), repair.clone())?;
            self.report.repair_attempts += 1;
            rfc_storage::fixture_boundary("before_rsync_repair_capture");
            let started = Instant::now();
            let attempt = self.capture_pair(&path, &input, upstream, stop).await;
            upstream.run().event(Level::Debug, "repair_attempt", || json!({
                "rfc": input.rfc, "attempt": repair.attempts, "duration_ms": diagnostics::millis(started.elapsed()),
                "outcome": match &attempt { Ok((true, _)) => "matched", Ok((false, _)) => "mismatch", Err(_) => "failed" },
            }));
            match attempt {
                Ok((true, proof)) => {
                    repair.equal = Some(proof);
                    repair.reason = None;
                    self.journal.repair(input.clone(), repair.clone())?;
                    rfc_storage::fixture_boundary("after_rsync_repair_pair");
                    continue;
                }
                Ok((false, _)) => {
                    let name = format!(
                        "{now}-{}-{}-{}",
                        self.journal.started_us, input.rfc, repair.attempts
                    );
                    fs::rename(&path, self.quarantine.join(name))?;
                    File::open(&self.quarantine)?.sync_all()?;
                    File::open(&self.root)?.sync_all()?;
                    repair.reason = Some(RepairReason::ByteMismatch);
                    let bytes = self.quarantine_usage(now)?.0;
                    self.report.record_quarantine(bytes);
                }
                Err(error) => {
                    stop.check()?;
                    if error.is::<LocalFailure>() {
                        return Err(error);
                    }
                    repair.reason = Some(RepairReason::CaptureFailed);
                    if repair.attempts == self.options.max_attempts {
                        self.journal.repair(input.clone(), repair.clone())?;
                        return Err(
                            format!("RFC {} TXT repair exhausted: {error}", input.rfc).into()
                        );
                    }
                }
            }
            repair.not_before_us = self.next_retry(repair.attempts)?;
            self.journal.repair(input.clone(), repair.clone())?;
        }
    }

    async fn capture_pair(
        &self,
        path: &Path,
        input: &Input,
        upstream: &Upstream,
        stop: &Shutdown,
    ) -> Result<(bool, Equal)> {
        let https = path.join("https");
        let budget = Accepted::new(TXT_LIMIT as u64);
        let id = input.id()?;
        let capture = stop
            .until(
                self.deadline,
                upstream.repair_txt(id, &https, stop, &budget),
            )
            .await?;
        let original = Original::captured(capture)?;
        let rsync_dir = path.join("rsync");
        let rsync_observed_us =
            transfer::fetch(&rsync_dir, input, upstream, stop, self.timeout()).await?;
        stop.check()?;
        let rsync_url = format!("{}{}", upstream.rsync_module(), input.filename());
        let (equal, digest, length) =
            compare_pair(&https, &rsync_dir.join(input.filename()), &original)?;
        let proof = Equal {
            original,
            https_url: crate::upstream::Source::Txt(id).url(),
            rsync_observed_us,
            rsync_url,
            rsync_digest: digest.to_string(),
            rsync_length: length,
        };
        seal_pair(path, input, &proof)?;
        Ok((equal, proof))
    }

    fn next_retry(&self, attempt: usize) -> Result<i64> {
        let delay = self
            .options
            .retry_backoff_seconds
            .get(attempt - 1)
            .copied()
            .unwrap_or(0);
        Ok(now_us()? + i64::try_from(jitter(Duration::from_secs(delay)).as_micros())?)
    }

    fn quarantine_usage(&self, now: i64) -> Result<(u64, u64)> {
        let mut allocated = 0_u64;
        let mut pairs = 0_u64;
        for entry in fs::read_dir(&self.quarantine)? {
            let entry = entry?;
            let name = entry.file_name();
            let stamp: i64 = name
                .to_str()
                .and_then(|name| name.split_once('-'))
                .and_then(|(stamp, _)| stamp.parse().ok())
                .ok_or("invalid audit quarantine entry")?;
            let path = entry.path();
            open_private_dir(&path)?;
            if now.saturating_sub(stamp) >= QUARANTINE_AGE_US {
                remove_directory(&path)?;
                File::open(&self.quarantine)?.sync_all()?;
                continue;
            }
            let mut parts = name
                .to_str()
                .ok_or("invalid audit quarantine entry")?
                .split('-');
            let id: u32 = parts
                .nth(2)
                .ok_or("invalid audit quarantine RFC")?
                .parse()?;
            let rsync = path.join("rsync");
            open_private_dir(&rsync)?;
            allocated += 8192; // Directory entries and filesystem overhead.
            for file in [
                path.join("receipt"),
                path.join("https"),
                rsync.join(format!("rfc{id}.txt")),
            ] {
                let meta = transfer::open_regular(&file)?.metadata()?;
                if !(1..=TXT_LIMIT as u64).contains(&meta.len()) {
                    return Err("invalid audit quarantine file length".into());
                }
                allocated += meta.len().next_multiple_of(4096);
            }
            pairs += 1;
            if allocated > QUARANTINE_BYTES || pairs > QUARANTINE_PAIRS {
                return Err("audit quarantine exceeds configured limits".into());
            }
        }
        Ok((allocated, pairs))
    }
}

fn compare_pair(
    https: &Path,
    rsync: &Path,
    original: &Original,
) -> Result<(bool, BlobDigest, u64)> {
    // Transfer length is bounded by the HTTP reader and the rsync child limit.
    let read = |path| -> Result<Vec<u8>> {
        let file = transfer::open_regular(path)?;
        let mut bytes = Vec::new();
        file.take(TXT_LIMIT as u64 + 1).read_to_end(&mut bytes)?;
        if !(1..=TXT_LIMIT).contains(&bytes.len()) {
            return Err("repair original exceeds its TXT limits".into());
        }
        Ok(bytes)
    };
    let https = read(https)?;
    let rsync = read(rsync)?;
    if https.len() as u64 != original.length || BlobDigest::hash(&https) != original.digest {
        return Err("repair HTTPS body failed original verification".into());
    }
    Ok((https == rsync, BlobDigest::hash(&rsync), rsync.len() as u64))
}

fn seal_pair(path: &Path, input: &Input, proof: &Equal) -> Result<()> {
    for body in [
        path.join("https"),
        path.join("rsync").join(input.filename()),
    ] {
        let file = transfer::open_regular(&body)?;
        file.set_permissions(fs::Permissions::from_mode(0o400))?;
        file.sync_all()?;
    }
    File::open(path.join("rsync"))?.sync_all()?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path.join("receipt"))?;
    let mut output = io::BufWriter::new(file);
    serde_json::to_writer(&mut output, &(input, proof))?;
    output.flush()?;
    output.get_ref().sync_all()?;
    File::open(path)?.sync_all()?;
    File::open(path.parent().ok_or("invalid repair path")?)?.sync_all()?;
    Ok(())
}
