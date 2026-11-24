//! Supervised, persistent rsync staging for one sealed catalog selection.

use crate::{
    ingest::Scope,
    metrics::{Stage, StageLease},
    pipeline::Shutdown,
    upstream::{
        MAX_CATALOG_ENTRIES, ProcessGroup, TXT_LIMIT, Upstream, filename, now_us, read_staged,
    },
};
use rfc_core::RfcNumber;
use rfc_storage::{blob::BlobWriter, postgres::Result};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Write},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};

const ROOT: &str = "/rfc-rsync-txt";
const MAX_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_INODES: u64 = 24_576;
const RECEIPT_BYTES_PER_FILE: usize = 128;
const RSYNC: &str = "/usr/bin/rsync";

#[derive(Debug, Serialize)]
pub(crate) struct Capacity {
    bytes: u64,
    inodes: u64,
}

pub(crate) fn preflight() -> Result<Capacity> {
    let root = private_directory(Path::new(ROOT))?;
    let mounts = fs::read_to_string("/proc/self/mountinfo")?;
    let dedicated = mounts.lines().any(|line| {
        let Some((mount, fs)) = line.split_once(" - ") else {
            return false;
        };
        let mut fields = mount.split_ascii_whitespace().skip(3);
        fields.next() == Some("/")
            && fields.next() == Some(ROOT)
            && fs.split_ascii_whitespace().next() == Some("ext4")
    });
    let device = root.metadata()?.dev();
    if !dedicated
        || device == fs::metadata("/")?.dev()
        || device == fs::metadata("/tmp")?.dev()
        || fs::metadata(rfc_storage::postgres::mirror_root())
            .is_ok_and(|mirror| device == mirror.dev())
    {
        return Err(
            "bulk rsync requires a dedicated ext4 filesystem mounted at /rfc-rsync-txt".into(),
        );
    }
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    check(unsafe { libc::fstatvfs(root.as_raw_fd(), stat.as_mut_ptr()) }.into())?;
    let stat = unsafe { stat.assume_init() };
    let bytes = stat
        .f_blocks
        .checked_mul(stat.f_frsize)
        .ok_or("bulk filesystem size overflow")?;
    if !(1..=MAX_BYTES).contains(&bytes)
        || !(1..=MAX_INODES).contains(&stat.f_files)
        || stat.f_flag & libc::ST_RDONLY != 0
    {
        return Err(
            "bulk rsync filesystem must be writable and limited to 5 GiB and 24576 inodes".into(),
        );
    }
    landlock_abi()?;
    Ok(Capacity {
        bytes,
        inodes: stat.f_files,
    })
}

fn private_directory(path: &Path) -> Result<File> {
    let fd = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = fd.metadata()?;
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o7777 != 0o700 {
        return Err("bulk rsync directories must be owned by Sync with mode 0700".into());
    }
    Ok(fd)
}

fn create_private(path: &Path, device: u64) -> Result<File> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let directory = private_directory(path)?;
    if directory.metadata()?.dev() != device {
        return Err("bulk rsync work tree crosses filesystem boundary".into());
    }
    Ok(directory)
}

#[derive(Clone, Copy)]
pub(crate) struct Proof {
    pub(crate) observed_us: i64,
    pub(crate) fetched_us: Option<i64>,
}

pub(crate) struct Snapshot {
    inventory: PathBuf,
    files: BTreeMap<String, Received>,
    observed_us: i64,
    _lock: File,
    _lease: StageLease,
}

impl Snapshot {
    pub(crate) fn take(&self, store: &BlobWriter, id: RfcNumber) -> Result<(Vec<u8>, Proof)> {
        let name = filename(id);
        let file = self
            .files
            .get(&name)
            .ok_or("RFC is outside the rsync snapshot")?;
        let bytes = read_staged(store, &self.inventory, &name, Some(file.length))?;
        Ok((
            bytes,
            Proof {
                observed_us: self.observed_us,
                fetched_us: file.transferred.then_some(self.observed_us),
            },
        ))
    }
}

pub(crate) async fn stage(
    upstream: &Upstream,
    scope: Scope,
    ids: &[RfcNumber],
    accepted_remaining: u64,
    shutdown: &Shutdown,
) -> Result<Snapshot> {
    if ids.is_empty() || ids.len() > MAX_CATALOG_ENTRIES {
        return Err("bulk rsync selection exceeds catalog bounds".into());
    }
    let metrics = upstream.metrics();
    let _timing = metrics.span(Stage::Rsync);
    let lease = metrics.stage_start();
    let root = Path::new(ROOT);
    let lock = private_directory(root)?;
    check(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }.into()).map_err(
        |error| {
            io::Error::new(
                error.kind(),
                format!("cannot lock bulk rsync staging: {error}"),
            )
        },
    )?;
    let device = lock.metadata()?.dev();
    let scope = root.join(match scope {
        Scope::Full => "full",
        Scope::Sample => "sample",
    });
    let scope_dir = create_private(&scope, device)?;
    let inventory = scope.join("files");
    let files_dir = create_private(&inventory, device)?;
    let temporary = scope.join("temporary");
    let tmp_dir = create_private(&temporary, device)?;
    let names: BTreeSet<_> = ids.iter().copied().map(filename).collect();
    clean(&inventory, Some(&names), device)?;
    clean(&temporary, None, device)?;

    let mut list = tempfile::NamedTempFile::new_in("/tmp")?;
    for name in &names {
        list.write_all(name.as_bytes())?;
        list.write_all(&[0])?;
    }
    let input = File::open(list.path())?;
    drop(list);
    let rules = child_rules(&scope)?;
    let parent = unsafe { libc::getpid() };
    let mut command = Command::new(RSYNC);
    command
        .env_clear()
        .args([
            "--no-motd",
            "--contimeout=5",
            "--timeout=30",
            "--checksum",
            "--fsync",
            "--no-relative",
            "--no-dirs",
            "--from0",
            "--files-from=-",
            "-ii",
            "--out-format=%i %l %n %b",
            "--temp-dir",
        ])
        .arg(&temporary)
        .arg("--")
        .arg(upstream.rsync_module())
        .arg(&inventory)
        .stdin(Stdio::from(input))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(
                libc::PR_SET_PDEATHSIG,
                libc::SIGKILL as libc::c_ulong,
                0,
                0,
                0,
            ) != 0
                || libc::getppid() != parent
            {
                return Err(io::Error::other("cannot supervise bulk rsync child"));
            }
            let limit = libc::rlimit {
                rlim_cur: TXT_LIMIT as _,
                rlim_max: TXT_LIMIT as _,
            };
            check(libc::setrlimit(libc::RLIMIT_FSIZE, &limit).into())?;
            check(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0).into())?;
            check(libc::syscall(
                libc::SYS_landlock_restrict_self,
                rules.as_raw_fd(),
                0_u32,
            ))?;
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    let mut group = ProcessGroup(child.id().map(i32::try_from).transpose()?);
    let capacity = names.len() * RECEIPT_BYTES_PER_FILE;
    let mut stdout = child
        .stdout
        .take()
        .expect("piped rsync receipt")
        .take((capacity + 1) as u64);
    let mut receipt = Vec::new();
    let transfer = async {
        stdout.read_to_end(&mut receipt).await?;
        if receipt.len() > capacity {
            return Err(io::Error::other("bulk rsync receipt exceeds its limit"));
        }
        child.wait().await
    };
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(io::Error::other("bulk rsync cancelled")),
        _ = tokio::time::sleep(Duration::from_secs(upstream.config().ingestion.rsync.bulk_transfer_timeout_seconds)) =>
            Err(io::Error::other("bulk rsync deadline exceeded")),
        result = transfer => result,
    };
    if !result.as_ref().is_ok_and(|status| status.success()) {
        group.kill();
        child.wait().await?;
        clean(&temporary, None, device)?;
        let status = result?;
        return Err(format!("bulk rsync failed ({status})").into());
    }
    group.0 = None;
    let observed_us = now_us()?;
    let files = parse_receipt(&receipt, &names, accepted_remaining)?;
    let mut count = 0;
    for entry in fs::read_dir(&inventory)? {
        let entry = entry?;
        let name = entry.file_name();
        let expected = name.to_str().and_then(|name| files.get(name));
        let length = regular_length(&entry, device)?;
        if !expected.is_some_and(|file| file.length as u64 == length) {
            return Err("bulk rsync inventory disagrees with its receipt".into());
        }
        count += 1;
    }
    if count != names.len() || fs::read_dir(&temporary)?.next().is_some() {
        return Err("bulk rsync inventory is incomplete or has temporary entries".into());
    }
    for directory in [&files_dir, &tmp_dir, &scope_dir, &lock] {
        directory.sync_all()?;
    }
    let bytes = files.values().map(|file| file.length as u64).sum();
    let transferred = files.values().filter(|file| file.transferred).count();
    metrics.staged(bytes, None, transferred, files.len() - transferred);
    Ok(Snapshot {
        inventory,
        files,
        observed_us,
        _lock: lock,
        _lease: lease,
    })
}

fn regular_length(entry: &fs::DirEntry, device: u64) -> Result<u64> {
    let metadata = entry.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.dev() != device
    {
        return Err("bulk rsync staging contains a non-regular or unsafe file".into());
    }
    Ok(metadata.len())
}

fn clean(path: &Path, retain: Option<&BTreeSet<String>>, device: u64) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        regular_length(&entry, device)?;
        if let Some(retain) = retain {
            let name = entry.file_name();
            let name = name.to_str().ok_or("invalid name in bulk rsync staging")?;
            let id = name
                .strip_prefix("rfc")
                .and_then(|name| name.strip_suffix(".txt"))
                .and_then(|number| number.parse().ok())
                .and_then(RfcNumber::new);
            if !id.is_some_and(|id| filename(id) == name) {
                return Err("unexpected name in bulk rsync staging".into());
            }
            if retain.contains(name) {
                continue;
            }
        }
        fs::remove_file(entry.path())?;
    }
    Ok(())
}

struct Received {
    length: usize,
    transferred: bool,
}

fn parse_receipt(
    receipt: &[u8],
    names: &BTreeSet<String>,
    accepted_remaining: u64,
) -> Result<BTreeMap<String, Received>> {
    let receipt = std::str::from_utf8(receipt)?;
    if !receipt.ends_with('\n') {
        return Err("bulk rsync receipt is incomplete".into());
    }
    let mut files = BTreeMap::new();
    let mut bytes = 0;
    for line in receipt.lines() {
        let (item, rest) = line
            .split_at_checked(11)
            .ok_or("invalid bulk rsync receipt")?;
        // Repeated itemization includes checksum-confirmed unchanged files.
        let transferred = match item.get(..2) {
            Some(">f") => true,
            Some(".f") if item.as_bytes()[2..].iter().all(|byte| *byte == b' ') => false,
            _ => return Err("bulk rsync skipped a file or reported a non-regular input".into()),
        };
        let fields: Vec<_> = rest.split_ascii_whitespace().collect();
        let [length, name, traffic] = fields.as_slice() else {
            return Err("invalid bulk rsync receipt fields".into());
        };
        let length: usize = length.parse()?;
        let traffic: u64 = traffic.parse()?;
        if !(1..=TXT_LIMIT).contains(&length) || transferred != (traffic > 0) {
            return Err("bulk rsync TXT size or transfer evidence exceeds bounds".into());
        }
        if !names.contains(*name)
            || files
                .insert(
                    (*name).to_owned(),
                    Received {
                        length,
                        transferred,
                    },
                )
                .is_some()
        {
            return Err("unexpected or duplicate bulk rsync receipt entry".into());
        }
        bytes += length as u64;
        if bytes > accepted_remaining {
            return Err("bulk rsync exceeds remaining accepted input byte budget".into());
        }
    }
    if files.len() != names.len() {
        return Err("bulk rsync is missing expected TXT receipt entries".into());
    }
    Ok(files)
}

#[repr(C, packed)]
struct PathBeneath {
    access: u64,
    fd: i32,
}

fn check(result: libc::c_long) -> io::Result<libc::c_long> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn landlock_abi() -> io::Result<libc::c_long> {
    let abi = check(unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<u8>(),
            0_usize,
            1_u32,
        )
    })?;
    if abi < 3 {
        return Err(io::Error::other(
            "bulk rsync requires Landlock ABI 3 or newer",
        ));
    }
    Ok(abi)
}

fn child_rules(scope: &Path) -> io::Result<OwnedFd> {
    const EXECUTE: u64 = 1;
    const WRITE: u64 = 1 << 1;
    const READ_FILE: u64 = 1 << 2;
    const READ_DIR: u64 = 1 << 3;
    const REMOVE_FILE: u64 = 1 << 5;
    const MAKE_REGULAR: u64 = 1 << 8;
    const REFER: u64 = 1 << 13;
    const TRUNCATE: u64 = 1 << 14;

    let abi = landlock_abi()?;
    let handled: u64 = (1 << if abi >= 5 { 16 } else { 15 }) - 1;
    let rules = check(unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &handled,
            size_of::<u64>(),
            0_u32,
        )
    })?;
    let rules = unsafe { OwnedFd::from_raw_fd(rules as i32) };
    let allow = |path: &Path, access| {
        let fd = File::options()
            .read(true)
            .custom_flags(libc::O_PATH)
            .open(path)?;
        let rule = PathBeneath {
            access,
            fd: fd.as_raw_fd(),
        };
        check(unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                rules.as_raw_fd(),
                1_u32,
                &rule,
                0_u32,
            )
        })?;
        Ok::<_, io::Error>(())
    };
    allow(
        scope,
        WRITE | READ_FILE | READ_DIR | REMOVE_FILE | MAKE_REGULAR | REFER | TRUNCATE,
    )?;
    allow(Path::new(RSYNC), EXECUTE | READ_FILE)?;
    for path in ["/lib", "/usr/lib"] {
        allow(Path::new(path), EXECUTE | READ_FILE | READ_DIR)?;
    }
    for path in [
        "/etc/resolv.conf",
        "/etc/hosts",
        "/etc/services",
        "/etc/protocols",
        "/etc/nsswitch.conf",
        "/dev/urandom",
    ] {
        if let Err(error) = allow(Path::new(path), READ_FILE)
            && error.kind() != io::ErrorKind::NotFound
        {
            return Err(error);
        }
    }
    Ok(rules)
}
