//! Deployment-wide HTTPS ownership and restart-safe cooldown.

use rfc_storage::postgres::Result;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) const ROOT: &str = "/rfc-https-spool";
pub(crate) const MANUAL_SOCKET: &str = "/rfc-https-spool/operator.sock";

pub(crate) fn preflight(accepted: u64) -> Result<(u64, u64)> {
    private_root()?;
    let root = c"/rfc-https-spool";
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(root.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let stats = unsafe { stats.assume_init() };
    let available = stats
        .f_bavail
        .checked_mul(stats.f_frsize)
        .ok_or("spool size overflow")?;
    if available
        < accepted
            .checked_add(640 * 1024 * 1024)
            .ok_or("spool size overflow")?
        || stats.f_favail < 40_016
        || stats.f_frsize > 4096
    {
        return Err(
            "HTTPS spool lacks free bytes or inodes for the configured ingestion budget".into(),
        );
    }
    Ok((available, stats.f_favail))
}

fn private_root() -> io::Result<()> {
    let metadata = fs::symlink_metadata(ROOT)?;
    if !metadata.is_dir()
        || metadata.mode() & 0o777 != 0o700
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HTTPS spool requires a private directory owned by Sync",
        ));
    }
    Ok(())
}

pub(crate) struct Lease(File);

impl Lease {
    pub(crate) fn acquire() -> io::Result<Self> {
        private_root()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(Path::new(ROOT).join("egress.lock"))?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid HTTPS lease file",
            ));
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::WouldBlock {
                Err(io::Error::new(
                    error.kind(),
                    "another Sync process owns the HTTPS deployment lease",
                ))
            } else {
                Err(error)
            };
        }
        Ok(Self(file))
    }

    pub(crate) fn cooldown(&self) -> Result<(Cooldown, Duration)> {
        let state = Cooldown(Path::new(ROOT).join("cooldown"));
        let _ = self.0.metadata()?;
        Ok((state.clone(), state.remaining()?))
    }
}

#[derive(Clone)]
pub(crate) struct Cooldown(PathBuf);

impl Cooldown {
    fn remaining(&self) -> Result<Duration> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.0)
        {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Duration::ZERO),
            Err(e) => return Err(e.into()),
        };
        if !file.metadata()?.is_file() {
            return Err("invalid HTTPS cooldown file".into());
        }
        let mut text = String::new();
        (&mut file).take(64).read_to_string(&mut text)?;
        let deadline: u64 = text
            .strip_suffix('\n')
            .ok_or("invalid HTTPS cooldown")?
            .parse()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        Ok(Duration::from_secs(deadline.saturating_sub(now)))
    }

    pub(crate) fn record(&self, delay: Duration) -> Result<()> {
        if delay <= self.remaining()? {
            return Ok(());
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let deadline = now.saturating_add(delay.as_secs()).saturating_add(1);
        let temp = self.0.with_extension("new");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&temp)?;
        if !file.metadata()?.is_file() {
            return Err("invalid HTTPS cooldown staging file".into());
        }
        writeln!(file, "{deadline}")?;
        file.sync_all()?;
        fs::rename(&temp, &self.0)?;
        File::open(ROOT)?.sync_all()?;
        Ok(())
    }
}
