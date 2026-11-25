//! Landlock rules for the packaged rsync process.

use std::{
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::fs::OpenOptionsExt,
    },
    path::Path,
};

pub(crate) const RSYNC: &str = "/usr/bin/rsync";

#[repr(C, packed)]
struct PathBeneath {
    access: u64,
    fd: i32,
}

pub(crate) fn check(result: libc::c_long) -> io::Result<libc::c_long> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

pub(crate) fn landlock_abi() -> io::Result<libc::c_long> {
    let abi = check(unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<u8>(),
            0_usize,
            1_u32,
        )
    })?;
    if abi < 3 {
        return Err(io::Error::other("rsync requires Landlock ABI 3 or newer"));
    }
    Ok(abi)
}

pub(crate) fn child_rules(scope: &Path, writable: bool) -> io::Result<OwnedFd> {
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
    let writes = if writable {
        WRITE | REMOVE_FILE | MAKE_REGULAR | REFER | TRUNCATE
    } else {
        0
    };
    allow(scope, READ_FILE | READ_DIR | writes)?;
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
