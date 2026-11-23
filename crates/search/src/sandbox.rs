//! Confinement for the fixed ripgrep executable.

use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind},
    net::{TcpListener, TcpStream},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd},
        unix::process::CommandExt,
    },
    process::Command,
};

const ALLOWED: &str = "/mirror/fixture.txt";
const UNSELECTED: &str = "/mirror/unselected.txt";
const SECRET: &str = "/phase0/secret.txt";
const SCRATCH: &str = "/tmp/phase0-denied";
const RG: &str = "/usr/local/bin/rg";
pub(crate) const CPU_SECONDS: u64 = 2;

/// Enter the worker sandbox and replace the probe with the pinned ripgrep.
pub fn sandbox_probe() -> io::Result<()> {
    // Positive controls establish that Docker mount and network policy alone do
    // not account for the worker denials.
    for path in [ALLOWED, UNSELECTED, SECRET, "/proc/self/status"] {
        File::open(path)?;
    }
    File::create_new(SCRATCH)?;
    fs::remove_file(SCRATCH)?;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let address = listener.local_addr()?;
    let inherited_fd = TcpStream::connect(address)?.into_raw_fd();

    let input = File::open(ALLOWED)?;
    // SAFETY: the disposable probe supplies a verified fixture as stdin.
    checked(unsafe { libc::dup2(input.as_raw_fd(), 0) }.into())?;
    drop(input);
    // Transfer the descriptor out of the wrapper before closing all inherited FDs.
    let listening_fd = listener.into_raw_fd();
    let landlock_abi = confine()?;
    for fd in [inherited_fd, listening_fd] {
        // SAFETY: F_GETFD only queries the integer descriptor.
        let result = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if result != -1 || io::Error::last_os_error().raw_os_error() != Some(libc::EBADF) {
            return Err(io::Error::other("inherited socket was not closed"));
        }
    }

    for path in [
        ALLOWED,
        UNSELECTED,
        SECRET,
        "/proc/self/status",
        "/proc/self/environ",
    ] {
        require_denial(File::open(path), path)?;
    }
    require_denial(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(SCRATCH),
        SCRATCH,
    )?;
    require_denial(TcpStream::connect(address), "new network connection")?;
    eprintln!(
        "Landlock ABI {landlock_abi}, seccomp, inherited descriptors and denial controls passed"
    );

    exec_rg("phase-zero", true, true)
}

/// Called before the serve runtime starts, in a dedicated child process.
pub fn sandbox_worker(pattern: &str, literal: bool, case_sensitive: bool) -> io::Result<()> {
    confine()?;
    exec_rg(pattern, literal, case_sensitive)
}

fn confine() -> io::Result<libc::c_long> {
    std::env::set_current_dir("/")?;
    // SAFETY: the helper is single-threaded and only stdio is supplied.
    checked(unsafe { libc::syscall(libc::SYS_close_range, 3_u32, u32::MAX, 0_u32) })?;
    no_new_privileges()?;
    let abi = install_landlock()?;
    set_limit(libc::RLIMIT_CPU, CPU_SECONDS as libc::rlim_t)?;
    set_limit(libc::RLIMIT_AS, 256 * 1024 * 1024)?;
    set_limit(libc::RLIMIT_FSIZE, 0)?;
    set_limit(libc::RLIMIT_CORE, 0)?;
    install_seccomp()?;
    Ok(abi)
}

fn exec_rg(pattern: &str, literal: bool, case_sensitive: bool) -> io::Result<()> {
    let mut command = Command::new(RG);
    command.env_clear().args([
        "--json",
        "--no-config",
        "--color",
        "never",
        "--encoding",
        "none",
        "--text",
        "--no-mmap",
        "--threads",
        "1",
        "--engine",
        "default",
    ]);
    if literal {
        command.arg("--fixed-strings");
    }
    command.arg(if case_sensitive {
        "--case-sensitive"
    } else {
        "--ignore-case"
    });
    Err(command.args(["-e", pattern, "--", "-"]).exec())
}

fn require_denial<T>(result: io::Result<T>, operation: &str) -> io::Result<()> {
    match result {
        Err(error) if error.kind() == ErrorKind::PermissionDenied => Ok(()),
        Err(error) => Err(io::Error::other(format!("{operation}: unexpected {error}"))),
        Ok(_) => Err(io::Error::other(format!(
            "{operation}: unexpectedly allowed"
        ))),
    }
}

fn checked(result: libc::c_long) -> io::Result<libc::c_long> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn no_new_privileges() -> io::Result<()> {
    // SAFETY: the fixed prctl arguments set only the calling process attribute.
    checked(unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) }.into())?;
    Ok(())
}

#[cfg(target_env = "gnu")]
type Resource = libc::__rlimit_resource_t;
#[cfg(not(target_env = "gnu"))]
type Resource = libc::c_int;

fn set_limit(resource: Resource, limit: libc::rlim_t) -> io::Result<()> {
    let limit = libc::rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };
    // SAFETY: the rlimit pointer lives for the call.
    checked(unsafe { libc::setrlimit(resource, &limit) }.into())?;
    Ok(())
}

#[repr(C, packed)]
struct PathBeneath {
    allowed_access: u64,
    parent_fd: i32,
}

fn install_landlock() -> io::Result<libc::c_long> {
    const EXECUTE: u64 = 1 << 0;
    const READ_FILE: u64 = 1 << 2;
    // SAFETY: the ABI query expects a null attribute, size zero and VERSION flag.
    let abi = checked(unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<u8>(),
            0_usize,
            1_u32,
        )
    })?;
    if abi < 3 {
        return Err(io::Error::other(format!(
            "Landlock ABI {abi} lacks truncate mediation"
        )));
    }

    // Filesystem rights through TRUNCATE (ABI 3) and IOCTL_DEV (ABI 5).
    let handled_fs: u64 = (1 << if abi >= 5 { 16 } else { 15 }) - 1;
    // SAFETY: the first u64 of landlock_ruleset_attr is handled_access_fs.
    let ruleset = checked(unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &handled_fs,
            size_of::<u64>(),
            0_u32,
        )
    })?;
    // SAFETY: a successful create call returned a new owned descriptor.
    let ruleset = unsafe { File::from_raw_fd(ruleset as i32) };
    let path = CString::new(RG).expect("fixed path has no NUL");
    // SAFETY: path is a valid C string and open creates an owned descriptor.
    let fd = checked(
        unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        }
        .into(),
    )?;
    // SAFETY: this is a fresh descriptor returned by open above.
    let anchor = unsafe { File::from_raw_fd(fd as i32) };
    let rule = PathBeneath {
        allowed_access: READ_FILE | EXECUTE,
        parent_fd: anchor.as_raw_fd(),
    };
    // SAFETY: the packed argument matches landlock_path_beneath_attr.
    checked(unsafe {
        libc::syscall(
            libc::SYS_landlock_add_rule,
            ruleset.as_raw_fd(),
            1_u32,
            &rule,
            0_u32,
        )
    })?;
    // SAFETY: the ruleset descriptor stays live through the call.
    checked(unsafe {
        libc::syscall(libc::SYS_landlock_restrict_self, ruleset.as_raw_fd(), 0_u32)
    })?;
    Ok(abi)
}

fn install_seccomp() -> io::Result<()> {
    // Audit architectures and seccomp return actions from Linux UAPI.
    #[cfg(target_arch = "x86_64")]
    const ARCH: u32 = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    const ARCH: u32 = 0xc000_00b7;
    const ALLOW: u32 = 0x7fff_0000;
    const KILL_PROCESS: u32 = 0x8000_0000;
    const DENY: u32 = 0x0005_0000 | libc::EPERM as u32;

    // Only process start-up, local file inspection, memory, signals and stdio
    // are needed by static, single-threaded ripgrep. Unknown syscalls fail.
    let mut syscalls = vec![
        libc::SYS_read,
        libc::SYS_write,
        libc::SYS_readv,
        libc::SYS_writev,
        libc::SYS_close,
        libc::SYS_ppoll,
        libc::SYS_openat,
        libc::SYS_newfstatat,
        libc::SYS_fstat,
        libc::SYS_statx,
        libc::SYS_getdents64,
        libc::SYS_readlinkat,
        libc::SYS_faccessat,
        libc::SYS_faccessat2,
        libc::SYS_lseek,
        libc::SYS_pread64,
        libc::SYS_fcntl,
        libc::SYS_ioctl,
        libc::SYS_mmap,
        libc::SYS_mprotect,
        libc::SYS_munmap,
        libc::SYS_madvise,
        libc::SYS_brk,
        libc::SYS_rt_sigaction,
        libc::SYS_rt_sigprocmask,
        libc::SYS_rt_sigreturn,
        libc::SYS_sigaltstack,
        libc::SYS_set_tid_address,
        libc::SYS_set_robust_list,
        libc::SYS_futex,
        libc::SYS_rseq,
        libc::SYS_sched_getaffinity,
        libc::SYS_prlimit64,
        libc::SYS_getrandom,
        libc::SYS_clock_gettime,
        libc::SYS_getpid,
        libc::SYS_gettid,
        libc::SYS_getcwd,
        libc::SYS_chdir,
        libc::SYS_execve,
        libc::SYS_exit,
        libc::SYS_exit_group,
    ];
    #[cfg(target_arch = "x86_64")]
    syscalls.extend([
        libc::SYS_arch_prctl,
        libc::SYS_poll,
        libc::SYS_open,
        libc::SYS_stat,
        libc::SYS_lstat,
        libc::SYS_access,
        libc::SYS_readlink,
    ]);

    let instruction = |code, jt, jf, k| libc::sock_filter { code, jt, jf, k };
    let mut filter = vec![
        instruction(0x20, 0, 0, 4), // seccomp_data.arch
        instruction(0x15, 1, 0, ARCH),
        instruction(0x06, 0, 0, KILL_PROCESS),
        instruction(0x20, 0, 0, 0), // seccomp_data.nr
    ];
    for syscall in syscalls {
        filter.push(instruction(0x15, 0, 1, syscall as u32));
        filter.push(instruction(0x06, 0, 0, ALLOW));
    }
    filter.push(instruction(0x06, 0, 0, DENY));
    let program = libc::sock_fprog {
        len: u16::try_from(filter.len()).expect("fixed seccomp program fits"),
        filter: filter.as_mut_ptr(),
    };
    // SAFETY: the kernel copies the initialized BPF program during this call.
    checked(unsafe { libc::syscall(libc::SYS_seccomp, 1_u32, 0_u32, &program) })?;
    Ok(())
}
