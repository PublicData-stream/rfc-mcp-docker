//! Read-only operator snapshots over a private local socket.

mod checks;
mod cli;
mod database;

pub use cli::{Command, inspect};

use crate::{
    Scope,
    diagnostics::{Run, RunSnapshot},
    egress,
    shutdown::Shutdown,
};
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    task::JoinSet,
    time::timeout,
};

const SOCKET: &str = "/rfc-https-spool/monitor.sock";
const VERSION: u8 = 1;
const PEERS: usize = 4;
const REPLY_BYTES: u64 = 16 * 1024;
const EXCHANGE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Availability {
    Available,
    Unavailable,
    TimedOut,
    AccessDenied,
    Incompatible,
    NotRequested,
}

impl Availability {
    fn label(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
            Self::TimedOut => "timed out",
            Self::AccessDenied => "access denied",
            Self::Incompatible => "incompatible reply",
            Self::NotRequested => "not requested",
        }
    }
}

impl From<io::Error> for Availability {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::PermissionDenied => Self::AccessDenied,
            io::ErrorKind::TimedOut => Self::TimedOut,
            _ => Self::Unavailable,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
struct Source<T> {
    state: Availability,
    data: Option<T>,
}

impl<T> From<Result<T, Availability>> for Source<T> {
    fn from(result: Result<T, Availability>) -> Self {
        match result {
            Ok(data) => Self {
                state: Availability::Available,
                data: Some(data),
            },
            Err(state) => Self { state, data: None },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RetryWait {
    pub job_id: i64,
    pub next_attempt_ms: i64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Snapshot {
    active: Option<RunSnapshot>,
    last: Option<RunSnapshot>,
    retry_wait: Option<RetryWait>,
}

#[derive(Default)]
struct State {
    active: Option<Run>,
    last: Option<Run>,
    retry_wait: Option<RetryWait>,
}

#[derive(Clone, Default)]
pub(crate) struct Live(Arc<Mutex<State>>);

impl Live {
    pub fn start(&self, scope: Scope, job: Option<i64>) -> Run {
        let run = Run::start(scope, job);
        self.0.lock().unwrap().active = Some(run.clone());
        run
    }

    pub fn finish(&self) {
        let mut state = self.0.lock().unwrap();
        state.last = state.active.take();
    }

    pub fn waiting(&self, retry_wait: Option<RetryWait>) {
        self.0.lock().unwrap().retry_wait = retry_wait;
    }

    fn snapshot(&self) -> Snapshot {
        let state = self.0.lock().unwrap();
        Snapshot {
            active: state.active.as_ref().map(Run::snapshot),
            last: state.last.as_ref().map(Run::snapshot),
            retry_wait: state.retry_wait,
        }
    }
}

pub(crate) struct Socket {
    listener: UnixListener,
    identity: (u64, u64),
}

impl Socket {
    /// The daemon holds the exclusive spool lease while binding and serving.
    pub fn bind() -> io::Result<Self> {
        match socket_metadata(SOCKET) {
            Ok(_) => fs::remove_file(SOCKET)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(error),
        }
        let listener = UnixListener::bind(SOCKET)?;
        let metadata = socket_metadata(SOCKET)?;
        let socket = Self {
            listener,
            identity: (metadata.dev(), metadata.ino()),
        };
        fs::set_permissions(SOCKET, fs::Permissions::from_mode(0o600))?;
        Ok(socket)
    }

    pub async fn serve(self, live: &Live, shutdown: &Shutdown) -> io::Result<()> {
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                Some(_) = clients.join_next() => {}
                connection = self.listener.accept() => {
                    let (stream, _) = connection?;
                    if clients.len() < PEERS {
                        let live = live.clone();
                        clients.spawn(timeout(EXCHANGE, answer(stream, live)));
                    }
                }
            }
        }
        clients.shutdown().await;
        Ok(())
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        if let Ok(metadata) = socket_metadata(SOCKET)
            && (metadata.dev(), metadata.ino()) == self.identity
        {
            let _ = fs::remove_file(SOCKET);
        }
    }
}

fn socket_metadata(path: &str) -> io::Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() || metadata.uid() != uid() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unexpected monitor socket entry",
        ));
    }
    Ok(metadata)
}

fn uid() -> u32 {
    // The effective UID is the same identity Linux uses for peer credentials here.
    unsafe { libc::geteuid() }
}

fn verify_peer(stream: &UnixStream) -> io::Result<()> {
    if stream.peer_cred()?.uid() != uid() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "monitor socket requires the Sync UID",
        ));
    }
    Ok(())
}

async fn answer(mut stream: UnixStream, live: Live) -> io::Result<()> {
    verify_peer(&stream)?;
    if stream.read_u8().await? != VERSION {
        return Ok(());
    }
    let bytes = serde_json::to_vec(&live.snapshot())?;
    if bytes.len() as u64 > REPLY_BYTES {
        return Err(io::Error::other("monitor reply exceeds its limit"));
    }
    stream.write_all(&bytes).await
}

async fn read_live() -> Result<Snapshot, Availability> {
    timeout(EXCHANGE, async {
        egress::private_root()?;
        if socket_metadata(SOCKET)?.mode() & 0o777 != 0o600 {
            return Err(Availability::AccessDenied);
        }
        let mut stream = UnixStream::connect(SOCKET).await?;
        verify_peer(&stream)?;
        stream.write_u8(VERSION).await?;
        let mut data = Vec::new();
        stream.take(REPLY_BYTES + 1).read_to_end(&mut data).await?;
        if data.is_empty() {
            return Err(Availability::Unavailable);
        }
        if data.len() as u64 > REPLY_BYTES {
            return Err(Availability::Incompatible);
        }
        serde_json::from_slice(&data).map_err(|_| Availability::Incompatible)
    })
    .await
    .unwrap_or(Err(Availability::TimedOut))
}
