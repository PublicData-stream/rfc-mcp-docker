//! Local operator requests share the daemon's acquisition worker.

use crate::{Scope, egress::MANUAL_SOCKET, shutdown::Shutdown};
use rfc_storage::postgres::Result;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, Permissions},
    io,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::oneshot,
    task::JoinSet,
    time::timeout,
};

const PEERS: usize = 8;
const REPLY_BYTES: u64 = 16 * 1024;
const HANDSHAKE: Duration = Duration::from_secs(3);

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Reply {
    pub success: bool,
    pub report: String,
}

type Peer = oneshot::Sender<Reply>;

struct Run {
    scope: Scope,
    peers: Vec<Peer>,
}

#[derive(Default)]
struct State {
    active: Option<Run>,
    queued: Option<Run>,
}

#[derive(Clone, Default)]
pub(crate) struct Manual(Arc<Mutex<State>>);

impl Manual {
    fn submit(&self, scope: Scope) -> Option<oneshot::Receiver<Reply>> {
        let mut state = self.0.lock().unwrap();
        let (peer, response) = oneshot::channel();
        if let Some(active) = &mut state.active
            && active.scope == scope
        {
            active.peers.retain(|peer| !peer.is_closed());
            if active.peers.len() == PEERS {
                return None;
            }
            active.peers.push(peer);
        } else if state
            .queued
            .as_ref()
            .is_none_or(|run| run.peers.iter().all(Peer::is_closed))
        {
            state.queued = Some(Run {
                scope,
                peers: vec![peer],
            });
        } else {
            return None;
        }
        Some(response)
    }

    pub(crate) fn claim(&self) -> Option<Scope> {
        let mut state = self.0.lock().unwrap();
        let mut run = state.queued.take()?;
        run.peers.retain(|peer| !peer.is_closed());
        if run.peers.is_empty() {
            return None;
        }
        let scope = run.scope;
        state.active = Some(run);
        Some(scope)
    }

    pub(crate) fn start_automatic(&self) {
        let mut state = self.0.lock().unwrap();
        let run = if state
            .queued
            .as_ref()
            .is_some_and(|run| run.scope == Scope::Full)
        {
            state.queued.take().expect("matching queued manual request")
        } else {
            Run {
                scope: Scope::Full,
                peers: Vec::new(),
            }
        };
        state.active = Some(run);
    }

    pub(crate) fn finish(&self, reply: Reply) {
        if let Some(run) = self.0.lock().unwrap().active.take() {
            for peer in run.peers {
                let _ = peer.send(reply.clone());
            }
        }
    }

    pub(crate) fn bind() -> Result<UnixListener> {
        match fs::symlink_metadata(MANUAL_SOCKET) {
            Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(MANUAL_SOCKET)?,
            Ok(_) => return Err("unexpected operator socket entry".into()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(MANUAL_SOCKET)?;
        fs::set_permissions(MANUAL_SOCKET, Permissions::from_mode(0o600))?;
        Ok(listener)
    }

    pub(crate) async fn serve(&self, listener: UnixListener, shutdown: &Shutdown) -> Result<()> {
        let mut peers = JoinSet::new();
        let result = loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break Ok(()),
                Some(_) = peers.join_next() => {}
                stream = listener.accept() => match stream {
                    Ok((stream, _)) if peers.len() < PEERS => {
                        let manual = self.clone();
                        let shutdown = shutdown.clone();
                        peers.spawn(async move { manual.handle(stream, &shutdown).await });
                    }
                    Ok((stream, _)) => drop(stream),
                    Err(error) => break Err(error.into()),
                }
            }
        };
        while peers.join_next().await.is_some() {}
        drop(listener);
        fs::remove_file(MANUAL_SOCKET)?;
        result
    }

    async fn handle(&self, mut stream: UnixStream, shutdown: &Shutdown) -> Result<()> {
        if stream.peer_cred()?.uid() != unsafe { libc::geteuid() } {
            return Err("operator socket requires the Sync UID".into());
        }
        let scope = match timeout(HANDSHAKE, stream.read_u8()).await?? {
            1 => Scope::Full,
            2 => Scope::Sample,
            _ => return Err("invalid operator scope".into()),
        };
        let reply = match self.submit(scope) {
            Some(response) => tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                _ = stream.read_u8() => return Ok(()),
                reply = response => reply?,
            },
            None => Reply {
                success: false,
                report: r#"{"state":"busy","error":"another manual reconciliation is queued"}"#
                    .to_owned(),
            },
        };
        let bytes = serde_json::to_vec(&reply)?;
        if bytes.len() as u64 > REPLY_BYTES {
            return Err("operator response exceeds bound".into());
        }
        timeout(HANDSHAKE, stream.write_all(&bytes)).await??;
        Ok(())
    }
}

pub(crate) async fn request(scope: Scope) -> Result<Option<Reply>> {
    let mut stream = match UnixStream::connect(MANUAL_SOCKET).await {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    if stream.peer_cred()?.uid() != unsafe { libc::geteuid() } {
        return Err("operator socket is not owned by the Sync UID".into());
    }
    timeout(
        HANDSHAKE,
        stream.write_u8(match scope {
            Scope::Full => 1,
            Scope::Sample => 2,
        }),
    )
    .await??;
    let mut bytes = Vec::new();
    stream.take(REPLY_BYTES + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > REPLY_BYTES {
        return Err("operator response exceeds bound".into());
    }
    if bytes.is_empty() {
        return Err("Sync closed the operator request before sending a result".into());
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn joins_matching_scope_and_reserves_one_different_scope() {
        let manual = Manual::default();
        manual.start_automatic();
        let full = manual.submit(Scope::Full).unwrap();
        let mut sample = manual.submit(Scope::Sample).unwrap();
        assert!(manual.submit(Scope::Sample).is_none());
        manual.finish(Reply {
            success: true,
            report: "full".into(),
        });
        assert_eq!(full.await.unwrap().report, "full");
        assert!(matches!(
            sample.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(manual.claim().is_some_and(|scope| scope == Scope::Sample));
        manual.finish(Reply {
            success: true,
            report: "sample".into(),
        });
        assert_eq!(sample.await.unwrap().report, "sample");
    }
}
