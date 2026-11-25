use crate::{shutdown::Shutdown, upstream::Upstream};
use rfc_mcp_docker::control::{FeedFormat, FeedPoll, FeedReply, feed_snapshot, publications};
use rfc_storage::postgres::Result;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};

const CADENCE: Duration = Duration::from_secs(900);
const MIN_BACKOFF: Duration = Duration::from_secs(30);

struct Poll {
    etag: Option<String>,
    modified: Option<String>,
    publications: Option<Vec<u32>>,
    next: Instant,
    backoff: Duration,
    pending: bool,
    failed: bool,
}

impl Default for Poll {
    fn default() -> Self {
        Self {
            etag: None,
            modified: None,
            publications: None,
            next: Instant::now(),
            backoff: MIN_BACKOFF,
            pending: false,
            failed: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Broker {
    state: Arc<Mutex<[Poll; 2]>>,
    requests: mpsc::Sender<FeedFormat>,
}

impl Broker {
    pub(crate) fn new() -> (Self, mpsc::Receiver<FeedFormat>) {
        let (requests, receiver) = mpsc::channel(2);
        (
            Self {
                state: Arc::default(),
                requests,
            },
            receiver,
        )
    }

    pub(crate) fn poll(&self, request: &FeedPoll) -> FeedReply {
        let format = request.format;
        let mut state = self.state.lock().unwrap();
        let poll = &mut state[slot(format)];
        if poll.pending {
            return FeedReply::Pending;
        }
        let remaining = poll.next.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            if self.requests.try_send(format).is_ok() {
                poll.pending = true;
                return FeedReply::Pending;
            }
            return FeedReply::Retry(2);
        }
        if poll.failed {
            return FeedReply::Retry(remaining.as_secs().saturating_add(1));
        }
        match &poll.publications {
            Some(ids)
                if request
                    .known
                    .as_ref()
                    .is_some_and(|known| *known == feed_snapshot(ids.iter().copied())) =>
            {
                FeedReply::Unchanged
            }
            Some(ids) => FeedReply::Publications(ids.clone()),
            None => FeedReply::Pending,
        }
    }

    pub(crate) async fn run(
        &self,
        mut receiver: mpsc::Receiver<FeedFormat>,
        upstream: &Upstream,
        shutdown: &Shutdown,
    ) -> Result<()> {
        loop {
            let format = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                format = receiver.recv() => format.ok_or("Feed broker mailbox closed")?,
            };
            let index = slot(format);
            let (etag, modified) = {
                let state = self.state.lock().unwrap();
                (state[index].etag.clone(), state[index].modified.clone())
            };
            let result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                result = upstream.feed(format, (&etag, &modified)) => result,
            };
            let result = match result {
                Ok(capture) if !capture.unchanged => {
                    let _permit = upstream.parse_slot(shutdown).await?;
                    tokio::task::spawn_blocking(move || {
                        let ids = publications(format, &capture.bytes)?.into_iter().collect();
                        Ok(Some((ids, capture.etag, capture.last_modified)))
                    })
                    .await?
                }
                Ok(_) => Ok(None),
                Err(error) => Err(error),
            };
            let mut state = self.state.lock().unwrap();
            let poll = &mut state[index];
            poll.pending = false;
            poll.failed = result.is_err();
            if let Ok(changed) = result {
                if let Some((ids, etag, modified)) = changed {
                    poll.publications = Some(ids);
                    poll.etag = etag;
                    poll.modified = modified;
                }
                #[cfg(feature = "phase6-fixtures")]
                let cadence = if upstream.is_fixture() {
                    Duration::from_secs(3)
                } else {
                    CADENCE
                };
                #[cfg(not(feature = "phase6-fixtures"))]
                let cadence = CADENCE;
                poll.next = Instant::now() + cadence;
                poll.backoff = MIN_BACKOFF;
            } else {
                poll.next = Instant::now() + poll.backoff;
                poll.backoff = (poll.backoff * 2).min(Duration::from_secs(3600));
            }
        }
    }
}

fn slot(format: FeedFormat) -> usize {
    match format {
        FeedFormat::Atom => 0,
        FeedFormat::Rss => 1,
    }
}
