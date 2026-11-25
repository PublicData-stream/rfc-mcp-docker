use rfc_mcp_docker::{
    ProbeResult,
    control::{
        self, Client, FeedFormat, FeedPoll, FeedReply, Keys, Link, SyncRequested, feed_snapshot,
        jitter, unix_seconds,
    },
};
use std::{collections::BTreeSet, env, time::Duration};
use tokio::time::sleep;

const MIN_RETRY: Duration = Duration::from_secs(30);
const MAX_RETRY: Duration = Duration::from_secs(3600);
enum Poll {
    Pending,
    Unchanged,
    Changed(BTreeSet<u32>),
    RetryAfter(Duration),
}

struct Feed {
    format: FeedFormat,
    interval: Duration,
    min_retry: Duration,
    acknowledged: BTreeSet<u32>,
    snapshot: Option<String>,
    pending: Option<(BTreeSet<u32>, Vec<SyncRequested>)>,
}

impl Feed {
    fn new() -> control::Result<Self> {
        let format = match env::var("RFC_FEED_FORMAT").as_deref() {
            Ok("atom") | Err(env::VarError::NotPresent) => FeedFormat::Atom,
            Ok("rss") => FeedFormat::Rss,
            _ => return Err("RFC_FEED_FORMAT requires atom or rss".into()),
        };
        let interval = Duration::from_secs(900);
        let min_retry = MIN_RETRY;
        #[cfg(feature = "phase6-fixtures")]
        let (interval, min_retry) = if env::var("RFC_DISCOVERY_FIXTURES").as_deref() == Ok("1") {
            (Duration::from_secs(3), Duration::from_secs(1))
        } else {
            (interval, min_retry)
        };
        Ok(Self {
            format,
            interval,
            min_retry,
            acknowledged: BTreeSet::new(),
            snapshot: None,
            pending: None,
        })
    }

    async fn poll(&mut self, client: &mut Client) -> control::Result<Poll> {
        let request = FeedPoll {
            format: self.format,
            known: self.snapshot.clone(),
        };
        Ok(match client.poll_feed(&request).await? {
            FeedReply::Unchanged => Poll::Unchanged,
            FeedReply::Pending => Poll::Pending,
            FeedReply::Retry(seconds) => Poll::RetryAfter(Duration::from_secs(seconds)),
            FeedReply::Publications(ids) => Poll::Changed(ids.into_iter().collect()),
        })
    }

    async fn cycle(&mut self, client: &mut Client) -> control::Result<Option<Duration>> {
        if self.pending.is_none() {
            let publications = match self.poll(client).await? {
                Poll::Pending => return Ok(Some(Duration::from_secs(2))),
                Poll::Unchanged => return Ok(None),
                Poll::RetryAfter(duration) => return Ok(Some(duration)),
                Poll::Changed(publications) => publications,
            };
            let now = unix_seconds()?;
            let events = publications
                .difference(&self.acknowledged)
                .map(|&rfc| SyncRequested::publication(rfc, now))
                .collect();
            self.pending = Some((publications, events));
        }
        let (_, events) = self.pending.as_mut().expect("pending feed poll");
        while let Some(event) = events.last() {
            client.request_sync(event).await?;
            events.pop();
        }
        let (publications, _) = self.pending.take().expect("completed feed poll");
        self.snapshot = Some(feed_snapshot(publications.iter().copied()));
        self.acknowledged = publications;
        Ok(None)
    }
}

pub async fn run() -> ProbeResult {
    let mut feed = Feed::new()?;
    let target = control::address(&env::var("RFC_FEED_SYNC_ADDR")?)?;
    let mut client = Client::new(target, Link::FeedSync, Keys::load(Link::FeedSync)?);
    let mut retry = feed.min_retry;
    loop {
        match feed.cycle(&mut client).await {
            Ok(None) => {
                retry = feed.min_retry;
                sleep(jitter(feed.interval)).await;
            }
            Ok(Some(delay)) => sleep(jitter(delay)).await,
            Err(_) => {
                eprintln!("feed poll or hint delivery failed");
                sleep(jitter(retry)).await;
                retry = (retry * 2).min(MAX_RETRY);
            }
        }
    }
}
