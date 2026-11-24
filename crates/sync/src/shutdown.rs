use rfc_storage::postgres::Result;
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct Shutdown(watch::Sender<bool>);

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self(watch::channel(false).0)
    }

    pub(crate) fn stop(&self) {
        self.0.send_replace(true);
    }

    pub(crate) fn check(&self) -> Result<()> {
        if *self.0.borrow() {
            Err("reconciliation cancelled".into())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn cancelled(&self) {
        let _ = self.0.subscribe().wait_for(|cancelled| *cancelled).await;
    }

    pub(crate) async fn until<T>(
        &self,
        deadline: tokio::time::Instant,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.cancelled() => Err("reconciliation cancelled".into()),
            _ = tokio::time::sleep_until(deadline) => Err("reconciliation deadline exceeded".into()),
            result = work => result,
        }
    }
}
