//! Each positive fact is independent; observations and errors cannot refund IO.
use super::*;
impl Attempt {
    fn accept(
        &self,
        scope: &mut HostedScope,
        receipt: zero_platform_tokio::contract::scope::HostedExitReceipt,
    ) -> io::Result<()> {
        {
            let mut rows = lock(&self.faults.joined);
            if !rows.iter().any(|row| row.id() == receipt.id()) {
                rows.push(receipt.joined().clone());
            }
        }
        scope.acknowledge(&receipt).map_err(io::Error::other)
    }
    pub(super) async fn progress(&self, end: tokio::time::Instant) -> io::Result<()> {
        self.stop.cancel();
        let mut guard = self.scope.lock().await;
        if let Some(scope) = guard.as_mut() {
            scope.begin_shutdown();
            while !scope.is_drained() {
                let receipt = tokio::time::timeout_at(end, scope.next_receipt())
                    .await
                    .map_err(|_| self.incomplete("same client Scope joins/ACK remain retained"))?;
                if let Some(receipt) = receipt {
                    self.accept(scope, receipt)?;
                }
            }
        }
        drop(guard);
        let mut native = self.native.lock().await;
        // A newly retained canonical source may require a second observation
        // after its local acceptance; never synthesize that positive receipt.
        let mut complete = native.close(end, &self.faults).await?;
        if !complete && tokio::time::Instant::now() < end {
            complete = native.close(end, &self.faults).await?;
        }
        drop(native);
        if complete {
            self.retired.store(true, Ordering::Release);
            // Raw driver JoinHandle is independently awaited by finish/recovery
            // before its connection/workspace credit is returned.
        }
        Ok(())
    }
}
