//! Queue publication is fenced separately from awaited native epoch cleanup.
use super::*;
impl Native {
    pub(super) async fn epoch(&self, binding: &UpstreamBinding) -> Result<(), RuntimeError> {
        {
            let queue = self.queue.lock().unwrap();
            if !self.stop.is_cancelled()
                && queue.accepting
                && self.owner.verify_epoch(binding).is_ok()
            {
                return Ok(());
            }
        }
        // Serialize the invalidate/prepare/publish operation, never the ready
        // queue or a physical open. An older completed preparation cannot reopen
        // publication after another transition has already cleared its users.
        let _transition = tokio::select! {biased;
            _=self.stop.cancelled()=>return Err(std::io::Error::other("preconnect stopped").into()),
            transition=self.transition.lock()=>transition,
        };
        let retired = {
            let mut queue = self.queue.lock().unwrap();
            if self.stop.is_cancelled() {
                return Err(std::io::Error::other("preconnect stopped").into());
            }
            if queue.accepting && self.owner.verify_epoch(binding).is_ok() {
                return Ok(());
            }
            // Fence queue publication before releasing old logical views. The
            // actual ManagedPool serializes epoch changes and original insertion;
            // this queue must never remain locked while that cleanup awaits IO.
            queue.accepting = false;
            std::mem::take(&mut queue.ready)
        };
        drop(retired);
        self.owner
            .prepare_epoch(&self.options.profile, binding)
            .await?;
        let mut queue = self.queue.lock().unwrap();
        if self.stop.is_cancelled() {
            return Err(std::io::Error::other("preconnect stopped").into());
        }
        self.owner.verify_epoch(binding)?;
        queue.accepting = true;
        Ok(())
    }
}
