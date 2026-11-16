//! Original prewarming slots and positive idle-retirement completion.
use super::*;
const LIFETIME: Duration = Duration::from_secs(120);
const REPLENISH: Duration = Duration::from_millis(200);
impl Native {
    pub(super) async fn worker(self: Arc<Self>, index: usize) {
        loop {
            let work = async {
                let binding = self.owner.current_root_binding().await?;
                self.epoch(&binding).await?;
                let (io, physical) = self
                    .open_one(
                        &binding,
                        CarrierContext {
                            session: None,
                            deadline: None,
                            cancel: self.stop.child_token(),
                        },
                        None,
                    )
                    .await?;
                let expires = Instant::now() + LIFETIME;
                let (taken, receipt) = tokio::sync::oneshot::channel();
                {
                    let mut queue = self.queue.lock().unwrap();
                    self.owner.verify_epoch(&binding)?;
                    if self.stop.is_cancelled() || !queue.accepting {
                        return Err(std::io::Error::other("preconnect epoch is retiring").into());
                    }
                    let usage = physical.user().map_err(std::io::Error::other)?;
                    queue.ready.push_back(Ready {
                        worker: index,
                        binding,
                        io,
                        physical: physical.clone(),
                        usage,
                        expires,
                        taken,
                    });
                }
                self.changed.notify_waiters();
                let handed = tokio::select! {biased;
                    _=self.stop.cancelled()=>false,
                    result=receipt=>result.unwrap_or(false),
                    _=tokio::time::sleep_until(expires)=>false,
                };
                let retired = {
                    let mut queue = self.queue.lock().unwrap();
                    queue
                        .ready
                        .iter()
                        .position(|ready| ready.worker == index)
                        .and_then(|position| queue.ready.remove(position))
                };
                drop(retired);
                if !handed {
                    // Keep this exact original across timeouts/errors. An
                    // expired queue view never authorizes a replacement IO.
                    loop {
                        let deadline =
                            Instant::now()
                                .checked_add(self.options.budget)
                                .ok_or_else(|| {
                                    std::io::Error::other("preconnect retirement deadline overflow")
                                })?;
                        let result = tokio::select! {biased;
                            _=self.stop.cancelled()=>break,
                            result=self.owner.retire_connection(&physical,deadline)=>result,
                        };
                        if matches!(result, Ok(true)) {
                            break;
                        }
                        if let Err(source) = result {
                            tracing::warn!(%source,"VLESS expired original remains retained");
                        }
                        tokio::select! {biased;_=self.stop.cancelled()=>break,_=tokio::time::sleep(REPLENISH)=>{}}
                    }
                }
                Ok::<(), RuntimeError>(())
            };
            let outcome =
                tokio::select! {biased;_=self.stop.cancelled()=>break,result=work=>result};
            if let Err(source) = outcome {
                if !self.stop.is_cancelled() {
                    tracing::warn!(%source,"VLESS owned pre-connect failed");
                }
            }
            tokio::select! {biased;_=self.stop.cancelled()=>break,_=tokio::time::sleep(REPLENISH)=>{}}
        }
    }
}
