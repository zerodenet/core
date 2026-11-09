//! Drive the existing association controller and its retained originals.
use super::*;
use zero_platform_tokio::contract::lifecycle::DynChannelControl;
impl DynChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.0.cancel(reason);
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.cancel(CloseReason::Shutdown);
            let _cleanup = tokio::time::timeout_at(end, self.0.cleanup.lock())
                .await
                .map_err(|source| admission_error(Reason::Timeout).with_source(source))?;
            let (preparing, protocol) = {
                let state = self.0.state.lock().unwrap();
                (state.preparing, state.protocol.clone())
            };
            if preparing {
                return Ok(CloseReport {
                    unfinished: vec![zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                        "actual Datagram preparation still running".into(),
                    )],
                    ..Default::default()
                });
            }
            if let Some(protocol) = protocol {
                let mut actual = tokio::time::timeout_at(end, protocol.lock())
                    .await
                    .map_err(|source| admission_error(Reason::Timeout).with_source(source))?;
                let report = actual.wait_closed(end).await?;
                let complete = report.is_complete();
                self.0.state.lock().unwrap().report.append(report);
                if !complete {
                    return Ok(std::mem::take(&mut self.0.state.lock().unwrap().report));
                }
                let returned = self.0.state.lock().unwrap().protocol.take();
                drop(returned);
            }
            // The actual logical protocol view was returned above. Observe
            // every same child receiver/global-return gate before refunding
            // this association; this wait never certifies its caller Scope.
            if let Some(children) = &self.0.protocols {
                if !children.wait_returned(end).await? {
                    let mut report = std::mem::take(&mut self.0.state.lock().unwrap().report);
                    report.unfinished.push(
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "same association protocol originals remain owned".into(),
                        ),
                    );
                    return Ok(report);
                }
            }
            let pending = self.0.state.lock().unwrap().pending.clone();
            for original in pending {
                let report = original.retire(end).await?;
                let complete = report.is_complete()
                    && original
                        .closed()
                        .is_some_and(|closed| closed.matches(&original))
                    && original
                        .users_closed()
                        .is_some_and(|users| users.matches(&original));
                self.0.state.lock().unwrap().report.append(report);
                if !complete {
                    let mut report = std::mem::take(&mut self.0.state.lock().unwrap().report);
                    report.unfinished.push(
                        zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                            "actual pending Datagram carrier remains owned".into(),
                        ),
                    );
                    return Ok(report);
                }
                let removed = {
                    let mut state = self.0.state.lock().unwrap();
                    state
                        .pending
                        .iter()
                        .position(|actual| actual.same_slot(&original))
                        .map(|index| state.pending.remove(index))
                };
                drop(removed);
            }
            Ok(std::mem::take(&mut self.0.state.lock().unwrap().report))
        })
    }
}
