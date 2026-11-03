//! One pending probe's exact original survives cancelled startup.
use super::*;
use zero_platform_tokio::contract::lifecycle::ChannelControl;
use zero_platform_tokio::{ControlledTcpControl, PendingTcpConnect};
pub(super) struct Probe {
    remaining: usize,
    original: Option<ControlledTcpControl>,
    confirmed: usize,
    #[cfg(test)]
    gate: Option<Arc<Gate>>,
}
#[derive(Debug)]
struct IncompleteClose(CloseReport);
impl std::fmt::Display for IncompleteClose {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "probe original close remains incomplete: {:?}",
            self.0.unfinished
        )
    }
}
impl std::error::Error for IncompleteClose {}
impl Probe {
    pub(super) fn new(attempts: NonZeroUsize) -> Self {
        Self {
            remaining: attempts.get(),
            original: None,
            confirmed: 0,
            #[cfg(test)]
            gate: None,
        }
    }
    pub(super) async fn run(
        &mut self,
        external: SocketAddr,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<bool, ChannelError> {
        self.close(deadline).await?;
        // An elapsed Ready budget cannot acquire another probe original. A
        // previous original, if any, remains owned until its real close below.
        if Instant::now() >= deadline {
            return Err(
                ChannelError::new(Stage::Prepare, Reason::Timeout, Impact::Component).with_source(
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "listener readiness deadline elapsed before the next probe",
                    ),
                ),
            );
        }
        if self.remaining == 0 || cancel.is_cancelled() {
            return Err(denied("probe start budget exhausted or revoked"));
        }
        self.remaining -= 1;
        let peer = if external.ip().is_unspecified() {
            SocketAddr::new(
                if external.is_ipv4() {
                    Ipv4Addr::LOCALHOST.into()
                } else {
                    Ipv6Addr::LOCALHOST.into()
                },
                external.port(),
            )
        } else {
            external
        };
        let (mut pending, control) = PendingTcpConnect::prepare_listener_probe(
            peer,
            cancel.child_token(),
        )
        .map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component).with_source(source)
        })?;
        self.original = Some(control);
        let result = {
            let connecting = pending.connect(deadline);
            tokio::pin!(connecting);
            #[cfg(test)]
            let first = if let Some(gate) = &self.gate {
                use std::{
                    future::{poll_fn, Future},
                    task::Poll,
                };
                let first = poll_fn(|cx| Poll::Ready(connecting.as_mut().poll(cx))).await;
                gate.pending
                    .store(first.is_pending(), std::sync::atomic::Ordering::Release);
                gate.entered.notify_one();
                match first {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => {
                        tokio::select! {biased;_=cancel.cancelled()=>return Err(ChannelError::new(Stage::Prepare,Reason::Cancelled,Impact::Component)),_=gate.release.notified()=>{}}
                        None
                    }
                }
            } else {
                None
            };
            #[cfg(not(test))]
            let first = None;
            match first {
                Some(result) => result,
                None => connecting.await,
            }
        };
        let connected = result.as_ref().is_ok_and(|socket| {
            socket.local_addr().is_ok_and(|local| local != peer)
                && socket.peer_addr().is_ok_and(|remote| remote == peer)
        });
        drop(result);
        drop(pending);
        self.close(deadline).await?;
        Ok(connected)
    }
    pub(super) async fn close(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        if let Some(original) = self.original.as_mut() {
            let report = original.wait_closed(deadline).await?;
            if !report.is_complete() {
                return Err(
                    ChannelError::new(Stage::Prepare, Reason::Timeout, Impact::Component)
                        .with_source(IncompleteClose(report)),
                );
            }
            self.confirmed += report.closed_io.len();
            self.original = None;
        }
        Ok(())
    }
    pub(super) fn take_report(&mut self) -> CloseReport {
        let count = std::mem::take(&mut self.confirmed);
        CloseReport {
            closed_io: if count == 0 {
                vec![]
            } else {
                vec![format!(
                    "{count} original readiness TCP probe close receipts"
                )]
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    pending: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl Gate {
    pub(crate) async fn entered(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
        assert!(
            self.pending.load(std::sync::atomic::Ordering::Acquire),
            "actual readiness connect must have polled Pending"
        );
    }
}
#[cfg(test)]
impl Probe {
    pub(super) fn pause(&mut self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.gate = Some(gate.clone());
        gate
    }
}
