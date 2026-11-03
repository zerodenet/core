//! An original logical-session controller; it never closes a shared underlay.
use super::*;
use crate::inbound::multiplex::{stream::Status, writer::Command};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl, Unfinished},
        BoxFuture, CloseReason,
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};

struct Control {
    id: u32,
    status: Arc<Status>,
    outgoing: mpsc::Sender<Command>,
    dropped: mpsc::UnboundedSender<(u32, Arc<Status>)>,
    driver: zero_platform_tokio::contract::scope::TaskJoinWitness,
    user: Option<super::super::pool::managed::UserLease>,
    work: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    done: bool,
}
impl Drop for Control {
    fn drop(&mut self) {
        if !self.done {
            // Abandonment requests the same real reader retirement as ordinary
            // stream Drop. It cannot issue LoanClosed, a barrier ACK, a driver
            // exit or a native receipt. Retained explicit close still owns the
            // ordered operation; this only prevents an orphan if it is dropped.
            let _ = self.dropped.send((self.id, self.status.clone()));
        }
    }
}
impl NativeTcpStream {
    pub fn original_carrier(&self) -> Option<&CarrierOrigin> {
        self.original.as_ref()
    }
    /// Transfer this actual session and a retained user debt to its logical
    /// controller. Existing stream Drop behavior is unchanged for other callers.
    pub fn into_carrier_loan(mut self) -> io::Result<StreamLoan> {
        let original = self.original.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "Mieru stream has no original native lineage",
            )
        })?;
        if original.is_retired() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let driver = original.driver().cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "Mieru stream has no actual owning driver",
            )
        })?;
        let control = Control {
            id: self.stream.id,
            status: self.stream.status.clone(),
            outgoing: self.stream.outgoing.clone(),
            dropped: self.stream.dropped.clone(),
            driver,
            user: Some(self.user.fork()?),
            work: None,
            done: false,
        };
        // The new persistent controller now owns the real retirement operation.
        // Returning the view alone must not abandon its ordered close barrier.
        self.stream.defer_retirement();
        Ok(StreamLoan::new(Box::pin(self), Box::new(control), original))
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        // The outer LoanOwner revokes its view. Do not cancel the shared driver
        // or remove the session ahead of this controller's ordered barrier.
    }
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            if self.done {
                return Ok(CloseReport::default());
            }
            if self.work.is_none() {
                let id = self.id;
                let status = self.status.clone();
                let outgoing = self.outgoing.clone();
                let dropped = self.dropped.clone();
                let driver = self.driver.clone();
                self.work = Some(Box::pin(async move {
                    let (done, receive) = oneshot::channel();
                    let barrier = async {
                        if outgoing
                            .send(Command::Barrier {
                                id,
                                close: true,
                                status: status.clone(),
                                done,
                            })
                            .await
                            .is_err()
                        {
                            return false;
                        }
                        match receive.await {
                            Ok(Ok(())) => true,
                            // A real ordered acknowledgement of an already
                            // closed session settles ownership, not delivery.
                            Ok(Err(_)) => status.closed(),
                            Err(_) => false,
                        }
                    };
                    // Reader removal alone cannot settle queued writer state.
                    // Require the actual ordered reply, or actual driver join.
                    tokio::select! { biased;
                        _ = driver.wait() => {},
                        settled = barrier => if !settled { let _=driver.wait().await; },
                    }
                    let _ = dropped.send((id, status.clone()));
                    status.wait_removed().await;
                }));
            }
            if tokio::time::timeout_at(deadline, self.work.as_mut().unwrap().as_mut())
                .await
                .is_err()
            {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::ReliableState(
                        "mieru original logical close".into(),
                    )],
                    ..Default::default()
                });
            }
            self.done = true;
            self.work = None;
            drop(self.user.take());
            Ok(CloseReport {
                released_resources: vec!["mieru original logical carrier returned".into()],
                ..Default::default()
            })
        })
    }
}
