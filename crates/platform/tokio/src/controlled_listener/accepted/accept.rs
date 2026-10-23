use super::*;
use std::task::{Context, Waker};
impl ControlledTcpListener {
    /// Same persistent owner; finite library opt-in. No handshake or task starts.
    pub async fn accept_owned(&mut self) -> Result<(AcceptedTcp, SocketAddr), ChannelError> {
        self.accept_owned_with(|| {}).await
    }
    pub(super) async fn accept_owned_with(
        &mut self,
        mut installed: impl FnMut(),
    ) -> Result<(AcceptedTcp, SocketAddr), ChannelError> {
        let pool = self.shared.accepted.clone().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Unsupported,
                "accepted TCP authority was not installed",
            )
        })?;
        let reservation = pool.reserve()?;
        let waiter = pool.relay.waiter();
        let cancelled = self.shared.cancel.clone().cancelled_owned();
        tokio::pin!(cancelled);
        poll_fn(|cx| {
            waiter.publish(cx.waker());
            let waker = Waker::from(pool.relay.clone());
            let cx = &mut Context::from_waker(&waker);
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(failure(
                    Stage::Prepare,
                    Reason::Revoked,
                    "accepted TCP admission revoked",
                )));
            }
            let _critical = pool.relay.enter();
            let state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.retired || self.shared.cancel.is_cancelled() {
                return Poll::Ready(Err(failure(
                    Stage::Prepare,
                    Reason::Revoked,
                    "accepted TCP admission revoked",
                )));
            }
            let Some(listener) = state.listener.as_ref() else {
                return Poll::Ready(Err(failure(
                    Stage::Prepare,
                    Reason::Revoked,
                    "native listener unavailable",
                )));
            };
            // Preinstalled before Tokio's native accept AND its consuming child
            // registration. An unwind leaves this same sentinel charged.
            reservation.entry.state().phase = Phase::Acquiring;
            match listener.poll_accept(cx) {
                Poll::Pending => {
                    reservation.entry.state().phase = Phase::Waiting;
                    Poll::Pending
                }
                Poll::Ready(Err(source)) => {
                    let mut error =
                        ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                            .with_source(source);
                    error.delivery = DeliveryState::Unknown;
                    Poll::Ready(Err(pool.remember(&reservation.entry, error, true)))
                }
                Poll::Ready(Ok((socket, peer))) => {
                    let (io, control) = ControlledTcpStream::from_accepted(
                        socket,
                        reservation.entry.cancel.clone(),
                    );
                    let installed_control = reservation.entry.control.set(AsyncMutex::new(control));
                    assert!(
                        installed_control.is_ok(),
                        "one actual accepted control per reservation"
                    );
                    reservation.entry.state().phase = Phase::Installed;
                    let io = AcceptedTcpStream {
                        io,
                        _usage: reservation.entry.users.as_ref().map(|users| users.view()),
                    };
                    // No original getter, configuration or external code ran
                    // before installing this exact control in the same entry.
                    installed();
                    if let Err(source) = io.io.configure_accepted() {
                        let error =
                            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                                .with_source(source);
                        return Poll::Ready(Err(pool.remember(&reservation.entry, error, false)));
                    }
                    pool.state().transferred = true;
                    Poll::Ready(Ok((
                        AcceptedTcp {
                            io,
                            claim: AcceptedTcpClaim {
                                entry: reservation.entry.clone(),
                            },
                        },
                        peer,
                    )))
                }
            }
        })
        .await
    }
}
