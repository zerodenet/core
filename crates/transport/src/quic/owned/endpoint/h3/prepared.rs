//! The original H3 handshake/lifecycle future, registered before its first poll.
use super::*;
use crate::quic::owned::{DriverBody, OwnedDriverOrigin};
use std::future::poll_fn;
use tokio::sync::oneshot;
use zero_core::component::TaskId;
use zero_platform_tokio::{
    contract::{scope::TaskClass, CancellationToken, Impact, Reason, Stage},
    controlled_datagram::metadata::users::drivers::WeakDrivers,
};

/// Actual H3 request senders/requests contain only sealed connection/streams.
pub type RequestSender = ::h3::client::SendRequest<OpenStreams, Bytes>;
pub struct PendingHttp3 {
    ready: Option<oneshot::Receiver<RequestSender>>,
    task: TaskId,
    original: WeakDrivers<OwnedDriverOrigin, DriverBody>,
}
impl PendingHttp3 {
    pub fn task_id(&self) -> TaskId {
        self.task
    }
    /// Cancel-safe wait: the original handoff remains owned on interruption.
    /// The separate original DriverExitReceiver must still receive real joins.
    pub async fn ready(&mut self) -> Result<RequestSender, ChannelError> {
        let receiver = self
            .ready
            .as_mut()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        match receiver.await {
            Ok(sender) => {
                self.ready.take();
                Ok(sender)
            }
            Err(source) => {
                self.ready.take();
                if let Some(outcome) = self
                    .original
                    .upgrade()
                    .and_then(|owner| owner.body(self.task))
                    .and_then(|body| body.http3.clone())
                {
                    return Err(outcome_error(outcome));
                }
                // A panic before a typed body is recorded is not reconstructed
                // from text or claimed as a successful driver. Its actual
                // native task result is retained by the original witness/map.
                Err(error(Stage::Handshake, Reason::Internal).with_source(source))
            }
        }
    }
}
impl super::super::OwnedEndpointOwner {
    /// The input must be a handle from this same actual graph. No alternate
    /// native socket, scope, runtime, pool or default task allowance is created.
    pub fn prepare_http3(&self, connection: OwnedConnection) -> Result<PendingHttp3, ChannelError> {
        if !Arc::ptr_eq(connection.permit.users(), &self.shared.users) {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        self.shared.check()?;
        let (handoff, receiver) = oneshot::channel();
        // The outer graph's real id/witness must be installed before the
        // protocol handshake is polled, even on a multi-thread runtime. This
        // single zero-payload registration gate creates no task or work queue.
        let (installed, accepted) = oneshot::channel();
        let cancel = self.shared.users.cancellation().clone();
        let callbacks = self.native.callbacks().clone();
        let ledger = self.drivers.application_ledger();
        let task = ledger
            .spawn(
                OwnedDriverOrigin::Http3,
                TaskClass::Auxiliary,
                |slot| async move {
                    // Same metadata callback broker/poll boundary as the actual Quinn
                    // drivers. Original observer panics resume outside library locks.
                    let outcome = if accepted.await.is_ok() {
                        crate::quic::owned::callbacks::Boundary::new(
                            run(connection, cancel, handoff),
                            callbacks,
                        )
                        .await
                    } else {
                        Http3DriverExit::HandoffDropped
                    };
                    let outcome = Arc::new(outcome);
                    slot.record(DriverBody {
                        origin: OwnedDriverOrigin::Http3,
                        io_error: None,
                        close_reason: None,
                        http3: Some(outcome.clone()),
                    });
                    match outcome.as_ref() {
                        Http3DriverExit::Handshake(_) => Err(outcome_error(outcome)),
                        Http3DriverExit::Closed(source) if !source.is_h3_no_error() => {
                            Err(outcome_error(outcome))
                        }
                        _ => Ok(()),
                    }
                },
            )
            .map_err(|_| {
                self.shared
                    .admission
                    .check()
                    .expect_err("same original rejection is sticky")
            })?;
        // This is the actual successful ledger registration return. No
        // synthetic TaskId or callback observation can open this gate.
        let _ = installed.send(());
        Ok(PendingHttp3 {
            ready: Some(receiver),
            task,
            original: ledger.downgrade(),
        })
    }
}
async fn run(
    connection: OwnedConnection,
    cancel: CancellationToken,
    mut handoff: oneshot::Sender<RequestSender>,
) -> Http3DriverExit {
    let handshake = ::h3::client::new(Connection::new(connection));
    tokio::pin!(handshake);
    let (mut driver, sender) = tokio::select! { biased;
        _ = cancel.cancelled() => return Http3DriverExit::Cancelled,
        _ = handoff.closed() => return Http3DriverExit::HandoffDropped,
        result = &mut handshake => match result {
            Ok(value) => value,
            Err(source) => return Http3DriverExit::Handshake(Arc::new(source)),
        },
    };
    if handoff.send(sender).is_err() {
        return Http3DriverExit::HandoffDropped;
    }
    tokio::select! { biased;
        _ = cancel.cancelled() => Http3DriverExit::Cancelled,
        source = poll_fn(|cx| driver.poll_close(cx)) => Http3DriverExit::Closed(Arc::new(source)),
    }
}
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
fn outcome_error(outcome: Arc<Http3DriverExit>) -> ChannelError {
    let (stage, reason) = match outcome.as_ref() {
        Http3DriverExit::Handshake(_) => (Stage::Handshake, Reason::Protocol),
        Http3DriverExit::Closed(_) => (Stage::Close, Reason::Protocol),
        Http3DriverExit::Cancelled => (Stage::Handshake, Reason::Cancelled),
        Http3DriverExit::HandoffDropped => (Stage::Handshake, Reason::Revoked),
    };
    error(stage, reason).with_source(Http3DriverFailure(outcome))
}
