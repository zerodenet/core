//! Library-only finite supervision of one original controlled H2 carrier.
//! This owns no pool and activates no production XHTTP path. The caller retains
//! its persistent CarrierOwner and must join the real task, then confirm native
//! closure through that owner. Dropping a pending handshake refunds nothing.
mod driver;
mod evidence;
mod executor;
mod failure;
mod observed_io;
mod receiver;
pub use receiver::LocalTaskReceiver;
mod sender;
pub use evidence::{H2ConnectionFailure, IoOperation};
use failure::Failure;
pub use sender::{ResponseBody, Sender};
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::oneshot;
use zero_core::component::TaskId;
use zero_platform_tokio::{
    contract::{
        lifecycle::TaskClass,
        scope::{TaskJoinWitness, TaskRegistrar},
        CancellationToken, ChannelError, Stage,
    },
    controlled_carrier::ControlledCarrier,
};

/// Explicit per-connection limits and lifetime. There is no default authority.
pub struct Options {
    /// Counts both queued and currently polled original executor futures.
    pub delegate_limit: NonZeroUsize,
    pub settings: Option<Vec<u8>>,
    /// Uses the existing prepared H2 keepalive interpretation without resetting it.
    pub keepalive: i64,
    /// Actual caller/owner lifetime; never synthesized from the first query.
    pub cancel: CancellationToken,
}

pub struct PendingH2<B> {
    receiver: Option<oneshot::Receiver<hyper::client::conn::http2::SendRequest<B>>>,
    failure: Arc<Failure>,
    id: TaskId,
    witness: TaskJoinWitness,
}
impl<B> PendingH2<B> {
    pub fn task_id(&self) -> TaskId {
        self.id
    }
    /// Observation of this actual aggregate's JoinSet consumption, not I/O close.
    pub fn join_witness(&self) -> &TaskJoinWitness {
        &self.witness
    }
    /// Keep this receiver in the actual caller owner before dropping request
    /// handles. Its explicit cleanup acceptance is separate from native close.
    pub fn local_task_receiver(&self) -> LocalTaskReceiver {
        LocalTaskReceiver::new(self.failure.clone(), self.witness.clone())
    }
    /// Bounded original diagnostic evidence, independent of task/native close.
    pub fn failure_evidence(&self) -> &H2ConnectionFailure {
        self.failure.evidence()
    }
    /// Cancelling this wait leaves the original receiver and owner intact.
    pub async fn ready(&mut self) -> Result<Sender<B>, Arc<ChannelError>> {
        let receiver = self.receiver.as_mut().ok_or_else(|| {
            self.failure.local(
                Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                std::io::Error::other("H2 handshake result was already taken"),
            )
        })?;
        let sender = self.failure.run(Stage::Handshake, receiver).await?;
        self.receiver.take();
        Ok(Sender::new(sender, self.failure.clone()))
    }
}

/// Register before the first handshake poll. All executor delegates are polled
/// inside this same task; no executor fallback task/thread/runtime is created.
pub fn spawn<B>(
    carrier: ControlledCarrier,
    registrar: &TaskRegistrar,
    class: TaskClass,
    options: Options,
) -> Result<PendingH2<B>, ChannelError>
where
    B: hyper::body::Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (io, adoption) = carrier.into_parts();
    let failure = Arc::new(Failure::new(adoption.instance()));
    if options.delegate_limit.get() > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(failure::forwarded(failure.local(
            Stage::Prepare,
            zero_platform_tokio::contract::Reason::InvalidConfig,
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "H2 delegate budget exceeds channel capacity",
            ),
        )));
    }
    let (handoff, receiver) = oneshot::channel();
    let task_failure = failure.clone();
    let (id, witness) = adoption.spawn_driver(registrar, class, io.into_driver(), move |io| {
        driver::run(io, options, task_failure, handoff)
    })?;
    failure.install_witness(witness.clone());
    Ok(PendingH2 {
        receiver: Some(receiver),
        failure,
        id,
        witness,
    })
}

#[cfg(test)]
#[path = "../../tests/xhttp/managed_h2/mod.rs"]
mod tests;
