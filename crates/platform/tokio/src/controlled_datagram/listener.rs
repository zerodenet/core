//! Shared server UDP views over one persistently owned original descriptor.
//! There is one receiver and close controller; cloned senders never own close.
use super::*;
use std::{future::poll_fn, net::SocketAddr};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

mod packet;
#[cfg(test)]
mod tests;

struct Shared {
    socket: ControlledDatagramSocket,
    retired: Mutex<bool>,
    writer: AsyncMutex<()>,
    sends: Semaphore,
}
impl Shared {
    fn lock(&self) -> MutexGuard<'_, bool> {
        self.retired
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn packet_failure(&self) -> Option<Arc<std_io::Error>> {
        self.socket
            .entry
            .packet_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    fn cancelled(&self) -> std_io::Error {
        self.packet_failure()
            .map_or_else(revoked, std_io::Error::other)
    }
    fn check(&self, retired: bool) -> std_io::Result<()> {
        if let Some(source) = self.packet_failure() {
            return Err(std_io::Error::other(source));
        }
        if retired || self.socket.entry.retired.is_cancelled() {
            Err(revoked())
        } else {
            Ok(())
        }
    }
}
fn revoked() -> std_io::Error {
    std_io::Error::new(std_io::ErrorKind::NotConnected, "UDP listener is revoked")
}
/// The unique receive capability. Dropping it does not revoke other views.
pub struct ControlledDatagramListenerRx {
    shared: Arc<Shared>,
}
/// Send-only clones share one native writer and explicit admission capacity.
/// No packet is copied or queued here; pending sends borrow caller storage.
#[derive(Clone)]
pub struct ControlledDatagramListenerTx {
    shared: Arc<Shared>,
}
/// The unique explicit retirement capability. Drop is not a close receipt.
/// The original remains charged to `ControlledDatagramOwner` until OS close.
pub struct ControlledDatagramListenerControl {
    shared: Arc<Shared>,
}
impl ControlledDatagramOwner {
    /// Acquire one exact listener address. The caller supplies the total bound
    /// for waiting plus in-flight sends; overload rejects new sends immediately.
    /// No peer's view Drop retires this listener or allocates another descriptor.
    pub fn bind_listener(
        &mut self,
        address: SocketAddr,
        max_pending_sends: NonZeroUsize,
    ) -> Result<
        (
            ControlledDatagramListenerRx,
            ControlledDatagramListenerTx,
            ControlledDatagramListenerControl,
        ),
        DatagramAcquisitionError,
    > {
        if max_pending_sends.get() > Semaphore::MAX_PERMITS {
            return Err(DatagramAcquisitionError {
                reservation: None,
                outcome: AcquisitionOutcome::KnownNoNative,
                source: Arc::new(std_io::Error::new(
                    std_io::ErrorKind::InvalidInput,
                    "UDP listener send capacity exceeds semaphore capacity",
                )),
            });
        }
        let socket = self.bind_native_listener(address)?;
        let shared = Arc::new(Shared {
            socket,
            retired: Mutex::new(false),
            writer: AsyncMutex::new(()),
            sends: Semaphore::new(max_pending_sends.get()),
        });
        Ok((
            ControlledDatagramListenerRx {
                shared: shared.clone(),
            },
            ControlledDatagramListenerTx {
                shared: shared.clone(),
            },
            ControlledDatagramListenerControl { shared },
        ))
    }
}
impl ControlledDatagramListenerRx {
    pub fn local_addr(&self) -> std_io::Result<SocketAddr> {
        self.shared.socket.local_addr()
    }
    pub async fn recv_from_addr(
        &mut self,
        bytes: &mut [u8],
    ) -> std_io::Result<(usize, SocketAddr)> {
        let shared = &self.shared;
        tokio::select! {
            biased;
            _ = shared.socket.entry.retired.cancelled() => Err(shared.cancelled()),
            result = poll_fn(|cx| {
                let mut prepared = match shared.socket.prepare_poll(super::wake_relay::Direction::Read, cx) {
                    Ok(prepared) => prepared,
                    Err(error) => return std::task::Poll::Ready(Err(error)),
                };
                let retired = shared.lock();
                if let Err(error) = shared.check(*retired) {
                    return std::task::Poll::Ready(Err(error));
                }
                let (result, observer) = shared.socket.poll_recv_unobserved(&prepared, bytes);
                drop(retired);
                super::observation::receive(observer, &result);
                prepared.pending(result.is_pending());
                result
            }) => result,
        }
    }
}
impl ControlledDatagramListenerTx {
    pub fn local_addr(&self) -> std_io::Result<SocketAddr> {
        self.shared.socket.local_addr()
    }
    pub async fn send_to_addr(&self, bytes: &[u8], peer: SocketAddr) -> std_io::Result<usize> {
        let shared = &self.shared;
        shared.check(*shared.lock())?;
        let _admission = shared.sends.try_acquire().map_err(|_| {
            std_io::Error::new(
                std_io::ErrorKind::WouldBlock,
                "UDP listener send capacity exhausted",
            )
        })?;
        tokio::select! {
            biased;
            _ = shared.socket.entry.retired.cancelled() => Err(shared.cancelled()),
            result = async {
                // Tokio UDP stores one writer waker. Keep only one actual
                // native send poll active; waiting clones own bounded permits.
                let _writer = shared.writer.lock().await;
                poll_fn(|cx| {
                    let mut prepared = match shared.socket.prepare_poll(super::wake_relay::Direction::Write, cx) {
                        Ok(prepared) => prepared,
                        Err(error) => return std::task::Poll::Ready(Err(error)),
                    };
                    let retired = shared.lock();
                    if let Err(error) = shared.check(*retired) {
                        return std::task::Poll::Ready(Err(error));
                    }
                    let (result, observer) = shared.socket.poll_send_unobserved(&prepared, bytes, peer);
                    drop(retired);
                    super::observation::send(observer, &result);
                    prepared.pending(result.is_pending());
                    result
                }).await
            } => result,
        }
    }
}
impl ControlledDatagramListenerControl {
    /// A persistent packet-policy restoration fault is separate from native
    /// close confirmation. Closing the original does not erase this source.
    pub fn packet_failure(&self) -> Option<Arc<std_io::Error>> {
        self.shared.packet_failure()
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.shared.socket.reservation()
    }
    /// Revoke all views, waking both native I/O and writer-lock waiters. This
    /// is admission retirement, with no claim that the descriptor has closed.
    pub fn revoke(&self) {
        let mut retired = self.shared.lock();
        *retired = true;
        drop(retired);
        self.shared.socket.entry.retire_io();
    }
    /// Serialize against actual I/O polls and close the same owned original.
    /// Unknown results retain the original source and reservation in the owner.
    pub fn close(&mut self) -> Result<DatagramCloseReceipt, Arc<std_io::Error>> {
        let mut retired = self.shared.lock();
        *retired = true;
        drop(retired);
        self.shared.socket.entry.retire_io();
        self.shared.socket.close()
    }
    /// Observation mutates the already owned original, even if callbacks panic.
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std_io::Result<()> {
        let retired = self.shared.lock();
        self.shared.check(*retired)?;
        let old = self.shared.socket.replace_observer(observer.clone())?;
        drop(retired);
        super::observation::boundary(&observer, old);
        Ok(())
    }
}
