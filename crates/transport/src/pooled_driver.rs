//! One inert connection future; the actual owner chooses its task registrar.
use std::{future::Future, pin::Pin};

mod control;
pub mod datagram;
pub mod datagram_queue;
mod epoch;
mod lifecycle;
mod loan;
mod managed;
mod physical;
mod stream;
pub use control::{PoolControl, PoolOptions};
pub use lifecycle::PoolFactory;
pub use managed::ManagedPool;
pub use physical::{Physical, PoolUse};
pub use stream::{ExclusiveIo, UserStream};

fn failure(
    instance: zero_core::component::ComponentInstanceId,
    stage: zero_platform_tokio::contract::Stage,
    reason: zero_platform_tokio::contract::Reason,
) -> zero_platform_tokio::contract::ChannelError {
    let mut error = zero_platform_tokio::contract::ChannelError::new(
        stage,
        reason,
        zero_platform_tokio::contract::Impact::Component,
    );
    error.component = Some(instance.component);
    error
}

pub type Driver = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct Exit<F: FnOnce()>(Option<F>);
impl<F: FnOnce()> Drop for Exit<F> {
    fn drop(&mut self) {
        if let Some(exit) = self.0.take() {
            exit();
        }
    }
}

/// Joining these protocol-owned futures spawns no child tasks. Dropping a
/// pending driver invalidates its logical channels; it proves no native close.
pub fn join<R, W, I, F>(read: R, write: W, idle: I, exit: F) -> Driver
where
    R: Future<Output = ()> + Send + 'static,
    W: Future<Output = ()> + Send + 'static,
    I: Future<Output = ()> + Send + 'static,
    F: FnOnce() + Send + 'static,
{
    // Install before the future is polled, including failed task registration.
    let exit = Exit(Some(exit));
    Box::pin(async move {
        let _exit = exit;
        tokio::join!(read, write, idle);
    })
}

/// A logical stream and the lineage of its actual registered physical driver.
/// The origin is read-only metadata; it does not retain native ownership.
pub struct PooledStream {
    pub io: zero_platform_tokio::contract::stream::BoxStream,
    pub origin: zero_platform_tokio::controlled_carrier::CarrierOrigin,
}

#[cfg(test)]
#[path = "../tests/pooled_driver/mod.rs"]
mod tests;
