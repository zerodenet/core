//! Network replacement shares the accepted configuration transaction gate.
use std::sync::Arc;
use tokio::{
    sync::{watch, Mutex, OwnedMutexGuard},
    time::Instant,
};

pub(super) async fn wait(
    serial: &Arc<Mutex<()>>,
    changes: &mut watch::Receiver<u64>,
    generation: u64,
    retry_at: Option<Instant>,
) -> OwnedMutexGuard<()> {
    // changed() may have consumed a wake before this future was cancelled
    // waiting for the gate. Its actual generation remains unprocessed.
    if *changes.borrow() == generation {
        tokio::select! {
            Ok(()) = changes.changed() => {},
            _ = async {
                if let Some(deadline) = retry_at {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {},
        }
    }
    // Acquire within the main select future: reload/shutdown must still run
    // while the accepted apply holds this gate and awaits their real ACK.
    serial.clone().lock_owned().await
}

#[cfg(test)]
#[path = "tests/network_change.rs"]
mod tests;
