//! Test observes the actual original receive poll; it proves no close/authority.
use super::*;
#[cfg(test)]
pub(super) type Probe = Arc<AtomicBool>;
#[cfg(not(test))]
pub(super) type Probe = ();
#[cfg(test)]
pub(super) fn new() -> Probe {
    Arc::new(AtomicBool::new(false))
}
#[cfg(not(test))]
pub(super) fn new() -> Probe {}
#[cfg(test)]
pub(super) fn clone_probe(probe: &Probe) -> Probe {
    probe.clone()
}
#[cfg(not(test))]
pub(super) fn clone_probe(_: &Probe) -> Probe {}
pub(super) async fn receive(
    io: &BorrowedDatagram,
    bytes: &mut [u8],
    probe: &Probe,
) -> io::Result<usize> {
    #[cfg(test)]
    {
        let receiving = io.receive(bytes);
        tokio::pin!(receiving);
        std::future::poll_fn(|cx| {
            let ready = std::future::Future::poll(receiving.as_mut(), cx);
            probe.store(ready.is_pending(), Ordering::Release);
            ready
        })
        .await
    }
    #[cfg(not(test))]
    {
        let _ = probe;
        io.receive(bytes).await
    }
}
