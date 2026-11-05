//! The exact device original, its two I/O tasks and explicit close outcome.
use crate::{
    OwnedTunDevice, OwnedTunIdentity, TunCloseReceipt, TunPacketReceiver, TunPacketSender,
};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    sync::{watch, Mutex},
    task::JoinHandle,
};
use zero_traits::IoObserver;

type ReadTask<D> = JoinHandle<(ReadHalf<D>, io::Result<()>)>;
type WriteTask<D> = JoinHandle<(WriteHalf<D>, io::Result<()>)>;
struct State<D> {
    device: Option<D>,
    reader: Option<ReadTask<D>>,
    writer: Option<WriteTask<D>>,
    read_half: Option<ReadHalf<D>>,
    write_half: Option<WriteHalf<D>>,
    errors: Vec<Arc<io::Error>>,
    closed: Option<Result<Arc<TunCloseReceipt>, Arc<io::Error>>>,
    started: bool,
}
/// Persistent custody for a device, including a failed configure or cancelled close.
/// Neither task completion nor dropping channel endpoints is a close receipt.
pub struct OwnedTunChannels<D> {
    identity: OwnedTunIdentity,
    name: String,
    stop: watch::Sender<bool>,
    state: Mutex<State<D>>,
}
impl<D> std::fmt::Debug for OwnedTunChannels<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedTunChannels")
            .field("identity", &self.identity)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
impl<D: OwnedTunDevice + 'static> OwnedTunChannels<D> {
    /// Install the original before configuration or spawning any I/O task.
    pub fn new(device: D) -> Arc<Self> {
        let identity = device.owned_identity();
        let name = device.name().to_owned();
        let (stop, _) = watch::channel(false);
        Arc::new(Self {
            identity,
            name,
            stop,
            state: Mutex::new(State {
                device: Some(device),
                reader: None,
                writer: None,
                read_half: None,
                write_half: None,
                errors: Vec::new(),
                closed: None,
                started: false,
            }),
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn identity(&self) -> &OwnedTunIdentity {
        &self.identity
    }
    pub fn configure_addresses(
        &self,
        addresses: &[(std::net::IpAddr, std::net::IpAddr)],
        mtu: u16,
    ) -> io::Result<()> {
        let state = self
            .state
            .try_lock()
            .map_err(|e| io::Error::new(io::ErrorKind::WouldBlock, e))?;
        state
            .device
            .as_ref()
            .ok_or_else(|| io::Error::other("TUN original is no longer cold"))?
            .configure_addresses(addresses, mtu)
    }
    pub fn start(
        &self,
        observer: Arc<dyn IoObserver>,
    ) -> io::Result<(TunPacketSender, TunPacketReceiver)> {
        let mut state = self
            .state
            .try_lock()
            .map_err(|e| io::Error::new(io::ErrorKind::WouldBlock, e))?;
        if state.started || state.closed.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "owned TUN channels already started or closed",
            ));
        }
        let device = state
            .device
            .take()
            .ok_or_else(|| io::Error::other("TUN original is absent"))?;
        let (mut reader, mut writer) = tokio::io::split(device);
        let (read_tx, read_rx) = tokio::sync::mpsc::channel(256);
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
        let mut read_stop = self.stop.subscribe();
        let mut write_stop = self.stop.subscribe();
        let reads = observer.clone();
        state.reader = Some(tokio::spawn(async move {
            let result = async {
                let mut buf = vec![0; 65536];
                loop {
                    let n = tokio::select! { biased; _ = read_stop.changed() => break, n = reader.read(&mut buf) => n? };
                    if n == 0 { break; }
                    reads.received(n);
                    tokio::select! { biased; _ = read_stop.changed() => break, sent = read_tx.send(buf[..n].to_vec()) => if sent.is_err() { break; } }
                }
                Ok(())
            }.await;
            if result.is_err() {
                reads.error();
            }
            (reader, result)
        }));
        state.writer = Some(tokio::spawn(async move {
            let result = async {
                loop {
                    let packet = tokio::select! { biased; _ = write_stop.changed() => break, packet = write_rx.recv() => match packet { Some(packet) => packet, None => break } };
                    tokio::select! { biased; _ = write_stop.changed() => break, result = writer.write_all(&packet) => result? }
                    observer.sent(packet.len());
                }
                Ok(())
            }.await;
            if result.is_err() {
                observer.error();
            }
            write_rx.close();
            while write_rx.try_recv().is_ok() {
                observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
            }
            (writer, result)
        }));
        state.started = true;
        Ok((write_tx, read_rx))
    }
    /// Await the actual handles in place; cancellation keeps every unobserved original.
    pub async fn close(&self) -> io::Result<Arc<TunCloseReceipt>> {
        self.stop.send_replace(true);
        let mut state = self.state.lock().await;
        if let Some(task) = state.reader.as_mut() {
            match task.await {
                Ok((half, result)) => {
                    state.read_half = Some(half);
                    if let Err(error) = result {
                        state.errors.push(Arc::new(error));
                    }
                }
                Err(error) => state.errors.push(Arc::new(io::Error::other(error))),
            }
            state.reader = None;
        }
        if let Some(task) = state.writer.as_mut() {
            match task.await {
                Ok((half, result)) => {
                    state.write_half = Some(half);
                    if let Err(error) = result {
                        state.errors.push(Arc::new(error));
                    }
                }
                Err(error) => state.errors.push(Arc::new(io::Error::other(error))),
            }
            state.writer = None;
        }
        if state.device.is_none() && state.read_half.is_some() && state.write_half.is_some() {
            let reader = state.read_half.take().unwrap();
            let writer = state.write_half.take().unwrap();
            state.device = Some(reader.unsplit(writer));
        }
        if state.closed.is_none() {
            if let Some(device) = state.device.take() {
                state.closed = Some(
                    device
                        .close_owned()
                        .and_then(|receipt| {
                            if self.identity.same_owner(receipt.identity()) {
                                Ok(Arc::new(receipt))
                            } else {
                                Err(io::Error::other(
                                    "owned TUN close receipt differs from its original",
                                ))
                            }
                        })
                        .map_err(Arc::new),
                );
            } else {
                return Err(io::Error::other(
                    "TUN original cannot be recovered from failed I/O task joins",
                ));
            }
        }
        if let Some(error) = state.errors.first() {
            return Err(io::Error::other(SharedError(error.clone())));
        }
        state
            .closed
            .as_ref()
            .unwrap()
            .clone()
            .map_err(|error| io::Error::other(SharedError(error)))
    }
}
#[derive(Debug)]
struct SharedError(Arc<io::Error>);
impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
