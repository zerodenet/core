//! One physical driver's bounded packet queue, including reserved FIFO endings.
use std::{io, sync::Arc};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

const DATA_FRAMES: usize = 8;
const LOGICAL_CONTROLS: usize = 32;
const FRAME_BYTES: usize = 65535 + 1024;
#[derive(Clone)]
pub struct PacketQueue {
    sender: mpsc::Sender<QueuedPacket>,
    data: Arc<Semaphore>,
    controls: Arc<Semaphore>,
}
pub struct QueuedPacket {
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}
impl QueuedPacket {
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}
pub fn channel() -> (PacketQueue, mpsc::Receiver<QueuedPacket>) {
    let (sender, receiver) = mpsc::channel(DATA_FRAMES + LOGICAL_CONTROLS);
    (
        PacketQueue {
            sender,
            data: Arc::new(Semaphore::new(DATA_FRAMES)),
            controls: Arc::new(Semaphore::new(LOGICAL_CONTROLS)),
        },
        receiver,
    )
}
/// The original logical controller reserves its real queue position before use.
pub struct PacketWriter {
    sender: mpsc::Sender<QueuedPacket>,
    data: Arc<Semaphore>,
    control: Option<OwnedSemaphorePermit>,
    ending: Option<mpsc::OwnedPermit<QueuedPacket>>,
    end_frame: Vec<u8>,
    submitted: bool,
}
fn capacity() -> io::Error {
    io::ErrorKind::WouldBlock.into()
}
impl PacketQueue {
    pub fn writer(&self, end_frame: Vec<u8>) -> io::Result<PacketWriter> {
        if end_frame.len() > 1024 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let control = self
            .controls
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        let ending = self
            .sender
            .clone()
            .try_reserve_owned()
            .map_err(|_| capacity())?;
        Ok(PacketWriter {
            sender: self.sender.clone(),
            data: self.data.clone(),
            control: Some(control),
            ending: Some(ending),
            end_frame,
            submitted: false,
        })
    }
}
impl PacketWriter {
    pub fn send(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        if bytes.len() > FRAME_BYTES {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let permit = self
            .data
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        self.sender
            .try_send(QueuedPacket {
                bytes,
                _permit: permit,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => capacity(),
                mpsc::error::TrySendError::Closed(_) => io::ErrorKind::BrokenPipe.into(),
            })?;
        self.submitted = true;
        Ok(())
    }
}
impl Drop for PacketWriter {
    fn drop(&mut self) {
        if self.submitted {
            if let (Some(ending), Some(control)) = (self.ending.take(), self.control.take()) {
                // The same FIFO owns every accepted data frame and this END.
                // The original physical driver, not a detached cleanup task,
                // retains queued closure work until dequeue or physical stop.
                ending.send(QueuedPacket {
                    bytes: std::mem::take(&mut self.end_frame),
                    _permit: control,
                });
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/pooled_driver/datagram_queue.rs"]
mod tests;
