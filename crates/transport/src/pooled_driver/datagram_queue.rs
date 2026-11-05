//! One physical driver's bounded packet queue, including reserved FIFO endings.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;

const DATA_FRAMES: usize = 8;
const LOGICAL_CONTROLS: usize = 32;
const FRAME_BYTES: usize = 65535 + 1024;
#[derive(Clone)]
pub struct PacketQueue {
    sender: mpsc::Sender<QueuedPacket>,
    data: Arc<Semaphore>,
    controls: Arc<Semaphore>,
    max_frame_bytes: usize,
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
    channel_with_limits(DATA_FRAMES, LOGICAL_CONTROLS, FRAME_BYTES)
        .expect("fixed physical queue limits are valid")
}
pub fn channel_with_limits(
    data_frames: usize,
    logical_controls: usize,
    max_frame_bytes: usize,
) -> io::Result<(PacketQueue, mpsc::Receiver<QueuedPacket>)> {
    let capacity = data_frames
        .checked_add(logical_controls)
        .filter(|capacity| *capacity <= Semaphore::MAX_PERMITS)
        .filter(|_| data_frames != 0 && logical_controls != 0 && max_frame_bytes != 0)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
    let (sender, receiver) = mpsc::channel(capacity);
    Ok((
        PacketQueue {
            sender,
            data: Arc::new(Semaphore::new(data_frames)),
            controls: Arc::new(Semaphore::new(logical_controls)),
            max_frame_bytes,
        },
        receiver,
    ))
}
/// The original logical controller reserves its real queue position before use.
pub struct PacketWriter {
    sender: mpsc::Sender<QueuedPacket>,
    data: Arc<Semaphore>,
    control: Option<OwnedSemaphorePermit>,
    ending: Option<mpsc::OwnedPermit<QueuedPacket>>,
    end_frame: Vec<u8>,
    submitted: bool,
    finished: bool,
    max_frame_bytes: usize,
    ready: Option<OwnedSemaphorePermit>,
    // The same waits survive repeated polls. Mutex supplies Sync without
    // claiming that a future can be polled through a shared reference.
    waiting: Mutex<PollSemaphore>,
    closed: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,
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
        Ok(self.writer_reserved(end_frame, control, ending))
    }
    /// Wait on this original control/FIFO capacity, without creating a second
    /// queue. Cancellation returns only the real unsubmitted reservation.
    pub async fn writer_when_ready(&self, end_frame: Vec<u8>) -> io::Result<PacketWriter> {
        if end_frame.len() > 1024 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let control = tokio::select! { biased;
            _ = self.sender.closed() => return Err(io::ErrorKind::BrokenPipe.into()),
            permit = self.controls.clone().acquire_owned() => permit.map_err(|_| io::ErrorKind::BrokenPipe)?,
        };
        let ending = self
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| io::ErrorKind::BrokenPipe)?;
        Ok(self.writer_reserved(end_frame, control, ending))
    }
    fn writer_reserved(
        &self,
        end_frame: Vec<u8>,
        control: OwnedSemaphorePermit,
        ending: mpsc::OwnedPermit<QueuedPacket>,
    ) -> PacketWriter {
        PacketWriter {
            sender: self.sender.clone(),
            data: self.data.clone(),
            control: Some(control),
            ending: Some(ending),
            end_frame,
            submitted: false,
            finished: false,
            max_frame_bytes: self.max_frame_bytes,
            ready: None,
            waiting: Mutex::new(PollSemaphore::new(self.data.clone())),
            closed: Mutex::new(Box::pin({
                let sender = self.sender.clone();
                async move { sender.closed().await }
            })),
        }
    }
}
impl PacketWriter {
    /// Reserve one actual payload slot without allocating a frame. Receiver
    /// shutdown wakes this same waiter even if another writer holds capacity.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.finished
            || self.sender.is_closed()
            || self.closed.get_mut().unwrap().as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if self.ready.is_some() {
            return Poll::Ready(Ok(()));
        }
        match self.waiting.get_mut().unwrap().poll_acquire(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Ready(Some(permit)) => {
                self.ready = Some(permit);
                Poll::Ready(Ok(()))
            }
        }
    }
    pub fn poll_send(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<()>> {
        if bytes.len() > self.max_frame_bytes {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        }
        match self.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(self.send(bytes.to_vec())),
        }
    }
    pub fn send(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        if self.finished || self.sender.is_closed() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if bytes.len() > self.max_frame_bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let permit = match self.ready.take() {
            Some(permit) => permit,
            None => self
                .data
                .clone()
                .try_acquire_owned()
                .map_err(|_| capacity())?,
        };
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
    pub fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ready.take();
        *self.waiting.get_mut().unwrap() = PollSemaphore::new(self.data.clone());
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
        self.ending.take();
        self.control.take();
    }
}
impl Drop for PacketWriter {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
#[path = "../../tests/pooled_driver/datagram_queue.rs"]
mod tests;
