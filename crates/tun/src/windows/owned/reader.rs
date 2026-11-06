//! Same SDK reader body, transferred before acquisition to its actual host Scope.
//! Completion here reports the body only; the host must consume the matching
//! real blocking TaskJoinWitness before native Session/Adapter cleanup.
mod control;
use super::*;
use crate::packet_memory::{NativeReadGrant, PacketMemoryError};
pub use control::{OwnedWintunReadControl, OwnedWintunReadPause};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
pub(super) struct PreparedReader {
    pub(super) grant: NativeReadGrant,
    pub(super) receiver: Option<mpsc::Receiver<zero_traits::PacketBuffer>>,
    pub(super) done: bool,
    #[cfg(test)]
    pub(super) waiting_session: bool,
    pub(super) control: OwnedWintunReadControl,
    pub(super) event_close: Option<Result<(), Arc<io::Error>>>,
}
pub struct OwnedWintunReadWork {
    owner: Arc<OwnedWintunOwner>,
    grant: NativeReadGrant,
    sender: mpsc::Sender<zero_traits::PacketBuffer>,
    control: OwnedWintunReadControl,
}
impl OwnedWintunOwner {
    /// Persist this work before the caller registers its real blocking task.
    /// The task can genuinely wait for this same owner's session; native calls
    /// start only after the actual host has admitted that original task.
    pub fn prepare_read_work(
        self: &Arc<Self>,
        grant: NativeReadGrant,
    ) -> io::Result<OwnedWintunReadWork> {
        let mut state = self.state();
        if state.attempted
            || state.prepared_reader.is_some()
            || state.reader.is_some()
            || self.closing.load(Ordering::Acquire)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "same Wintun reader already attempted",
            ));
        }
        // This is the original bounded read channel, not a new buffer pool.
        // The prepaid exclusive workspace bounds real outstanding payloads.
        let (sender, receiver) = mpsc::channel(256);
        let control = OwnedWintunReadControl::new(grant.clone(), Arc::downgrade(self));
        state.prepared_reader = Some(PreparedReader {
            grant: grant.clone(),
            receiver: Some(receiver),
            done: false,
            #[cfg(test)]
            waiting_session: false,
            control: control.clone(),
            event_close: None,
        });
        Ok(OwnedWintunReadWork {
            owner: self.clone(),
            grant,
            sender,
            control,
        })
    }
    pub fn read_control(&self) -> Option<OwnedWintunReadControl> {
        self.state()
            .prepared_reader
            .as_ref()
            .map(|reader| reader.control.clone())
    }
    pub fn read_work_body_finished(&self) -> bool {
        self.state()
            .prepared_reader
            .as_ref()
            .is_some_and(|reader| reader.done)
    }
}
impl OwnedWintunReadWork {
    pub fn run(self) -> io::Result<()> {
        // The actual registered blocking task returns the typed source to its
        // same Scope. It is a task fault, not a fabricated native-close fault.
        if !self.control.activate()? {
            return Ok(());
        }
        self.receive()
    }
    fn receive(&self) -> io::Result<()> {
        let session = loop {
            if !self.control.checkpoint() {
                return Ok(());
            }
            let mut state = self.owner.state();
            if self.owner.closing.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.control.paused().load(Ordering::Acquire) {
                drop(state);
                continue;
            }
            if let Some(session) = &state.session {
                break session.clone();
            }
            #[cfg(test)]
            {
                state
                    .prepared_reader
                    .as_mut()
                    .expect("same original reader")
                    .waiting_session = true;
            }
            state = self
                .owner
                .reader_ready
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            #[cfg(test)]
            {
                state
                    .prepared_reader
                    .as_mut()
                    .expect("same original reader")
                    .waiting_session = false;
            }
            drop(state);
        };
        loop {
            if !self.control.checkpoint() {
                return Ok(());
            }
            let mut buffer = match self
                .grant
                .buffer_blocking_interruptible(&self.owner.closing, self.control.paused())
            {
                Ok(buffer) => buffer,
                Err(PacketMemoryError::Stopped) => return Ok(()),
                Err(PacketMemoryError::Paused) => continue,
                Err(source) => return Err(io::Error::other(source)),
            };
            let packet = loop {
                if self.owner.closing.load(Ordering::Acquire) {
                    return Ok(());
                }
                if self.control.paused().load(Ordering::Acquire) {
                    break None;
                }
                match session.try_receive() {
                    Ok(Some(packet)) => break Some(packet),
                    Ok(None) => self.control.wait(&session)?,
                    Err(_) if self.owner.closing.load(Ordering::Acquire) => return Ok(()),
                    Err(source) => return Err(io::Error::from(source)),
                }
            };
            let Some(packet) = packet else {
                drop(buffer);
                continue;
            };
            let length = packet.bytes().len();
            if length > buffer.as_mut_bytes().len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "actual Wintun packet exceeds prepaid read workspace",
                ));
            }
            buffer.as_mut_bytes()[..length].copy_from_slice(packet.bytes());
            drop(packet); // Release this real SDK packet before transferring bytes.
            let original = match buffer.into_packet(length) {
                Ok(original) => original,
                Err(_) => unreachable!("checked same actual read length"),
            };
            if self.sender.blocking_send(original).is_err() {
                return Ok(());
            }
        }
    }
}

impl Drop for OwnedWintunReadWork {
    fn drop(&mut self) {
        // The real body has returned or unwound, or registration rejected this
        // never-run body. This records no TaskJoined or physical-close fact.
        self.control.finished();
        let mut state = self.owner.state();
        state
            .prepared_reader
            .as_mut()
            .expect("same prepared reader")
            .done = true;
        drop(state);
        self.owner.reader_ready.notify_all();
    }
}
