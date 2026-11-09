use super::*;
mod buffer;
pub use buffer::NativeReadBuffer;
use buffer::ReadPacket;
pub(super) mod header;
use zero_traits::packet_metadata::PacketMetadataHeaderPromise;
/// One prepaid workspace in the original native driver's custody. This keeps
/// no buffer queue: the current exclusive allocation returns its credit only
/// when its memory is released. Other originals cannot steal the next read.
#[derive(Clone)]
pub struct NativeReadGrant(pub(super) PacketMetadataArc<ReadSlot>);
pub(super) struct ReadSlot {
    pub(super) root: Arc<PacketReturnTotals>,
    bytes: usize,
    pub(super) state: Mutex<SlotState>,
    pub(super) changed: Notify,
    pub(super) blocking: Condvar,
    pub(super) notifications: notifications::Notifications,
}
pub(super) struct SlotState {
    pub(super) ready: Option<credit::Credit>,
    pub(super) revoked: bool,
    envelope: Option<super::transmit::PacketTransmitEnvelope>,
    pub(super) header: Option<PacketMetadataHeaderPromise>,
}
impl ReadSlot {
    pub(super) fn return_credit(&self, credit: credit::Credit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            drop(state);
            drop(credit);
            return;
        }
        assert!(
            state.ready.is_none(),
            "same unique read workspace returned once"
        );
        state.ready = Some(credit);
        let unused = state
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.anchor_returned_credit());
        drop(state);
        drop(unused);
        self.notify();
    }
    pub(super) fn notify(&self) {
        if self.notifications.request() {
            self.notify_now();
        }
    }
    pub(super) fn notify_now(&self) {
        self.changed.notify_waiters();
        self.blocking.notify_all();
    }
}
impl NativeReadGrant {
    pub(super) fn from_prepaid_credit(
        credit: credit::Credit,
        envelope: Option<super::transmit::PacketTransmitEnvelope>,
        mut metadata: PacketMetadataCredit,
    ) -> Self {
        assert_eq!(credit.frames, 1, "one original IO workspace");
        let slot_metadata = metadata
            .split(
                PacketMetadataArc::<ReadSlot>::layout()
                    .unwrap_or_else(|_| unreachable!("fixed actual read-slot layout")),
            )
            .unwrap_or_else(|_| unreachable!("same read-slot metadata was prepaid"));
        let slot = PacketMetadataArc::from_prepaid(
            ReadSlot {
                root: credit.root.clone(),
                bytes: credit.bytes,
                state: Mutex::new(SlotState {
                    ready: Some(credit),
                    revoked: false,
                    envelope,
                    header: None,
                }),
                changed: Notify::new(),
                blocking: Condvar::new(),
                notifications: notifications::Notifications::default(),
            },
            slot_metadata,
        )
        .unwrap_or_else(|_| unreachable!("same actual read-slot Arc metadata"));
        let header = header::promise(&slot, metadata)
            .unwrap_or_else(|_| unreachable!("same actual read-header metadata was prepaid"));
        slot.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .header = Some(header);
        Self(slot)
    }
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }
    /// New fragment assembly admission charges the same kernel root. The
    /// prepaid original read slot remains separate, so existing assemblies
    /// cannot prevent the next read needed to complete them.
    pub fn try_assembly(&self, bytes: usize) -> Result<NativeReadBuffer, PacketMemoryError> {
        if self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .revoked
        {
            return Err(PacketMemoryError::Stopped);
        }
        self.0.root.native_assembly(bytes)
    }
    pub fn try_buffer(&self) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.try_workspace().map(NativeReadBuffer::from_credit)
    }
    pub fn try_workspace(&self) -> Result<NativePacketCredit, PacketMemoryError> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return Err(PacketMemoryError::Stopped);
        }
        if state.ready.is_none() {
            return Err(PacketMemoryError::Busy);
        }
        let header = state
            .header
            .as_ref()
            .ok_or(PacketMemoryError::Stopped)?
            .try_lease(1)?;
        let credit = state.ready.take().expect("same admitted read");
        let mut packet =
            NativePacketCredit::new(credit, Some(PacketMetadataArc::downgrade(&self.0)));
        packet.install_header_lease(header);
        packet.reserved_envelope = state.envelope.clone();
        if let Some(envelope) = &packet.reserved_envelope {
            envelope.anchor_borrowed();
        }
        Ok(packet)
    }
    pub async fn buffer(&self) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.workspace().await.map(NativeReadBuffer::from_credit)
    }
    pub async fn workspace(&self) -> Result<NativePacketCredit, PacketMemoryError> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_workspace() {
                Err(PacketMemoryError::Busy) => changed.await,
                other => return other,
            }
        }
    }
    /// Observe return of this exact exclusive workspace without taking or
    /// allocating it. This is only a memory-custody census, never a native
    /// close, task join, registration ACK or returned reservation receipt.
    pub async fn wait_workspace_returned(&self) -> Result<(), PacketMemoryError> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self
                    .0
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.ready.is_some()
                    && state
                        .header
                        .as_ref()
                        .is_some_and(|header| header.is_available())
                {
                    return Ok(());
                }
                if state.revoked {
                    return Err(PacketMemoryError::Stopped);
                }
            }
            changed.await;
        }
    }
    /// Revoke allocation only. An outstanding payload keeps its same debit
    /// until its real memory is released; this is no native or join receipt.
    pub fn revoke(&self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.revoked = true;
        let unused = state.ready.take();
        let envelope = state.envelope.take();
        let header = state.header.take();
        drop(state);
        drop(unused);
        drop(envelope);
        drop(header);
        self.wake();
        self.0.root.wake();
    }
    /// The actual blocking SDK reader waits on its own admitted workspace.
    /// Stop wakes this original Condvar; no polling timeout/reaper is needed.
    pub fn buffer_blocking(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.buffer_blocking_inner(stopped, None)
    }
    /// A real native reader may interrupt only allocation waiting for its own
    /// reversible pause. This observation is not that reader's pause ACK.
    pub fn buffer_blocking_interruptible(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
        paused: &std::sync::atomic::AtomicBool,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        self.buffer_blocking_inner(stopped, Some(paused))
    }
    fn buffer_blocking_inner(
        &self,
        stopped: &std::sync::atomic::AtomicBool,
        paused: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<NativeReadBuffer, PacketMemoryError> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if state.revoked || stopped.load(std::sync::atomic::Ordering::Acquire) {
                return Err(PacketMemoryError::Stopped);
            }
            if paused.is_some_and(|paused| paused.load(std::sync::atomic::Ordering::Acquire)) {
                return Err(PacketMemoryError::Paused);
            }
            if state.ready.is_some()
                && state
                    .header
                    .as_ref()
                    .is_some_and(|header| header.is_available())
            {
                let header = state
                    .header
                    .as_ref()
                    .expect("same live read header")
                    .try_lease(1)
                    .unwrap_or_else(|_| unreachable!("same read-slot mutex serializes checkout"));
                let credit = state.ready.take().expect("same original ready credit");
                let envelope = state.envelope.clone();
                if let Some(envelope) = &envelope {
                    envelope.anchor_borrowed();
                }
                drop(state);
                let mut packet =
                    NativePacketCredit::new(credit, Some(PacketMetadataArc::downgrade(&self.0)));
                packet.install_header_lease(header);
                packet.reserved_envelope = envelope;
                return Ok(NativeReadBuffer::from_credit(packet));
            }
            state = self
                .0
                .blocking
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
    /// The same IO workspace has returned; this is no native or task receipt.
    pub fn workspace_returned(&self) -> bool {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.ready.is_some()
            && state
                .header
                .as_ref()
                .is_some_and(|header| header.is_available())
    }
    pub fn wake(&self) {
        // External stop/pause predicates can change without holding this mutex.
        // Serialize with Condvar::wait to avoid missing their final wakeup.
        drop(
            self.0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        self.0.notify();
    }
}
