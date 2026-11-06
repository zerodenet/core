//! Same-source acquisition pause; acknowledgement is never a close receipt.
use super::*;
use std::{io, sync::atomic::AtomicBool};

#[derive(Default)]
pub(in crate::runtime) struct Acquisition {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
    idle: AtomicBool,
    #[cfg(test)]
    last_accepted_closed: Mutex<
        Option<(
            zero_platform_tokio::AcceptedTcpClaim,
            zero_platform_tokio::AcceptedTcpClosed,
        )>,
    >,
    peer_dirty: AtomicBool,
    peer_changed: tokio::sync::Notify,
}
#[derive(Default)]
struct State {
    serial: u64,
    requested: Option<u64>,
    acknowledged: Option<(u64, bool)>,
}
pub(in crate::runtime) struct AcquisitionPause {
    acquisition: Arc<Acquisition>,
    request: Option<u64>,
    #[cfg(feature = "raw-ip-runtime")]
    packet: Option<zero_platform_tokio::contract::packet::PacketSourcePaused>,
}
impl Drop for AcquisitionPause {
    fn drop(&mut self) {
        let mut state = self
            .acquisition
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.request.is_some() && state.requested == self.request {
            state.requested = None;
            state.acknowledged = None;
        }
        drop(state);
        self.acquisition.changed.notify_waiters();
    }
}
impl Acquisition {
    #[cfg(test)]
    pub(in crate::runtime) fn record_accepted_closed(
        &self,
        claim: zero_platform_tokio::AcceptedTcpClaim,
        closed: zero_platform_tokio::AcceptedTcpClosed,
    ) {
        assert!(closed.matches(&claim));
        let old = self
            .last_accepted_closed
            .lock()
            .unwrap()
            .replace((claim, closed));
        drop(old);
    }

    pub(in crate::runtime) fn peer_cleanup_progressed(&self) {
        self.peer_dirty.store(true, Ordering::Release);
        self.peer_changed.notify_waiters();
    }
    pub(in crate::runtime) async fn peer_cleanup_changed(&self) {
        loop {
            let changed = self.peer_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.peer_dirty.swap(false, Ordering::AcqRel) {
                return;
            }
            changed.await;
        }
    }

    fn request(self: &Arc<Self>) -> io::Result<AcquisitionPause> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.requested.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same source acquisition already paused",
            ));
        }
        state.serial = state
            .serial
            .checked_add(1)
            .ok_or_else(|| io::Error::other("source pause identity exhausted"))?;
        let request = state.serial;
        state.requested = Some(request);
        state.acknowledged = None;
        drop(state);
        self.changed.notify_waiters();
        Ok(AcquisitionPause {
            acquisition: self.clone(),
            request: Some(request),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
        })
    }
    pub(in crate::runtime) async fn requested(&self) -> u64 {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(request) = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .requested
            {
                return request;
            }
            changed.await;
        }
    }
    pub(in crate::runtime) fn observe_idle(&self, idle: bool) {
        self.idle.store(idle, Ordering::Release);
    }
    pub(in crate::runtime) fn acknowledge(&self, request: u64, idle: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.requested == Some(request) {
            state.acknowledged = Some((request, idle));
        }
        drop(state);
        self.changed.notify_waiters();
    }
    pub(in crate::runtime) async fn resumed(&self, request: u64) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .requested
                != Some(request)
            {
                return;
            }
            changed.await;
        }
    }
}
impl InboundOwner {
    #[cfg(test)]
    pub(crate) fn accepted_closed_for_test(
        &self,
    ) -> Option<(
        zero_platform_tokio::AcceptedTcpClaim,
        zero_platform_tokio::AcceptedTcpClosed,
    )> {
        self.acquisition
            .last_accepted_closed
            .lock()
            .unwrap()
            .clone()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) async fn pause_packet_main_if_idle(
        &self,
        deadline: Instant,
    ) -> Result<Option<zero_platform_tokio::contract::packet::PacketSourceMainPaused>, EngineError> {
        if self.terminal().is_some() { return Ok(None); }
        match self.packet_pause.get() {
            Some(packet) => packet.pause_main(deadline).await.map(Some).map_err(|source| std::io::Error::other(source).into()),
            None => Ok(None),
        }
    }
    pub(in crate::runtime) async fn pause_acquisition_if_idle(
        &self,
        deadline: Instant,
    ) -> Result<Option<AcquisitionPause>, EngineError> {
        if self.terminal().is_some() {
            return Ok(None);
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(packet) = self.packet_pause.get() {
            let paused = packet
                .pause(deadline)
                .await
                .map_err(std::io::Error::other)?;
            return Ok(Some(AcquisitionPause {
                acquisition: self.acquisition.clone(),
                request: None,
                packet: Some(paused),
            }));
        }
        if !self.is_running() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "original source is not quiescent",
            )
            .into());
        }
        let pause = self.acquisition.request()?;
        loop {
            let changed = self.acquisition.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let acknowledged = self
                .acquisition
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .acknowledged;
            if let Some((request, idle)) = acknowledged {
                if Some(request) == pause.request {
                    return if idle {
                        Ok(Some(pause))
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "original source still retains accepted users or peer originals",
                        )
                        .into())
                    };
                }
            }
            tokio::select! { biased;
                _=self.cancel.cancelled()=>return Err(io::Error::new(io::ErrorKind::WouldBlock,"original source stopped before its acquisition pause").into()),
                result=tokio::time::timeout_at(deadline,changed)=>if result.is_err() {return Err(io::Error::new(io::ErrorKind::TimedOut,"original source acquisition pause not acknowledged").into());},
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn acquisition_idle_for_test(&self) -> bool {
        self.acquisition.idle.load(Ordering::Acquire)
    }
}
impl InboundTaskAccess {
    pub(in crate::runtime) fn acquisition(&self) -> Arc<Acquisition> {
        self.acquisition.clone()
    }
    pub(in crate::runtime) fn peer_groups_quiescent(&self) -> bool {
        #[cfg(feature = "managed-datagram-runtime")]
        {
            let Some(groups) = self.peer_groups.upgrade() else {
                return false;
            };
            let groups = groups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            groups.iter().all(|group| group.acquisition_quiescent())
        }
        #[cfg(not(feature = "managed-datagram-runtime"))]
        {
            true
        }
    }
}

impl AcquisitionPause {
    pub(in crate::runtime) fn validate(&self) -> Result<(), EngineError> {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(packet) = &self.packet {
            packet.validate().map_err(std::io::Error::other)?;
        }
        Ok(())
    }
}
