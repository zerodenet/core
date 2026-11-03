//! Logical association continuity never authorizes overlapping old/new originals.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Current {
    channel: Arc<OwnedUdpChannel>,
    stop: CancellationToken,
}
pub(super) struct Shared {
    current: Mutex<Option<Current>>,
    retiring: Mutex<Option<Arc<OwnedUdpChannel>>>,
    last_retirement: Mutex<Option<CloseReport>>,
    generation: std::sync::Weak<Generation>,
    profile: usize,
    binding: UpstreamBinding,
    dns_generation: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    pub(super) views: AtomicUsize,
    pub(super) changed: tokio::sync::Notify,
    pub(super) stop: CancellationToken,
    reads: AtomicUsize,
}
struct ReadView {
    shared: Arc<Shared>,
    channel: Option<Arc<OwnedUdpChannel>>,
}
impl Drop for ReadView {
    fn drop(&mut self) {
        drop(self.channel.take());
        self.shared.reads.fetch_sub(1, Ordering::AcqRel);
        self.shared.changed.notify_waiters();
    }
}
impl Shared {
    pub(super) fn new(
        channel: OwnedUdpChannel,
        generation: &Arc<Generation>,
        profile: usize,
        binding: UpstreamBinding,
        stop: CancellationToken,
    ) -> Arc<Self> {
        let dns_generation = channel.dns_generation();
        Arc::new(Self {
            current: Mutex::new(Some(Current {
                channel: Arc::new(channel),
                stop: stop.child_token(),
            })),
            retiring: Mutex::new(None),
            last_retirement: Mutex::new(None),
            generation: Arc::downgrade(generation),
            profile,
            binding,
            dns_generation,
            views: AtomicUsize::new(2),
            changed: Default::default(),
            stop,
            reads: AtomicUsize::new(0),
        })
    }
    pub(super) fn returned(&self) {
        self.views.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify_waiters();
    }
    fn retire(&self, channel: &Arc<OwnedUdpChannel>) {
        let stop = {
            let mut current = self.current.lock().unwrap();
            if current
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(&actual.channel, channel))
            {
                let old = current.take().unwrap();
                let mut retiring = self.retiring.lock().unwrap();
                assert!(
                    retiring.is_none(),
                    "a new channel cannot replace unsettled original logical custody"
                );
                *retiring = Some(old.channel);
                Some(old.stop)
            } else {
                None
            }
        };
        if let Some(stop) = stop {
            stop.cancel();
            self.changed.notify_waiters();
        }
    }
    fn settle_retiring(&self) -> Result<(), ChannelError> {
        let retired = self.retiring.lock().unwrap().take();
        if let Some(channel) = retired {
            match Arc::try_unwrap(channel) {
                Ok(channel) => {
                    let report = channel.retire();
                    if !report.is_complete() {
                        *self.last_retirement.lock().unwrap() = Some(report);
                        return Err(error(Stage::Close, Reason::Timeout));
                    }
                    *self.last_retirement.lock().unwrap() = Some(report);
                }
                Err(channel) => {
                    *self.retiring.lock().unwrap() = Some(channel);
                    return Err(error(Stage::Close, Reason::Timeout));
                }
            }
        }
        Ok(())
    }
    pub(super) fn release(&self) -> Result<CloseReport, ChannelError> {
        let current = self
            .current
            .lock()
            .unwrap()
            .as_ref()
            .map(|current| current.channel.clone());
        if let Some(current) = current {
            self.retire(&current);
            drop(current);
        }
        self.settle_retiring()?;
        self.last_retirement
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Stage::Close, Reason::Internal))
    }
    pub(super) async fn send(
        self: &Arc<Self>,
        target: zero_core::request::Destination,
        payload: Arc<[u8]>,
    ) -> Result<usize, SendFailure> {
        let fail =
            |reason| SendFailure::new(error(Stage::Send, reason), DeliveryState::NotSubmitted);
        let mut retries = 0;
        loop {
            self.check_dns()
                .map_err(|error| SendFailure::new(error, DeliveryState::NotSubmitted))?;
            if self.stop.is_cancelled() {
                return Err(fail(Reason::Cancelled));
            }
            let channel = self
                .current
                .lock()
                .unwrap()
                .as_ref()
                .map(|current| current.channel.clone());
            if let Some(channel) = channel {
                if !channel.is_closed() {
                    match channel.send(&target.address, target.port, &payload) {
                        Ok(_) => return Ok(payload.len()),
                        Err(failure)
                            if failure.delivery == DeliveryState::NotSubmitted
                                && channel.is_closed() => {}
                        Err(failure) => return Err(failure),
                    }
                }
                self.retire(&channel);
                drop(channel);
            }
            if retries >= 1 {
                return Err(fail(Reason::Revoked));
            }
            retries += 1;
            let end = Instant::now() + Duration::from_secs(30);
            // A receiver must actually return its original channel guard.
            while self.reads.load(Ordering::Acquire) != 0 {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.reads.load(Ordering::Acquire) == 0 {
                    break;
                }
                tokio::select! {biased;_=self.stop.cancelled()=>return Err(fail(Reason::Cancelled)),result=tokio::time::timeout_at(end,changed)=>{result.map_err(|source|SendFailure::new(error(Stage::Close,Reason::Timeout).with_source(source),DeliveryState::NotSubmitted))?;}}
            }
            self.settle_retiring()
                .map_err(|error| SendFailure::new(error, DeliveryState::NotSubmitted))?;
            let generation = self
                .generation
                .upgrade()
                .ok_or_else(|| fail(Reason::Revoked))?;
            let auth = tokio::select! {biased;_=self.stop.cancelled()=>return Err(fail(Reason::Cancelled)),result=generation.acquire(self.profile,self.binding.clone(),Some(self.dns_generation),end)=>result.map_err(|error|SendFailure::new(error,DeliveryState::NotSubmitted))?};
            let channel = auth
                .open_udp()
                .await
                .map_err(|error| SendFailure::new(error, DeliveryState::NotSubmitted))?;
            *self.current.lock().unwrap() = Some(Current {
                channel: Arc::new(channel),
                stop: self.stop.child_token(),
            });
            self.changed.notify_waiters();
        }
    }
    pub(super) async fn receive(
        self: &Arc<Self>,
    ) -> Result<(zero_core::Address, u16, Vec<u8>), ChannelError> {
        loop {
            self.check_dns()?;
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let current = {
                let current = self.current.lock().unwrap();
                current.as_ref().map(|current| {
                    self.reads.fetch_add(1, Ordering::AcqRel);
                    (current.channel.clone(), current.stop.clone())
                })
            };
            if let Some((channel, stop)) = current {
                let view = ReadView {
                    shared: self.clone(),
                    channel: Some(channel),
                };
                let result = tokio::select! {biased;
                    _=self.stop.cancelled()=>return Err(error(Stage::Receive,Reason::Cancelled)),
                    _=stop.cancelled()=>None,
                    result=view.channel.as_ref().unwrap().receive()=>Some(result),
                };
                let result = match result {
                    Some(Ok(packet)) => Ok(Some(packet)),
                    Some(Err(source)) => {
                        self.retire(view.channel.as_ref().unwrap());
                        Err(source)
                    }
                    None => {
                        self.retire(view.channel.as_ref().unwrap());
                        Ok(None)
                    }
                };
                drop(view);
                self.settle_retiring()?;
                self.check_dns()?;
                if let Some(packet) = result? {
                    return Ok(packet);
                }
            } else {
                tokio::select! {biased;_=self.stop.cancelled()=>return Err(error(Stage::Receive,Reason::Cancelled)),_=changed=>{}}
            }
        }
    }
    fn check_dns(&self) -> Result<(), ChannelError> {
        let generation = self
            .generation
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        if generation.upstreams[self.profile].carrier_dns_generation() != self.dns_generation {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        Ok(())
    }
}
