//! One actual binding, its serial initialization and original close token.
use super::*;
pub struct NativeSlot {
    pub(super) binding: UpstreamBinding,
    gate: Arc<tokio::sync::Mutex<()>>,
    original: Mutex<Option<CarrierProtocolRetirement>>,
    cached: Mutex<
        Option<(
            PacketMetadataArc<super::super::owned::OwnedClient>,
            CarrierOrigin,
        )>,
    >,
    _memory: PacketMetadataCredit,
}
pub struct NativeLane {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _slot: Arc<NativeSlot>,
}
impl NativeSlot {
    pub async fn lock(self: &Arc<Self>) -> NativeLane {
        NativeLane {
            _guard: self.gate.clone().lock_owned().await,
            _slot: self.clone(),
        }
    }

    pub(super) fn new(
        binding: UpstreamBinding,
        memory: &PacketDriverMetadataView,
    ) -> Result<Arc<Self>, ChannelError> {
        let layout = zero_platform_tokio::contract::network::UpstreamTransport::opener_metadata_layout::<Self>(&binding)
            .and_then(|layout| layout.checked_add(PacketMetadataLayout::arc::<tokio::sync::Mutex<()>>()?))
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        let credit = memory
            .reserve(layout)
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        Ok(Arc::new(Self {
            binding,
            gate: Arc::new(tokio::sync::Mutex::new(())),
            original: Mutex::new(None),
            cached: Mutex::new(None),
            _memory: credit,
        }))
    }
    pub fn cached(
        &self,
    ) -> Option<(
        PacketMetadataArc<super::super::owned::OwnedClient>,
        CarrierOrigin,
    )> {
        let cached = self.cached.lock().unwrap().clone();
        cached.filter(|(client, _)| client.usable())
    }

    /// The caller retains the same gate and epoch lease through initialization.
    /// It installs this opaque original before the first resourceful await.
    pub fn install_original(
        &self,
        original: CarrierProtocolRetirement,
    ) -> Result<(), ChannelError> {
        let mut actual = self.original.lock().unwrap();
        if actual
            .as_ref()
            .is_some_and(|previous| !previous.is_settled())
        {
            return Err(error(Reason::Revoked));
        }
        let previous = actual.replace(original);
        drop(actual);
        drop(previous);
        Ok(())
    }
    pub fn publish(
        &self,
        client: PacketMetadataArc<super::super::owned::OwnedClient>,
        origin: CarrierOrigin,
    ) -> Result<(), ChannelError> {
        if !self
            .original
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|original| client.names_retirement(original))
            || !client.usable()
        {
            return Err(error(Reason::StalePlan));
        }
        let previous = self.cached.lock().unwrap().replace((client, origin));
        drop(previous);
        Ok(())
    }
    pub fn request_retirement(&self) {
        let cached = self.cached.lock().unwrap().take();
        drop(cached);
        let original = self.original.lock().unwrap().clone();
        if let Some(original) = original {
            original.request_retirement();
        }
    }
    pub async fn retire_original(&self) -> Result<(), ChannelError> {
        let cached = self.cached.lock().unwrap().take();
        drop(cached);
        let original = self.original.lock().unwrap().clone();
        if let Some(original) = original {
            original.request_retirement();
            original.wait_settled_original().await?;
            let old = self.original.lock().unwrap().take();
            drop(old);
        }
        Ok(())
    }
}
