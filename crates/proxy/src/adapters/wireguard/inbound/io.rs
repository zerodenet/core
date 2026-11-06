//! The one actual Source's endpoint queue and peer watch survive graph changes.
//! A retained recipe aliases this same pipe; it neither binds nor spawns.
use super::*;
pub(in crate::adapters::wireguard) struct EndpointIo {
    pub(super) peers: watch::Sender<Arc<EndpointPeerState>>,
    pub(super) packets: Mutex<Option<mpsc::Receiver<EndpointPacket>>>,
    pub(super) active: Arc<AtomicBool>,
}
pub(in crate::adapters::wireguard) struct EndpointIoRecipe {
    pub(in crate::adapters::wireguard) sender: watch::Sender<mpsc::Sender<EndpointPacket>>,
    custody: IoCustody,
}
enum IoCustody {
    Fresh(mpsc::Receiver<EndpointPacket>),
    Retained(Arc<EndpointIo>),
}
impl EndpointIoRecipe {
    pub(in crate::adapters::wireguard) fn new() -> Self {
        let (sender, packets) = mpsc::channel(256);
        let (sender, _) = watch::channel(sender);
        Self::fresh(sender, packets)
    }
    pub(super) fn fresh(
        sender: watch::Sender<mpsc::Sender<EndpointPacket>>,
        packets: mpsc::Receiver<EndpointPacket>,
    ) -> Self {
        Self {
            sender,
            custody: IoCustody::Fresh(packets),
        }
    }
    pub(in crate::adapters::wireguard) fn retained(link: &LinkedEndpoint) -> Self {
        Self {
            sender: link.sender.clone(),
            custody: IoCustody::Retained(link.io.clone()),
        }
    }
}
impl LinkedEndpoint {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::adapters::wireguard) fn with_endpoint_io(
        identity: [u8; 32],
        generation: u64,
        listen: zero_config::ListenConfig,
        live: Arc<LiveInboundDevice>,
        devices: Vec<Arc<SharedRawIpDevice>>,
        endpoints: Vec<Option<SocketAddr>>,
        carriers: Vec<Option<Arc<dyn RawIpWireCarrier>>>,
        recipe: EndpointIoRecipe,
    ) -> Self {
        let peers = Arc::new(EndpointPeerState {
            revision: 0,
            devices: devices.clone(),
            initial_endpoints: endpoints,
            carriers,
        });
        let (io, pending) = match recipe.custody {
            IoCustody::Fresh(packets) => {
                let (peers, _) = watch::channel(peers);
                (
                    Arc::new(EndpointIo {
                        peers,
                        packets: Mutex::new(Some(packets)),
                        active: Arc::new(AtomicBool::new(false)),
                    }),
                    None,
                )
            }
            IoCustody::Retained(io) => (io, Some(peers)),
        };
        Self {
            listen,
            live,
            io_incarnation: AtomicU64::new(crate::runtime::raw_ip::next_incarnation()),
            state: Mutex::new(LinkedEndpointState {
                identity,
                generation,
                devices,
            }),
            sender: recipe.sender,
            io,
            pending_peers: Mutex::new(pending),
            owned_resources: OnceLock::new(),
        }
    }
    /// A synchronous part of the existing actual candidate publication guard.
    /// The original Source keeps its sole receiver and real driver; only its
    /// peer graph changes after the old graph's confirmed retirement.
    pub(in crate::adapters::wireguard) fn publish_endpoint_io(&self) {
        let pending = self.pending_peers.lock().unwrap().take();
        if let Some(pending) = pending {
            let revision = self.io.peers.borrow().revision.wrapping_add(1);
            self.io.peers.send_replace(Arc::new(EndpointPeerState {
                revision,
                devices: pending.devices.clone(),
                initial_endpoints: pending.initial_endpoints.clone(),
                carriers: pending.carriers.clone(),
            }));
        }
    }
    pub(super) fn declared_peers(&self) -> Arc<EndpointPeerState> {
        self.pending_peers
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| self.io.peers.borrow().clone())
    }
}

impl LinkedEndpoint {
    pub(in crate::adapters::wireguard) fn owned_resources(
        &self,
    ) -> Option<Arc<super::super::lifecycle::PeerResources>> {
        self.owned_resources.get().cloned()
    }
    pub(in crate::adapters::wireguard) fn install_owned_resources(
        &self,
        resources: Arc<super::super::lifecycle::PeerResources>,
    ) -> Result<(), EngineError> {
        if let Some(current) = self.owned_resources.get() {
            if Arc::ptr_eq(current, &resources) {
                return Ok(());
            }
            return Err(invalid(
                "linked endpoint already belongs to another resource owner",
            ));
        }
        self.owned_resources
            .set(resources)
            .map_err(|_| invalid("linked resource owner changed during installation"))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(in crate::adapters::wireguard) fn outer_carriers(&self) -> Vec<Arc<dyn RawIpWireCarrier>> {
        self.declared_peers()
            .carriers
            .iter()
            .flatten()
            .cloned()
            .collect()
    }
    pub(in crate::adapters::wireguard) fn reused_carriers(
        &self,
    ) -> Vec<Option<Arc<dyn RawIpWireCarrier>>> {
        self.declared_peers().carriers.clone()
    }
    pub(in crate::adapters::wireguard) fn carrier_owners_match(
        &self,
        owners: &[Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>],
    ) -> bool {
        let peers = self.declared_peers();
        peers.carriers.len() <= owners.len()
            && peers.carriers.iter().zip(owners).all(|(carrier, owner)| {
                carrier.as_ref().is_none_or(|carrier| {
                    carrier
                        .native_owner()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, owner))
                })
            })
    }
    #[allow(clippy::too_many_arguments)]
    pub(in crate::adapters::wireguard) fn new(
        identity: [u8; 32],
        generation: u64,
        listen: zero_config::ListenConfig,
        live: Arc<LiveInboundDevice>,
        devices: Vec<Arc<SharedRawIpDevice>>,
        endpoints: Vec<Option<SocketAddr>>,
        carriers: Vec<Option<Arc<dyn RawIpWireCarrier>>>,
        sender: watch::Sender<mpsc::Sender<EndpointPacket>>,
        packets: mpsc::Receiver<EndpointPacket>,
    ) -> Self {
        Self::with_endpoint_io(
            identity,
            generation,
            listen,
            live,
            devices,
            endpoints,
            carriers,
            EndpointIoRecipe::fresh(sender, packets),
        )
    }

    pub(in crate::adapters::wireguard) fn take_endpoint(
        &self,
    ) -> Result<RawIpInboundEndpoint, EngineError> {
        if self.io.active.swap(true, Ordering::AcqRel) {
            return Err(invalid(
                "linked WireGuard endpoint listener already running",
            ));
        }
        let packets = self
            .io
            .packets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .unwrap_or_else(|| {
                let (sender, packets) = mpsc::channel(256);
                self.sender.send_replace(sender);
                packets
            });
        Ok(RawIpInboundEndpoint {
            packets,
            peers: self.io.peers.subscribe(),
            lease: self.io.active.clone(),
        })
    }
}
