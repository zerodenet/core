//! Original granted resources and protocol lifecycle over the same host provider.
use super::*;
pub(super) struct BoundResources {
    pub(super) carrier_buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    pub(super) packet_metadata: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    pub(super) source_entry: std::sync::Weak<super::super::InboundOwner>,
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    pub(super) source_protocols: crate::inventory::WeakProtocolInventory,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) source_pause: Option<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>>,
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    pub(super) packet_native:
        Option<std::sync::Weak<super::super::bind::native::udp::NativeDatagramGrant>>,
    pub(super) handoff: Option<Arc<super::super::bind::native::handoff::Grant>>,
    pub(super) process: Option<Arc<crate::component::instance::HostResources>>,
    pub(super) extra_tcp: Mutex<Vec<(Destination, ControlledTcpListener)>>,
    pub(super) files: Option<files::FileTrees>,
    pub(super) targets: Option<targets::Targets>,
    pub(super) metadata: Option<Arc<crate::component::instance::HostResources>>,
    pub(super) icmp_carrier: bool,
    pub(super) carrier: Option<Arc<std::sync::OnceLock<std::net::SocketAddr>>>,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) udp: Option<datagram::DatagramResource>,
    pub(super) primary_udp: Mutex<Option<GrantedDatagramListener>>,
    pub(super) address: Destination,
    pub(super) listener: Mutex<Option<ControlledTcpListener>>,
    pub(super) starting: AtomicBool,
    pub(super) cancel: CancellationToken,
    pub(super) datagram:
        Option<zero_platform_tokio::contract::component::AcceptedDatagramAuthority>,
}
impl ResourceAccess for BoundResources {
    fn carrier_buffer_budget(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierBufferBudget> {
        Some(self.carrier_buffers.clone())
    }

    fn packet_driver_metadata(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketDriverMetadataView, BuildError>
    {
        Ok(self.packet_metadata.clone())
    }

    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn receive_packet_driver_failure(
        &self,
        grant: &zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
        source: Arc<ChannelError>,
    ) -> Result<bool, BuildError> {
        let owner = self.source_entry.upgrade().ok_or_else(denied)?;
        let native = self
            .packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?;
        owner.receive_packet_driver_failure(&native, grant, source)
    }

    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn register_software_packet_source_driver(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<
        zero_platform_tokio::controlled_carrier::LogicalPacketProviderRegistration,
        BuildError,
    > {
        let entry = self.source_entry.upgrade().ok_or_else(denied)?;
        entry.validate_packet_software_activation()?;
        let protocols = self.source_protocols.upgrade().ok_or_else(denied)?;
        let tag = entry.source_tag.get().ok_or_else(denied)?;
        let input = entry.source_input.get().ok_or_else(denied)?;
        let dependencies = entry.source_dependencies.get().ok_or_else(denied)?;
        // Exact already-prepared pending/current Source input and actual
        // assigned dependencies. Never use Engine.current as a fallback.
        protocols
            .runtime_host()
            .components()
            .packet_source_driver_candidate(tag, input, dependencies)?;
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .register_software_driver(driver)
    }
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn packet_driver_caller(
        &self,
        original: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
        binding: zero_platform_tokio::contract::network::UpstreamBinding,
        call: zero_platform_tokio::contract::network::CarrierContext,
    ) -> Result<zero_platform_tokio::contract::component::GrantedPacketServiceCaller, BuildError>
    {
        let entry = self.source_entry.upgrade().ok_or_else(denied)?;
        let protocols = self.source_protocols.upgrade().ok_or_else(denied)?;
        let tag = entry.source_tag.get().ok_or_else(denied)?;
        let input = entry.source_input.get().ok_or_else(denied)?;
        let dependencies = entry.source_dependencies.get().ok_or_else(denied)?;
        let (_, plan) = protocols
            .runtime_host()
            .components()
            .packet_source_driver_candidate(tag, input, dependencies)?;
        let targets = plan.packet_driver_targets(dependencies, binding.network_generation)?;
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .packet_driver_caller(original, driver, binding, call, targets)
    }

    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn install_packet_source_memory(
        &self,
        datagram: usize,
        memory: zero_platform_tokio::contract::packet::PacketSourceMemory,
    ) -> Result<(), BuildError> {
        if datagram != 0 {
            return Err(denied());
        }
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .install_packet_memory(memory)
    }

    #[cfg(feature = "raw-ip-runtime")]
    fn packet_source_pause(
        &self,
    ) -> Option<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>> {
        self.source_pause.clone()
    }

    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn take_packet_source_wire_launch(
        &self,
        datagram: usize,
    ) -> Result<zero_platform_tokio::contract::scope::TaskLaunch, BuildError> {
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .take_packet_wire_launch(datagram)
    }
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn take_packet_datagram_reads(
        &self,
        datagram: usize,
    ) -> Result<[zero_tun::packet_memory::NativeReadGrant; 2], BuildError> {
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .take_packet_reads(datagram)
    }
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn take_packet_datagram_receive_workspace(
        &self,
        datagram: usize,
        reader: usize,
    ) -> Result<zero_tun::packet_memory::NativeReadGrant, BuildError> {
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .take_packet_receive_workspace(datagram, reader)
    }
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn take_packet_datagram_control(
        &self,
        datagram: usize,
    ) -> Result<Option<zero_tun::packet_memory::NativeReadGrant>, BuildError> {
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .take_packet_control(datagram)
    }
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    fn register_packet_source_driver(
        &self,
        original: &zero_platform_tokio::controlled_datagram::listener::DatagramListenerIdentity,
        witness: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<zero_platform_tokio::contract::component::GrantedPacketSourceDriver, BuildError>
    {
        let original = self
            .packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .register_driver(original, witness)?;
        let checkpoint = self.source_pause.as_ref().ok_or_else(denied)?.bind(
            zero_platform_tokio::contract::packet::PacketPauseParticipant::Wire,
            witness,
        )?;
        original.bind_packet_source(&checkpoint)?;
        Ok(
            zero_platform_tokio::contract::component::GrantedPacketSourceDriver {
                origin: original,
                checkpoint,
            },
        )
    }

    fn inbound_endpoints(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::InboundListenerEndpoints, BuildError>
    {
        let grant = self.handoff.as_ref().ok_or_else(denied)?;
        let endpoints = grant.endpoints.get().ok_or_else(denied)?;
        #[cfg(feature = "managed-datagram-runtime")]
        let datagram = self
            .udp
            .as_ref()
            .and_then(|udp| udp.actual.get().copied())
            .ok_or_else(denied)?;
        #[cfg(not(feature = "managed-datagram-runtime"))]
        return Err(denied());
        #[cfg(feature = "managed-datagram-runtime")]
        Ok(
            zero_platform_tokio::contract::component::InboundListenerEndpoints {
                external: endpoints.external,
                stream: endpoints.stream,
                datagram,
            },
        )
    }
    fn probe_inbound_listener(&self, deadline: Instant) -> BoxFuture<'_, Result<bool, BuildError>> {
        Box::pin(async move {
            if !self.starting.load(Ordering::Acquire) || self.cancel.is_cancelled() {
                return Err(denied());
            }
            self.handoff
                .as_ref()
                .ok_or_else(denied)?
                .probe(deadline)
                .await
        })
    }
    fn spawn_process(
        &self,
        request: zero_platform_tokio::contract::component::ProcessRequest,
    ) -> Result<tokio::process::Child, BuildError> {
        if !self.starting.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            return Err(denied());
        }
        self.process
            .as_ref()
            .ok_or_else(denied)?
            .spawn_process(request)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn process_endpoint_retirement(
        &self,
        ordinal: usize,
    ) -> Option<Arc<dyn zero_platform_tokio::owned_process::ProcessEndpointRetirement>> {
        self.process.as_ref()?.process_endpoint_retirement(ordinal)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn prepare_owned_process(
        &self,
        request: zero_platform_tokio::contract::component::ProcessRequest,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<Arc<zero_platform_tokio::owned_process::OwnedProcess>, BuildError> {
        if !self.starting.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            return Err(denied());
        }
        self.process
            .as_ref()
            .ok_or_else(denied)?
            .prepare_owned_process(request, tasks)
    }
    fn open_inbound_file_tree(
        &self,
        caller: zero_platform_tokio::AcceptedCaller,
        root: PathBuf,
    ) -> Result<zero_platform_tokio::file::tree::FileTreeSession, BuildError> {
        self.files.as_ref().ok_or_else(denied)?.open(caller, root)
    }
    fn bind_inbound_icmp(
        &self,
        source: std::net::IpAddr,
    ) -> Result<zero_platform_tokio::controlled_icmp::ControlledIcmpSocket, BuildError> {
        if !self.icmp_carrier {
            return Err(denied());
        }
        let socket = self
            .metadata
            .as_ref()
            .ok_or_else(denied)?
            .bind_inbound_icmp(source)?;
        if let Some(local) = &self.carrier {
            local
                .set(std::net::SocketAddr::new(
                    socket.local_addr().map_err(|e| denied().with_source(e))?,
                    0,
                ))
                .map_err(|_| denied())?;
        }
        Ok(socket)
    }

    fn listen_metadata_datagram(
        &self,
        request: MetadataDatagramRequest,
    ) -> BoxFuture<'_, Result<GrantedMetadataListener, BuildError>> {
        Box::pin(async move {
            {
                let grant = self
                    .metadata
                    .as_ref()
                    .ok_or_else(denied)?
                    .listen_metadata_datagram(request)
                    .await?;
                if let Some(local) = self.carrier.as_ref().filter(|_| !self.icmp_carrier) {
                    local
                        .set(grant.tx.local_addr().map_err(|e| denied().with_source(e))?)
                        .map_err(|_| denied())?;
                }
                Ok(grant)
            }
        })
    }
    fn confirm_metadata_users(
        &self,
        ordinal: usize,
        receipt:&zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<(), BuildError> {
        self.metadata
            .as_ref()
            .ok_or_else(denied)?
            .confirm_metadata_users(ordinal, receipt, tasks)
    }
    fn open_inbound_stream(
        &self,
        caller: zero_platform_tokio::AcceptedCaller,
        endpoint: zero_traits::FallbackEndpoint,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::TcpRelayStream, BuildError>> {
        Box::pin(async move {
            self.targets
                .as_ref()
                .ok_or_else(denied)?
                .open(caller, endpoint)
                .await
        })
    }
    fn open_accepted_datagram(
        &self,
        caller: &zero_platform_tokio::AcceptedCaller,
    ) -> Result<GrantedDatagramListener, BuildError> {
        self.datagram.as_ref().ok_or_else(denied)?.open(caller)
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<ControlledTcpListener, BuildError>> {
        Box::pin(async move {
            if !self.starting.load(Ordering::Acquire) || self.cancel.is_cancelled() {
                return Err(denied());
            }
            let mut extra = self.extra_tcp.lock().unwrap();
            if let Some(index) = extra.iter().position(|(declared, _)| *declared == address) {
                return Ok(extra.remove(index).1);
            }
            if address != self.address {
                return Err(denied());
            }
            drop(extra);
            self.listener
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .ok_or_else(denied)
        })
    }
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        Box::pin(async { Err(denied()) })
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, BuildError>> {
        Box::pin(async move {
            #[cfg(feature = "managed-datagram-runtime")]
            if self.starting.load(Ordering::Acquire) && !self.cancel.is_cancelled() {
                if address != self.address {
                    return Err(denied());
                }
                if let Some(view) = self.primary_udp.lock().unwrap().take() {
                    return Ok(view);
                }
                return self.udp.as_ref().ok_or_else(denied)?.open(address);
            }
            let _ = address;
            Err(denied())
        })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        Box::pin(async { Err(denied()) })
    }
}
pub(super) struct Lifecycle {
    pub(super) protocol: Box<dyn DynResourceLifecycle>,
    pub(super) resources: Arc<BoundResources>,
}
struct StartAccess<'a>(&'a AtomicBool);
impl Drop for StartAccess<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        self.resources.starting.store(true, Ordering::Release);
        let _access = StartAccess(&self.resources.starting);
        let _metadata_access = self
            .resources
            .metadata
            .as_ref()
            .map(|r| r.activate(deadline))
            .transpose()?;
        let _process_access = self
            .resources
            .process
            .as_ref()
            .map(|r| r.activate(deadline))
            .transpose()?;
        if let Some(handoff) = &self.resources.handoff {
            handoff.release_public(deadline).await?;
        }
        if let Some(files) = &self.resources.files {
            files.start(deadline).await?;
        }
        self.protocol.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.protocol.revoke(reason);
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(udp) = &self.resources.udp {
            udp.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.resources.starting.store(false, Ordering::Release);
        let view = self
            .resources
            .listener
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(view);
        self.resources.extra_tcp.lock().unwrap().clear();
        drop(self.resources.primary_udp.lock().unwrap().take());
        let mut report = self.protocol.shutdown(deadline).await?;
        if let Some(files) = &self.resources.files {
            report.append(files.shutdown(deadline).await?);
        }
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(udp) = &self.resources.udp {
            report.append(udp.close(deadline)?);
        }
        Ok(report)
    }
}
