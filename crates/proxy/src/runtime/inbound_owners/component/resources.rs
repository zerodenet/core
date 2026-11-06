//! Original granted resources and protocol lifecycle over the same host provider.
use super::*;
pub(super) struct BoundResources {
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
    fn register_packet_source_driver(
        &self,
        original: &zero_platform_tokio::controlled_datagram::listener::DatagramListenerIdentity,
        witness: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin, BuildError>
    {
        self.packet_native
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(denied)?
            .register_driver(original, witness)
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
