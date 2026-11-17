//! All real peer drivers precede the one software Source registration.
//! Original outer native debt stays with its real callee, independently.
use super::*;
use crate::component::background::packet_driver::PacketCodecReceiveGroups;
use zero_platform_tokio::contract::{
    component::GrantedLogicalPacketWorkspaces, network::CarrierContext, scope::TaskJoinWitness,
};
pub(super) struct Original {
    grants: GrantedLogicalPacketWorkspaces,
    driver: TaskJoinWitness,
    endpoint: SocketAddr,
    input: Mutex<
        Option<(
            crate::runtime::raw_ip::PendingNativeWire,
            crate::runtime::packet_route::host::pause::Checkpoint,
        )>,
    >,
    codecs: PacketCodecReceiveGroups,
    carrier: OnceLock<Arc<dyn crate::runtime::raw_ip::RawIpWireCarrier>>,
}
impl StandaloneStackOptions {
    #[expect(
        clippy::too_many_arguments,
        reason = "original generation, caller, native and budget inputs remain explicitly bound"
    )]
    pub(super) async fn start_logical_peer(
        &self,
        index: usize,
        endpoint: SocketAddr,
        options: RawIpStackOptions,
        tunnel: Box<dyn crate::runtime::raw_ip::RawIpTunnel>,
        checkpoint: crate::runtime::packet_route::host::pause::Checkpoint,
        resources: &dyn ResourceAccess,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let memory = resources.packet_driver_metadata()?;
        let peer = &self.peers[index];
        let (recipe, mut pending) = crate::runtime::raw_ip::WireStackRecipe::prepare_logical(
            self.plan.local_addresses(),
            self.plan.mtu(),
            endpoint,
            tunnel,
            options.with_packet_memory_cell(peer.memory.clone()),
            &memory,
        )
        .map_err(|source| failure(Stage::Prepare, source))?;
        peer.owner
            .start_wire(recipe)
            .map_err(|source| failure(Stage::Prepare, source))?;
        let driver = peer
            .owner
            .driver_witness()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        peer.pause
            .get()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .bind(&driver)?;
        pending
            .wait_input_pending(deadline)
            .await
            .map_err(|source| failure(Stage::Prepare, source))?;
        let grants = resources.take_logical_packet_workspaces(index, &driver)?;
        if grants.control.is_none() {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let view = grants.reads[0]
            .memory_view()
            .map_err(|source| error(Stage::Prepare, Reason::Revoked).with_source(source))?;
        if !view
            .driver_metadata_view()
            .map_err(|source| error(Stage::Prepare, Reason::Revoked).with_source(source))?
            .same_root(&memory)
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        peer.memory
            .set(view)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        let layouts = self
            .outer
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?
            .codec_receive_layouts()
            .map_err(|source| failure(Stage::Prepare, source))?;
        let codecs = PacketCodecReceiveGroups::from_originals(&memory, &grants.receive, &layouts)?;
        peer.logical
            .set(Original {
                grants,
                driver,
                endpoint,
                input: Mutex::new(Some((pending, checkpoint))),
                codecs,
                carrier: OnceLock::new(),
            })
            .map_err(|_returned| error(Stage::Prepare, Reason::StalePlan))?;
        Ok(())
    }
    pub(super) fn register_logical_source(
        &self,
        resources: &dyn ResourceAccess,
    ) -> Result<(), ChannelError> {
        let memory = resources.packet_driver_metadata()?;
        let mut drivers = zero_platform_tokio::contract::component::PacketMetadataVec::default();
        for peer in &self.peers {
            let driver = peer
                .logical
                .get()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
                .driver
                .clone();
            drivers
                .try_push_with_metadata(driver, &memory, self.peers.len())
                .map_err(|(source, returned)| {
                    drop(returned);
                    error(Stage::Prepare, Reason::Capacity).with_source(source)
                })?;
        }
        if drivers.is_empty() {
            return Ok(());
        }
        let source = resources.register_software_packet_source_drivers(&drivers)?;
        resources.prepare_logical_packet_source(&source)?;
        resources.install_logical_packet_source(source.clone())?;
        self.logical_source
            .set(source)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        Ok(())
    }
    pub(in crate::adapters::wireguard::lifecycle) async fn install_outer(
        &self,
        path: &crate::adapters::devices::PreparedDevicePacketPath,
    ) -> Result<(), EngineError> {
        let outer = self
            .outer
            .as_ref()
            .ok_or_else(|| invalid("plain standalone received an outer Path"))?;
        if path.recipe.binding().network_generation != self.binding.network_generation {
            return Err(invalid(
                "same standalone outer Path changed network generation",
            ));
        }
        let network = outer
            .network()
            .upgrade()
            .ok_or_else(|| invalid("same standalone Host retired"))?;
        let (resources, tasks, cancel) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.stopped {
                return Err(invalid("same standalone outer Source stopped"));
            }
            (
                state
                    .resources
                    .clone()
                    .ok_or_else(|| invalid("standalone actual resources missing"))?,
                state
                    .tasks
                    .clone()
                    .ok_or_else(|| invalid("standalone actual registrar missing"))?,
                state
                    .cancel
                    .clone()
                    .ok_or_else(|| invalid("standalone actual cancel missing"))?,
            )
        };
        for (index, peer) in self.peers.iter().enumerate() {
            let original = peer
                .logical
                .get()
                .ok_or_else(|| invalid("same standalone peer input is missing"))?;
            let pending = original
                .input
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some();
            if pending {
                let carrier = match original.carrier.get() {
                    Some(carrier) => carrier.clone(),
                    None => {
                        let grant = resources
                            .packet_driver_caller(
                                &self.carrier_origin(index).map_err(std::io::Error::other)?,
                                &original.driver,
                                path.recipe.binding().clone(),
                                CarrierContext {
                                    session: None,
                                    deadline: None,
                                    cancel: cancel.clone(),
                                },
                            )
                            .map_err(std::io::Error::other)?;
                        let execution = network
                            .packet_driver_execution(grant, Arc::downgrade(&resources))
                            .map_err(std::io::Error::other)?
                            .with_prepaid_receive(original.codecs.clone())
                            .map_err(std::io::Error::other)?;
                        let carrier: Arc<dyn crate::runtime::raw_ip::RawIpWireCarrier> = Arc::new(
                            crate::runtime::raw_ip::RegisteredProxiedRawIpWireCarrier::prepare(
                                path.recipe.clone(),
                                execution,
                                tasks.clone(),
                                original.endpoint,
                            )?,
                        );
                        original.carrier.set(carrier.clone()).map_err(|_| {
                            invalid("same standalone outer controller was already installed")
                        })?;
                        carrier
                    }
                };
                carrier.initialize().await?;
                // The original input is consumed synchronously AFTER the same
                // attempt's actual Path/controller is retained. Cancellation of
                // the build never drops this input or replaces its driver.
                let (input, checkpoint) = original
                    .input
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .ok_or_else(|| invalid("same standalone input was already consumed"))?;
                input.install_logical(
                    carrier,
                    original.grants.reads.clone(),
                    original
                        .grants
                        .control
                        .as_ref()
                        .ok_or_else(|| invalid("same control workspace is missing"))?
                        .clone(),
                    original.driver.clone(),
                    checkpoint,
                )?;
            }
            self.device(index)
                .map_err(std::io::Error::other)?
                .wait_ready()
                .await?;
        }
        Ok(())
    }
}

impl Original {
    pub(super) fn request_stop(&self) {
        if let Some(carrier) = self.carrier.get() {
            carrier.request_stop();
        }
    }

    pub(super) fn carrier_retired(&self) -> bool {
        self.carrier
            .get()
            .is_none_or(|carrier| carrier.retirement_complete())
    }

    /// Covers pre-handoff originals as well as the carrier installed into the
    /// real wire. The caller first stops/joins that wire, never its ancestor.
    pub(super) async fn close_carrier(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), EngineError> {
        match self.carrier.get() {
            Some(carrier) => tokio::time::timeout_at(deadline, carrier.close_native())
                .await
                .map_err(|source| std::io::Error::new(std::io::ErrorKind::TimedOut, source))?,
            None => Ok(()), // No carrier acquisition was started for this peer.
        }
    }
}
