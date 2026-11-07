use super::*;
use crate::component::ResourcePreparation;
use zero_config::RuntimeConfig;
use zero_engine::EngineError;
impl DirectAdapter {
    pub(crate) fn prepare_packet_component(
        self: &Arc<Self>,
        config: &Arc<RuntimeConfig>,
        resources: &mut ResourcePreparation<'_>,
        totals: Arc<crate::runtime::raw_ip::PacketReturnTotals>,
    ) -> Result<Option<ComponentId>, EngineError> {
        let Some(binding) = config.runtime.network.direct_packet_device.clone() else {
            self.packet_candidate.lock().unwrap().take();
            return Ok(None);
        };
        let stack = crate::runtime::raw_ip::RawIpStackOptions::from_config(
            &config.runtime.network.udp_receive_queue,
        )
        .with_packet_native_read_limits(
            config
                .runtime
                .effective_packet_native_read_total_max_frames(),
            config
                .runtime
                .effective_packet_native_read_total_max_bytes(),
        )
        .with_packet_transmit_limits(
            config.runtime.effective_packet_transmit_total_max_frames(),
            config.runtime.effective_packet_transmit_total_max_bytes(),
        )
        .with_packet_return_limits(
            config.runtime.effective_packet_return_max_frames(),
            config.runtime.effective_packet_return_max_bytes(),
        )
        .with_packet_return_totals(
            totals,
            config.runtime.effective_packet_return_total_max_frames(),
            config.runtime.effective_packet_return_total_max_bytes(),
        );
        let desired = Options::new(binding, config.runtime.network.mtu, stack)
            .map_err(std::io::Error::other)?;
        let (root, fresh) = {
            let previous = self.packet_candidate.lock().unwrap();
            let reusable = previous
                .as_ref()
                .filter(|old| old.root.spec == desired.spec)
                .filter(|old| {
                    // Repeated decode within this exact cold pass must keep its
                    // already selected immutable original Options, including cold
                    // scopes. An unrelated pass must pass actual scope reuse.
                    let same_pass = old
                        .config
                        .upgrade()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, config))
                        && resources.resources().any(|entry| {
                            entry.component() == old.id && entry.recipe().spec() == &old.root.spec
                        });
                    same_pass
                        || resources.can_reuse_shared_resource(
                            NAMESPACE,
                            &old.root.spec,
                            old.root.scope.lock().unwrap().as_ref(),
                        )
                });
            match reusable {
                Some(old) => (old.root.clone(), false),
                None => (Arc::new(desired), true),
            }
        };
        let id = resources
            .prepare_component(NAMESPACE, Arc::new(factory::Factory), root.clone())
            .map_err(std::io::Error::other)?;
        if fresh {
            resources
                .require_fresh_resource(id)
                .map_err(std::io::Error::other)?;
        }
        *self.packet_candidate.lock().unwrap() = Some(Candidate {
            config: Arc::downgrade(config),
            id,
            root,
        });
        Ok(Some(id))
    }
    pub(crate) fn direct_options(
        &self,
        input: &zero_config::OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<super::super::super::factory::DirectOptions, crate::component::RegistrationError>
    {
        let mut options = super::super::super::factory::decode(input)
            .map_err(crate::component::RegistrationError::Options)?;
        let Some(config) = resources.candidate() else {
            return Ok(options);
        };
        if config.runtime.network.direct_packet_device.is_none()
            || options.policy != zero_traits::DialPolicy::default()
        {
            return Ok(options);
        }
        let candidate = self.packet_candidate.lock().unwrap();
        let candidate = candidate
            .as_ref()
            .filter(|candidate| {
                candidate
                    .config
                    .upgrade()
                    .is_some_and(|actual| std::ptr::eq(actual.as_ref(), config))
                    && resources.resources().any(|entry| {
                        entry.component() == candidate.id
                            && entry.recipe().spec() == &candidate.root.spec
                    })
            })
            .ok_or_else(|| {
                crate::component::RegistrationError::Planning(error(
                    Reason::StalePlan,
                    "Direct Packet root differs from the same actual cold pass",
                ))
            })?;
        options.packet = Some(Alias {
            root: candidate.root.clone(),
            id: candidate.id,
        });
        Ok(options)
    }
    pub(crate) async fn prepare_hosted_packet(
        &self,
        config: &RuntimeConfig,
        context: OutboundDevicePreparationContext,
    ) -> Result<Prepared, EngineError> {
        if context.direct_packet_device.is_none() {
            self.close_devices().await?;
            return Ok(Prepared {
                next: None,
                fresh: false,
                published: false,
            });
        }
        let next = {
            let candidate = self.packet_candidate.lock().unwrap();
            candidate
                .as_ref()
                .filter(|candidate| {
                    candidate
                        .config
                        .upgrade()
                        .is_some_and(|actual| std::ptr::eq(actual.as_ref(), config))
                })
                .and_then(|candidate| candidate.root.device())
                .ok_or_else(|| {
                    std::io::Error::other(
                        "same candidate Direct Packet native driver is not installed",
                    )
                })?
        };
        if context.direct_packet_device.as_ref() != Some(&next.config)
            || usize::from(context.mtu) != next.mtu
        {
            return Err(std::io::Error::other(
                "Direct Packet publication differs from actual native declaration",
            )
            .into());
        }
        let fresh = !self
            .packet
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, &next));
        *self.packet.lock().unwrap() = Some(next.clone());
        Ok(Prepared {
            next: Some(next),
            fresh,
            published: false,
        })
    }
}
impl Options {
    fn new(
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
        stack: crate::runtime::raw_ip::RawIpStackOptions,
    ) -> Result<Self, ChannelError> {
        let binding = match config.backend {
            zero_config::DirectPacketDeviceBackend::Descriptor => PacketDeviceBinding::Descriptor {
                fd: config
                    .fd
                    .ok_or_else(|| error(Reason::InvalidConfig, "Direct descriptor is missing"))?,
                interface: config.interface.clone().into(),
            },
            zero_config::DirectPacketDeviceBackend::Wintun => {
                PacketDeviceBinding::ExistingWindows {
                    interface: config.interface.clone().into(),
                    addresses: config.router_addresses.clone(),
                    mtu,
                }
            }
        };
        use sha2::{Digest, Sha256};
        let identity = stack.identity(
            Sha256::digest(serde_json::to_vec(&(&config, mtu)).map_err(|source| {
                error(
                    Reason::InvalidConfig,
                    "Direct Packet identity encoding failed",
                )
                .with_source(source)
            })?)
            .into(),
        );
        Ok(Self {
            config,
            mtu,
            stack,
            spec: ComponentSpec {
                owner: ResourceOwner::Component,
                roles: vec![RoleKind::Packet],
                dependencies: vec![],
                static_identity: identity.to_vec().into(),
                resources: vec![ResourceDeclaration::ControlledPacketDevice {
                    name: NATIVE_NAME.into(),
                    binding,
                }],
                task_limit: std::num::NonZeroUsize::new(if cfg!(target_os = "windows") {
                    2
                } else {
                    1
                }),
            },
            role: OnceLock::new(),
            scope: Mutex::new(None),
        })
    }
}

impl DirectAdapter {
    pub(crate) fn packet_alias_recipe(
        &self,
        input: &zero_config::OutboundConfig,
        resources: &mut crate::component::ResourcePreparation<'_>,
    ) -> Result<Option<crate::component::ComponentRecipe>, ChannelError> {
        let options = self.direct_options(input, resources).map_err(|source| {
            error(
                Reason::StalePlan,
                "Direct recovery alias differs from this actual cold pass",
            )
            .with_source(source)
        })?;
        if options.packet.is_none() {
            return Ok(None);
        }
        Ok(Some(crate::component::ComponentRecipe::from_validated(
            Arc::new(super::super::super::factory::DirectFactory),
            Arc::new(options),
        )))
    }
}
