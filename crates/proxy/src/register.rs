//! Compiled protocol registration boundary.

use std::sync::Arc;

use crate::protocol_registry::ProtocolRegistry;
pub(crate) fn compiled_registries() -> (
    ProtocolRegistry,
    crate::component::ComponentRegistry<zero_config::OutboundConfig>,
    Arc<crate::adapters::devices::Devices>,
) {
    compiled_registries_with_background(None)
}
pub(crate) fn compiled_registries_with_background(
    background: Option<Arc<crate::component::background::BackgroundNetwork>>,
) -> (
    ProtocolRegistry,
    crate::component::ComponentRegistry<zero_config::OutboundConfig>,
    Arc<crate::adapters::devices::Devices>,
) {
    compiled_registries_with_network(background.clone(), background)
}
pub(crate) fn compiled_registries_with_network(
    background: Option<Arc<crate::component::background::BackgroundNetwork>>,
    node_network: Option<Arc<crate::component::background::BackgroundNetwork>>,
) -> (
    ProtocolRegistry,
    crate::component::ComponentRegistry<zero_config::OutboundConfig>,
    Arc<crate::adapters::devices::Devices>,
) {
    let _ = &background;
    use zero_traits::ProtocolMetadata;
    let mut components = crate::component::ComponentRegistry::default();
    components
        .register_with_resources(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            crate::component::DirectFactory,
            {
                let network = node_network.clone();
                move |input, resources| {
                    if let Some(network) = &network {
                        crate::component::native_dns::prepare(resources, network.clone())?;
                        crate::component::native_dns::prepare_node(resources, network.clone())?;
                    }
                    crate::adapters::direct::factory::decode(input)
                        .map_err(crate::component::RegistrationError::Options)
                }
            },
        )
        .expect("unique built-in Direct registration");
    components
        .declare("socks5", "socks5")
        .expect("unique SOCKS5 declaration");
    components
        .declare("trojan", "trojan")
        .expect("unique Trojan declaration");
    components
        .declare("vmess", "vmess")
        .expect("unique VMess declaration");
    components
        .declare("vless", "vless")
        .expect("unique VLESS declaration");
    let mut registry = ProtocolRegistry::default();
    registry
        .inbounds
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            crate::adapters::direct::inbound::Factory,
            crate::adapters::direct::inbound::options,
        )
        .expect("unique built-in Direct inbound component registration");
    let mut devices = crate::adapters::devices::Devices::default();
    devices.set_node_network(node_network.as_ref().or(background.as_ref()));
    #[cfg(feature = "socks5")]
    {
        use crate::adapters::socks5::Socks5Adapter;
        components
            .register_with_resources(
                ::socks5::Socks5Protocol.descriptor(),
                ::socks5::component::Factory,
                {
                    let native = node_network.is_some();
                    move |input, resources| {
                        crate::adapters::socks5::component_options(input)
                            .map(|options| {
                                if native {
                                    let limit = resources
                                        .candidate()
                                        .map(|config| {
                                            config.runtime.effective_native_stream_max_originals()
                                        })
                                        .unwrap_or(4);
                                    options.with_carrier_limit(
                                        std::num::NonZeroUsize::new(limit).unwrap(),
                                    )
                                } else {
                                    options
                                }
                            })
                            .map_err(crate::component::RegistrationError::Options)
                    }
                },
            )
            .expect("unique SOCKS5 component registration");
        components
            .set_legacy_connect_stage("socks5", crate::adapters::socks5::LEGACY_CONNECT_STAGE);

        registry
            .inbounds
            .register(
                ::socks5::Socks5Protocol.descriptor(),
                ::socks5::component::listener::InboundFactory,
                crate::adapters::socks5::inbound::component_options,
            )
            .expect("unique SOCKS5 inbound component registration");
        registry.register_standard_protocol(
            Arc::new(Socks5Adapter),
            Some(Socks5Adapter::claim_outbound_leaf_impl),
        );
    }
    #[cfg(feature = "http")]
    {
        use crate::adapters::http::HttpConnectAdapter;
        registry
            .inbounds
            .register(
                ::http::HttpConnectProtocol.descriptor(),
                ::http::component::InboundFactory,
                crate::adapters::http::component_options,
            )
            .expect("unique HTTP inbound component registration");
        registry.register_standard_protocol(Arc::new(HttpConnectAdapter), None);
    }
    #[cfg(feature = "vless")]
    {
        use crate::adapters::vless::VlessAdapter;
        let adapter = Arc::new(VlessAdapter::default());
        if let Some(network) = background.clone() {
            let factory = Arc::new(crate::component::background::pool::Factory::new(network));
            let projected = adapter.clone();
            components
                .register_with_resources_where(
                    ::vless::metadata::VlessProtocol.descriptor(),
                    crate::adapters::vless::component::mux::Factory,
                    crate::adapters::vless::component::mux::eligible,
                    move |input, resources| {
                        projected.owned_mux_options(input, resources, factory.clone())
                    },
                )
                .expect("unique owned vless component registration");
        } else if let Some(network) = node_network.clone() {
            let factory = Arc::new(crate::component::background::pool::Factory::new(network));
            let projected = adapter.clone();
            let selected = adapter.clone();
            components
                .register_with_resources_where(
                    ::vless::metadata::VlessProtocol.descriptor(),
                    crate::adapters::vless::component::mux::Factory,
                    crate::adapters::vless::component::mux::eligible,
                    move |input, resources| {
                        projected.production_mux_options(input, resources, factory.clone())
                    },
                )
                .expect("unique ordinary vless prefix registration");
            components.set_candidate_acceptance("vless", move |input, config| {
                selected.production_selected(input, config, true)
            });
        } else {
            components
                .register_where(
                    ::vless::metadata::VlessProtocol.descriptor(),
                    ::vless::component::Factory,
                    crate::adapters::vless::native_input,
                    crate::adapters::vless::component_options,
                )
                .expect("unique VLESS component registration");
        }
        components.set_legacy_connect_stage("vless", crate::adapters::vless::LEGACY_CONNECT_STAGE);
        let inbound_adapter = adapter.clone();
        registry
            .inbounds
            .register_with_resources(
                ::vless::metadata::VlessProtocol.descriptor(),
                ::vless::component::inbound::listener::Factory,
                move |input, resources| {
                    inbound_adapter.inbound_options(input, resources.source_dir())
                },
            )
            .expect("unique standard VLESS inbound registration");
        registry.register_inbound_service(adapter.clone());
        registry.register_standard_protocol(adapter, Some(VlessAdapter::claim_outbound_leaf_impl));
    }
    #[cfg(feature = "hysteria2")]
    {
        use crate::adapters::hysteria2::Hysteria2Adapter;
        let adapter = Arc::new(Hysteria2Adapter::default());
        let projected = adapter.clone();
        registry
            .inbounds
            .register_with_resources(
                ::hysteria2::Hysteria2Protocol.descriptor(),
                ::hysteria2::component::Factory,
                move |input, resources| projected.inbound_options(input, resources.source_dir()),
            )
            .expect("unique standard Hysteria2 inbound registration");
        if let Some(network) = node_network.clone() {
            let factory = Arc::new(crate::adapters::hysteria2::outbound::Factory::new(network));
            let projected = adapter.clone();
            components
                .register_with_resources(
                    ::hysteria2::Hysteria2Protocol.descriptor(),
                    ::hysteria2::transport::owned::RoleFactory,
                    move |input, resources| {
                        projected.outbound_options(input, resources, factory.clone())
                    },
                )
                .expect("unique standard Hysteria2 outbound registration");
            components.set_legacy_connect_stage("hysteria2", "connect_upstream_hysteria2");
        }
        registry.register_standard_protocol(adapter, None);
    }
    #[cfg(feature = "shadowsocks")]
    {
        use crate::adapters::shadowsocks::ShadowsocksAdapter;
        let adapter = Arc::new(ShadowsocksAdapter::default());
        let inbound_adapter = adapter.clone();
        registry
            .inbounds
            .register_with_resources(
                ::shadowsocks::ShadowsocksProtocol.descriptor(),
                ::shadowsocks::component::inbound::listener::Factory,
                move |input, _resources| inbound_adapter.inbound_options(input),
            )
            .expect("unique native Shadowsocks inbound registration");
        let component_adapter = adapter.clone();
        let replay_factory = Arc::new(::shadowsocks::component::ReplayFactory);
        components
            .register_with_resources_where(
                ::shadowsocks::ShadowsocksProtocol.descriptor(),
                crate::component::background::stateless::Factory(
                    ::shadowsocks::component::StreamFactory,
                ),
                crate::adapters::shadowsocks::component::native_input,
                move |input, resources| {
                    component_adapter.component_options(input, resources, replay_factory.clone())
                },
            )
            .expect("unique Shadowsocks component registration");
        components.set_legacy_connect_stage("shadowsocks", "connect_upstream_shadowsocks");
        registry.register_standard_protocol(
            adapter,
            Some(ShadowsocksAdapter::claim_outbound_leaf_impl),
        );
    }
    #[cfg(feature = "trojan")]
    {
        use crate::adapters::trojan::TrojanAdapter;
        let adapter = Arc::new(TrojanAdapter::default());
        if let Some(network) = background.clone() {
            let factory = Arc::new(crate::component::background::pool::Factory::new(network));
            let projected = adapter.clone();
            components
                .register_with_resources_where(
                    ::trojan::metadata::TrojanProtocol.descriptor(),
                    crate::adapters::trojan::component::mux::Factory,
                    crate::adapters::trojan::component::mux::eligible,
                    move |input, resources| {
                        projected.owned_mux_options(input, resources, factory.clone())
                    },
                )
                .expect("unique owned trojan component registration");
        } else if let Some(network) = node_network.clone() {
            let factory = Arc::new(crate::component::background::pool::Factory::new(network));
            let projected = adapter.clone();
            let selected = adapter.clone();
            components
                .register_with_resources_where(
                    ::trojan::metadata::TrojanProtocol.descriptor(),
                    crate::adapters::trojan::component::mux::Factory,
                    crate::adapters::trojan::component::mux::eligible,
                    move |input, resources| {
                        projected.production_mux_options(input, resources, factory.clone())
                    },
                )
                .expect("unique ordinary trojan prefix registration");
            components.set_candidate_acceptance("trojan", move |input, config| {
                selected.production_selected(input, config, true)
            });
        } else {
            components
                .register_where(
                    ::trojan::metadata::TrojanProtocol.descriptor(),
                    ::trojan::component::Factory,
                    crate::adapters::trojan::native_input,
                    crate::adapters::trojan::component_options,
                )
                .expect("unique Trojan component registration");
        }
        components
            .set_legacy_connect_stage("trojan", crate::adapters::trojan::LEGACY_CONNECT_STAGE);
        let inbound = adapter.clone();
        registry
            .inbounds
            .register_with_resources(
                adapter.descriptor(),
                ::trojan::component::inbound::listener::Factory,
                move |input, resources| inbound.inbound_options(input, resources.source_dir()),
            )
            .expect("unique Trojan standard inbound registration");
        registry.register_standard_protocol(adapter, Some(TrojanAdapter::claim_outbound_leaf_impl));
    }
    #[cfg(feature = "vmess")]
    {
        use crate::adapters::vmess::VmessAdapter;
        let mut adapter = VmessAdapter::default();
        adapter.native_node_dns = node_network.is_some() && background.is_none();
        let adapter = Arc::new(adapter);
        if let Some(network) = background.clone() {
            let factory = Arc::new(crate::component::background::pool::Factory::new(network));
            let projected = adapter.clone();
            components
                .register_with_resources_where(
                    ::vmess::metadata::VmessProtocol.descriptor(),
                    crate::adapters::vmess::component::mux::Factory,
                    crate::adapters::vmess::component::mux::eligible,
                    move |input, resources| {
                        projected.owned_mux_options(input, resources, factory.clone())
                    },
                )
                .expect("unique owned vmess component registration");
        } else {
            components
                .register_with_resources_where(
                    ::vmess::metadata::VmessProtocol.descriptor(),
                    crate::adapters::vmess::component::mux::Factory,
                    |input| {
                        crate::adapters::vmess::component::grpc::eligible(input)
                            || crate::adapters::vmess::component::mux::eligible(input)
                    },
                    {
                        let adapter = adapter.clone();
                        let network = node_network.clone();
                        let pool = network.clone().map(|network| {
                            Arc::new(crate::component::background::pool::Factory::new(network))
                        });
                        move |input, resources| {
                            if crate::adapters::vmess::component::mux::has_mux(input) {
                                return adapter.production_mux_options(
                                    input,
                                    resources,
                                    pool.clone().expect("selected production MUX provider"),
                                );
                            }
                            let mut options = adapter
                                .standard_options_for(input, resources.candidate())
                                .map_err(crate::component::RegistrationError::Options)?;
                            if matches!(
                                &options,
                                crate::adapters::vmess::component::grpc::Options::Grpc(_)
                            ) && network.is_some()
                                && resources.candidate().is_some_and(|candidate| {
                                    zero_dns::native::validate_node_config(
                                        candidate.runtime.dns.as_ref(),
                                    )
                                    .is_ok()
                                })
                            {
                                let dns = crate::component::native_dns::prepare_node(
                                    resources,
                                    network.clone().expect("selected native node DNS"),
                                )?;
                                options.bind_node_dns(dns);
                            }
                            Ok(crate::adapters::vmess::component::mux::Options::Ordinary(
                                Arc::new(options),
                            ))
                        }
                    },
                )
                .expect("unique VMess component registration");
        }
        if background.is_none() {
            let native = node_network.is_some();
            let selector = adapter.clone();
            components.set_candidate_acceptance("vmess", move |input, config| {
                selector.production_selected(input, config, native)
            });
        }
        components.set_legacy_connect_stage("vmess", crate::adapters::vmess::LEGACY_CONNECT_STAGE);
        let inbound_adapter = adapter.clone();
        registry
            .inbounds
            .register_with_resources(
                adapter.descriptor(),
                ::vmess::component::inbound::listener::Factory,
                move |inbound, resources| {
                    inbound_adapter.inbound_options(inbound, resources.source_dir())
                },
            )
            .expect("unique standard VMess inbound registration");
        registry.register_standard_protocol(adapter, Some(VmessAdapter::claim_outbound_leaf_impl));
    }
    #[cfg(feature = "mieru")]
    {
        use crate::adapters::mieru::MieruAdapter;
        let adapter = Arc::new(MieruAdapter::default());
        registry
            .inbounds
            .register_with_resources(
                adapter.descriptor(),
                ::mieru::component::inbound::Factory,
                |input, _| crate::adapters::mieru::inbound::options(input),
            )
            .expect("unique standard Mieru inbound registration");
        if let Some(network) = node_network {
            let factory = Arc::new(crate::adapters::mieru::component::Factory::new(network));
            let component_adapter = adapter.clone();
            components
                .register_with_resources(
                    ::mieru::MieruProtocol.descriptor(),
                    ::mieru::component::StreamFactory,
                    move |input, resources| {
                        component_adapter.component_options(input, resources, factory.clone())
                    },
                )
                .expect("unique Mieru component registration");
            components.set_candidate_acceptance("mieru", |_, candidate| {
                crate::adapters::mieru::component::production_selected(candidate)
            });
        }
        registry.register_standard_protocol(adapter, Some(MieruAdapter::claim_outbound_leaf_impl));
    }
    #[cfg(feature = "wireguard")]
    {
        use crate::adapters::wireguard::WireguardAdapter;
        let adapter = Arc::new(WireguardAdapter::default());
        registry.register_endpoint_observer(adapter.clone());
        registry.register_endpoint_controller(adapter.clone());
        devices.wireguard = Some(adapter.clone());
        let inbound_adapter = adapter.clone();
        registry
            .inbounds
            .register_with_resources(
                adapter.descriptor(),
                crate::adapters::wireguard::inbound::cold::Factory,
                move |input, _| Ok(inbound_adapter.inbound_options(input)?),
            )
            .expect("unique standard WireGuard inbound registration");
        let outbound_adapter = adapter.clone();
        components
            .register_with_resources(
                adapter.descriptor(),
                crate::adapters::wireguard::lifecycle::LinkedOutboundFactory,
                move |input, resources| {
                    outbound_adapter
                        .linked_outbound_options(input, resources)
                        .map_err(crate::component::RegistrationError::from)
                },
            )
            .expect("unique canonical WireGuard outbound registration");
        components.set_candidate_acceptance("wireguard", |outbound, _| {
            matches!(
                &outbound.protocol,
                zero_config::OutboundProtocolConfig::Wireguard {
                    inbound_tag: Some(_),
                    outer_udp_proxy: None,
                    ..
                }
            )
        });
        registry
            .register_standard_protocol(adapter, Some(WireguardAdapter::claim_outbound_leaf_impl));
    }
    #[cfg(feature = "mixed")]
    {
        use crate::adapters::mixed::MixedAdapter;
        registry
            .inbounds
            .register(
                MixedAdapter.descriptor(),
                crate::adapters::mixed::inbound::Factory,
                crate::adapters::mixed::inbound::component_options,
            )
            .expect("unique mixed inbound component registration");
        registry.register_standard_protocol(Arc::new(MixedAdapter), None);
    }
    #[cfg(feature = "udp-runtime")]
    {
        use crate::adapters::direct::DirectAdapter;
        let adapter = Arc::new(DirectAdapter::default());
        #[cfg(feature = "raw-ip-runtime")]
        {
            devices.direct = Some(adapter.clone());
        }
        registry.register_standard_protocol(adapter, Some(DirectAdapter::claim_outbound_leaf_impl));
    }
    #[cfg(not(feature = "udp-runtime"))]
    {
        use crate::adapters::direct::DirectAdapter;
        registry.register_standard_protocol(
            Arc::new(DirectAdapter::default()),
            Some(DirectAdapter::claim_outbound_leaf_impl),
        );
    }
    (registry, components, Arc::new(devices))
}

#[cfg(test)]
pub(crate) fn component_registry()
-> crate::component::ComponentRegistry<zero_config::OutboundConfig> {
    compiled_registries().1
}

#[cfg(test)]
pub(crate) fn protocol_registry() -> ProtocolRegistry {
    compiled_registries().0
}

pub(crate) fn compiled_protocol_features() -> Vec<String> {
    compiled_registries()
        .0
        .compiled_feature_names()
        .into_iter()
        .map(str::to_owned)
        .collect()
}
