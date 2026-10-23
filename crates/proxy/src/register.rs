//! Compiled protocol registration boundary.

use std::sync::Arc;

use crate::protocol_registry::ProtocolRegistry;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::ManagedUdpHandlers;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_flow::registered::RegisteredUdpHandlers;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamUdpHandlers;

fn compiled_registries() -> (
    ProtocolRegistry,
    crate::component::ComponentRegistry<zero_config::OutboundConfig>,
) {
    use zero_traits::ProtocolMetadata;
    let mut components = crate::component::ComponentRegistry::default();
    components
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            crate::component::DirectFactory,
            crate::adapters::direct::factory::decode,
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
    #[cfg(feature = "socks5")]
    {
        use crate::adapters::socks5::Socks5Adapter;
        components
            .register(
                ::socks5::Socks5Protocol.descriptor(),
                ::socks5::component::Factory,
                crate::adapters::socks5::component_options,
            )
            .expect("unique SOCKS5 component registration");
        components
            .set_legacy_connect_stage("socks5", crate::adapters::socks5::LEGACY_CONNECT_STAGE);

        registry.register_upstream_capability(
            Arc::new(Socks5Adapter),
            Socks5Adapter::claim_outbound_leaf_impl,
        );
    }
    #[cfg(feature = "http")]
    {
        use crate::adapters::http::HttpConnectAdapter;
        registry.register_core_capability(Arc::new(HttpConnectAdapter), None);
    }
    #[cfg(feature = "vless")]
    {
        use crate::adapters::vless::VlessAdapter;
        components
            .register_where(
                ::vless::metadata::VlessProtocol.descriptor(),
                ::vless::component::Factory,
                crate::adapters::vless::native_input,
                crate::adapters::vless::component_options,
            )
            .expect("unique VLESS component registration");
        components.set_legacy_connect_stage("vless", crate::adapters::vless::LEGACY_CONNECT_STAGE);
        let adapter = Arc::new(VlessAdapter::default());
        registry.register_inbound_service(adapter.clone());
        registry.register_managed_capability(adapter, VlessAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "hysteria2")]
    {
        use crate::adapters::hysteria2::Hysteria2Adapter;
        let adapter = Arc::new(Hysteria2Adapter::default());
        registry.register_managed_capability(adapter, Hysteria2Adapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "shadowsocks")]
    {
        use crate::adapters::shadowsocks::ShadowsocksAdapter;
        let adapter = Arc::new(ShadowsocksAdapter::default());
        registry.register_managed_capability(adapter, ShadowsocksAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "trojan")]
    {
        use crate::adapters::trojan::TrojanAdapter;
        components
            .register_where(
                ::trojan::metadata::TrojanProtocol.descriptor(),
                ::trojan::component::Factory,
                crate::adapters::trojan::native_input,
                crate::adapters::trojan::component_options,
            )
            .expect("unique Trojan component registration");
        components
            .set_legacy_connect_stage("trojan", crate::adapters::trojan::LEGACY_CONNECT_STAGE);
        let adapter = Arc::new(TrojanAdapter::default());
        registry.register_managed_capability(adapter, TrojanAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "vmess")]
    {
        use crate::adapters::vmess::VmessAdapter;
        components
            .register_where(
                ::vmess::metadata::VmessProtocol.descriptor(),
                ::vmess::component::Factory,
                crate::adapters::vmess::native_input,
                crate::adapters::vmess::component_options,
            )
            .expect("unique VMess component registration");
        components.set_legacy_connect_stage("vmess", crate::adapters::vmess::LEGACY_CONNECT_STAGE);
        let adapter = Arc::new(VmessAdapter::default());
        registry.register_managed_capability(adapter, VmessAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "mieru")]
    {
        use crate::adapters::mieru::MieruAdapter;
        registry.register_managed_capability(
            Arc::new(MieruAdapter::default()),
            MieruAdapter::claim_outbound_leaf_impl,
        );
    }
    #[cfg(feature = "wireguard")]
    {
        use crate::adapters::wireguard::WireguardAdapter;
        let adapter = Arc::new(WireguardAdapter::default());
        registry.register_endpoint_observer(adapter.clone());
        registry.register_endpoint_controller(adapter.clone());
        registry.register_outbound_device_lifecycle(adapter.clone());
        registry.register_capability(adapter, WireguardAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "mixed")]
    {
        use crate::adapters::mixed::MixedAdapter;
        registry.register_core_capability(Arc::new(MixedAdapter), None);
    }
    #[cfg(feature = "udp-runtime")]
    {
        use crate::adapters::direct::DirectAdapter;
        let adapter = Arc::new(DirectAdapter::default());
        #[cfg(feature = "raw-ip-runtime")]
        registry.register_outbound_device_lifecycle(adapter.clone());
        registry.register_capability(adapter, DirectAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(not(feature = "udp-runtime"))]
    {
        use crate::adapters::direct::DirectAdapter;
        registry.register_core_capability(
            Arc::new(DirectAdapter::default()),
            Some(DirectAdapter::claim_outbound_leaf_impl),
        );
    }
    (registry, components)
}

pub(crate) fn component_registry(
) -> crate::component::ComponentRegistry<zero_config::OutboundConfig> {
    compiled_registries().1
}

pub(crate) fn protocol_registry() -> ProtocolRegistry {
    let (mut registry, components) = compiled_registries();
    // Preserve the exact factory state assembled beside the legacy adapters.
    // Reassembling later would create another protocol pool/resource owner.
    registry.components = Arc::new(crate::component::catalog::ComponentCatalog::with_factories(
        components,
    ));
    registry
}

pub(crate) fn compiled_protocol_features() -> Vec<String> {
    compiled_registries()
        .0
        .compiled_feature_names()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

#[cfg(feature = "udp-runtime")]
pub(crate) fn registered_udp_handlers(registry: &ProtocolRegistry) -> RegisteredUdpHandlers {
    #[cfg(feature = "managed-stream-runtime")]
    let (stream_packet, relay) = registry
        .managed_udp_handler_providers()
        .filter_map(|capability| capability.managed_stream_udp_handlers())
        .map(|handlers| (handlers.stream_packet, handlers.relay))
        .unzip();

    RegisteredUdpHandlers {
        #[cfg(any(
            feature = "managed-stream-runtime",
            feature = "managed-datagram-runtime"
        ))]
        managed: ManagedUdpHandlers {
            #[cfg(feature = "managed-datagram-runtime")]
            datagram: registry
                .managed_udp_handler_providers()
                .filter_map(|capability| capability.managed_datagram_udp_handler())
                .collect(),
            #[cfg(feature = "managed-stream-runtime")]
            stream_packet,
            #[cfg(feature = "managed-stream-runtime")]
            relay,
        },
        #[cfg(feature = "upstream-association-runtime")]
        upstream: UpstreamUdpHandlers {
            upstream: registry
                .upstream_udp_handler_providers()
                .map(|provider| provider.upstream_association_handler())
                .collect(),
        },
    }
}
