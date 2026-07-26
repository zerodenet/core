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

fn compiled_protocol_registry() -> ProtocolRegistry {
    let mut registry = ProtocolRegistry::default();
    #[cfg(feature = "socks5")]
    {
        use crate::adapters::socks5::Socks5Adapter;
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
        let adapter = Arc::new(VlessAdapter::default());
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
        let adapter = Arc::new(TrojanAdapter::default());
        registry.register_managed_capability(adapter, TrojanAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "vmess")]
    {
        use crate::adapters::vmess::VmessAdapter;
        let adapter = Arc::new(VmessAdapter::default());
        registry.register_managed_capability(adapter, VmessAdapter::claim_outbound_leaf_impl);
    }
    #[cfg(feature = "mieru")]
    {
        use crate::adapters::mieru::MieruAdapter;
        registry.register_managed_capability(
            Arc::new(MieruAdapter),
            MieruAdapter::claim_outbound_leaf_impl,
        );
    }
    #[cfg(feature = "mixed")]
    {
        use crate::adapters::mixed::MixedAdapter;
        registry.register_core_capability(Arc::new(MixedAdapter), None);
    }
    #[cfg(feature = "udp-runtime")]
    {
        use crate::adapters::direct::DirectAdapter;
        registry.register_capability(
            Arc::new(DirectAdapter),
            DirectAdapter::claim_outbound_leaf_impl,
        );
    }
    #[cfg(not(feature = "udp-runtime"))]
    {
        use crate::adapters::direct::DirectAdapter;
        registry.register_core_capability(
            Arc::new(DirectAdapter),
            Some(DirectAdapter::claim_outbound_leaf_impl),
        );
    }
    registry
}

pub(crate) fn protocol_registry() -> ProtocolRegistry {
    compiled_protocol_registry()
}

pub(crate) fn compiled_protocol_features() -> Vec<String> {
    compiled_protocol_registry()
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
