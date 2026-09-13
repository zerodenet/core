use crate::runtime::path::{OutboundEndpoint, TcpPathCategory};

/// Runtime-neutral facts about one resolved outbound leaf.
///
/// The proxy runtime uses this for orchestration decisions without matching on
/// concrete protocol variants. Protocol-private fields remain owned by the
/// adapter that claimed the leaf.
#[derive(Debug, Clone)]
pub(crate) struct OutboundLeafRuntime {
    pub(crate) tag: Option<String>,
    pub(crate) protocol: String,
    pub(crate) tcp_path: TcpPathCategory,
    #[cfg(feature = "udp-runtime")]
    pub(crate) health_tag: Option<String>,
    pub(crate) endpoint: Option<OutboundEndpoint>,
    pub(crate) kernel_tag: Option<String>,
    #[cfg(feature = "udp-runtime")]
    pub(crate) udp_policy_tag: Option<String>,
}

impl OutboundLeafRuntime {
    pub(crate) fn virtual_outbound(tag: &str, protocol: &str, tcp_path: TcpPathCategory) -> Self {
        Self {
            tag: Some(tag.to_owned()),
            protocol: protocol.to_owned(),
            tcp_path,
            #[cfg(feature = "udp-runtime")]
            // An endpoint-free pool reports availability on each admission.
            // Dial health cooldowns would hide newly registered workers.
            health_tag: None,
            endpoint: None,
            kernel_tag: None,
            #[cfg(feature = "udp-runtime")]
            udp_policy_tag: Some(tag.to_owned()),
        }
    }

    pub(crate) fn proxy(
        tag: &str,
        protocol: &str,
        server: &str,
        port: u16,
        tcp_path: TcpPathCategory,
    ) -> Self {
        Self {
            tag: Some(tag.to_owned()),
            protocol: protocol.to_owned(),
            tcp_path,
            #[cfg(feature = "udp-runtime")]
            health_tag: Some(tag.to_owned()),
            endpoint: Some(OutboundEndpoint {
                server: server.to_owned(),
                port,
            }),
            kernel_tag: None,
            #[cfg(feature = "udp-runtime")]
            udp_policy_tag: Some(tag.to_owned()),
        }
    }
}

impl OutboundLeafRuntime {
    pub(crate) fn direct(tag: Option<&str>) -> Self {
        Self {
            tag: tag.map(str::to_owned),
            protocol: "direct".to_owned(),
            tcp_path: TcpPathCategory::Direct,
            #[cfg(feature = "udp-runtime")]
            health_tag: None,
            endpoint: None,
            kernel_tag: tag.map(str::to_owned),
            #[cfg(feature = "udp-runtime")]
            udp_policy_tag: tag.map(str::to_owned),
        }
    }

    pub(crate) fn block(tag: Option<&str>) -> Self {
        Self {
            tag: tag.map(str::to_owned),
            protocol: "block".to_owned(),
            tcp_path: TcpPathCategory::Block,
            #[cfg(feature = "udp-runtime")]
            health_tag: None,
            endpoint: None,
            kernel_tag: tag.map(str::to_owned),
            #[cfg(feature = "udp-runtime")]
            udp_policy_tag: tag.map(str::to_owned),
        }
    }
}
