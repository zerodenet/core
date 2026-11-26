//! The original UDP network projections, split without changing ownership.
use super::*;
impl UdpNetworkServices {
    #[cfg(any(feature = "peer-raw-ip-runtime", test))]
    pub(crate) fn from_tcp_execution(services: &super::TcpExecutionServices) -> Self {
        Self {
            upstream: services.upstream(),
            engine: services.engine().clone(),
        }
    }

    pub(crate) fn egress_generation(&self) -> u64 {
        self.upstream.egress_interface.generation()
    }

    pub(crate) fn direct_policy_is_current(
        &self,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
    ) -> bool {
        self.engine.runtime_snapshot().direct_dial_policy_matches(
            policy.tag.as_deref(),
            &policy.dial_policy,
            policy.generation,
        )
    }

    pub(crate) fn ensure_direct_policy_current(
        &self,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
    ) -> Result<(), zero_engine::EngineError> {
        if self.direct_policy_is_current(policy) {
            Ok(())
        } else {
            Err(zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "direct UDP dial policy changed while this flow was active",
            )))
        }
    }

    pub(crate) fn direct_datagram_selection(
        &self,
        peer: std::net::SocketAddr,
        policy: &zero_traits::DialPolicy,
    ) -> Result<zero_platform_tokio::EgressSelection, zero_engine::EngineError> {
        self.upstream
            .egress_interface
            .select_for_peer_with_policy(peer, policy)
            .map_err(Into::into)
    }

    pub(crate) async fn bind_direct_datagram_socket(
        &self,
        peer: std::net::SocketAddr,
        preferred_port: Option<u16>,
        policy: &zero_traits::DialPolicy,
        selection: &zero_platform_tokio::EgressSelection,
    ) -> Result<zero_platform_tokio::TokioDatagramSocket, zero_engine::EngineError> {
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_with_policy_preserving_port(
            peer,
            preferred_port,
            policy,
            selection,
        )
        .await
        .map_err(Into::into)
    }

    pub(crate) fn direct_datagram_observer(
        &self,
    ) -> Option<std::sync::Arc<dyn zero_traits::IoObserver>> {
        self.upstream.observer()
    }
}
