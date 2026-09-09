use super::{
    Hysteria2AuthenticatedConnection, Hysteria2ManagedDatagramFlowResume,
    Hysteria2ManagedUdpPacketPathCarrierBuild, Hysteria2OutboundOptionsRef,
};
use std::sync::Arc;
use zero_core::Address;
use zero_transport::RuntimeError;

pub fn managed_datagram_connector_flow_from_resume(
    resume: &Hysteria2ManagedDatagramFlowResume,
    server: &str,
    port: u16,
) -> crate::udp::Hysteria2UdpConnectorFlow {
    resume.connector_flow(server, port)
}

async fn open_udp_profile_connection(
    server: &str,
    port: u16,
    profile: crate::udp::Hysteria2UdpConnectorProfile,
    pool: &super::Hysteria2ConnectionPool,
    tag: &str,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<Arc<Hysteria2AuthenticatedConnection>, RuntimeError> {
    let connection = super::pool::acquire(
        pool,
        tag,
        server,
        port,
        Hysteria2OutboundOptionsRef {
            password: profile.password(),
            client_fingerprint: profile.client_fingerprint(),
            server_name: profile.server_name(),
            insecure: profile.insecure(),
            settings: profile.settings(),
        },
        sockets,
    )
    .await?;
    connection.require_udp()?;
    Ok(connection)
}

pub async fn open_hysteria2_udp_packet_path_build(
    build: Hysteria2ManagedUdpPacketPathCarrierBuild,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<crate::udp::Hysteria2UdpChannel, RuntimeError> {
    let parts = build.protocol.into_connection_parts();
    let (server, port, profile, _) = parts.into_shared_codec_parts();
    let connection =
        open_udp_profile_connection(&server, port, profile, &build.pool, &build.tag, sockets)
            .await?;
    crate::udp::Hysteria2UdpChannel::new(connection).map_err(RuntimeError::Core)
}

pub async fn establish_hysteria2_udp_flow_connection(
    server: &str,
    port: u16,
    target: &Address,
    target_port: u16,
    payload: &[u8],
    resume: Hysteria2ManagedDatagramFlowResume,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<crate::udp::Hysteria2UdpFlowConnection, RuntimeError> {
    let flow = managed_datagram_connector_flow_from_resume(&resume, server, port);
    let profile = flow.into_connection_parts().into_profile();
    let connection =
        open_udp_profile_connection(server, port, profile, &resume.pool, &resume.tag, sockets)
            .await?;
    crate::udp::start_managed_udp_flow(
        connection,
        target,
        target_port,
        payload,
        resume.lifetime.subscribe(),
    )
    .map_err(RuntimeError::Core)
}
