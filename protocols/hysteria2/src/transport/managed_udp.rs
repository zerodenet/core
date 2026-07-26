use std::sync::Arc;

use zero_core::Address;
use zero_traits::DatagramCodec;
use zero_transport::RuntimeError;

use super::{
    auth::authenticate_http3, connection::negotiated_alpn, open_quic_connection,
    outbound_quic_alpn_protocols, Hysteria2AuthenticatedConnection,
    Hysteria2ManagedDatagramFlowResume, Hysteria2ManagedUdpPacketPathCarrierBuild,
    Hysteria2QuicProfile, QuicConnectionOptions,
};

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
) -> Result<Arc<Hysteria2AuthenticatedConnection>, RuntimeError> {
    let quic_profile = Hysteria2QuicProfile::from_parts(profile.client_fingerprint());
    let connection = open_quic_connection(QuicConnectionOptions {
        server,
        port,
        alpn: outbound_quic_alpn_protocols(),
        quic_profile,
        datagram_receive_buffer_size: Some(65536),
    })
    .await?;
    if negotiated_alpn(&connection).as_deref() == Some(b"h3") {
        authenticate_http3(connection, profile.password())
            .await
            .map(Arc::new)
    } else {
        let (send, recv) = connection.open_bi().await.map_err(|error| {
            RuntimeError::Io(std::io::Error::other(format!("hysteria2 open_bi: {error}")))
        })?;
        let mut stream = super::Hysteria2Stream::new(send, recv);
        profile
            .authenticate_connection(&connection, &mut stream)
            .await
            .map_err(RuntimeError::Core)?;
        Ok(Arc::new(Hysteria2AuthenticatedConnection::legacy(
            connection,
        )))
    }
}

pub async fn open_hysteria2_udp_packet_path_build(
    build: Hysteria2ManagedUdpPacketPathCarrierBuild,
) -> Result<
    (
        Arc<Hysteria2AuthenticatedConnection>,
        Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    ),
    RuntimeError,
> {
    let parts = build.into_protocol_build().into_connection_parts();
    let (server, port, profile, codec) = parts.into_shared_codec_parts();
    let connection = open_udp_profile_connection(&server, port, profile).await?;
    Ok((connection, codec))
}

pub async fn establish_hysteria2_udp_flow_connection(
    server: &str,
    port: u16,
    target: &Address,
    target_port: u16,
    payload: &[u8],
    resume: Hysteria2ManagedDatagramFlowResume,
) -> Result<crate::udp::Hysteria2UdpFlowConnection, RuntimeError> {
    let flow = managed_datagram_connector_flow_from_resume(&resume, server, port);
    let profile = flow.into_connection_parts().into_profile();
    let connection = open_udp_profile_connection(server, port, profile).await?;
    Ok(crate::udp::start_udp_flow_with_initial_packet(
        connection,
        target,
        target_port,
        payload,
        resume.into_protocol_resume(),
    ))
}
