use std::io;
use std::sync::Arc;

use zero_core::Session;
use zero_transport::RuntimeError;

mod auth;
mod connection;
mod inbound;
mod managed_udp;
mod model;
mod options;
mod projection;
mod stream;

pub use connection::open_quic_connection;
pub use inbound::Hysteria2InboundBindPlan;
pub use managed_udp::{
    establish_hysteria2_udp_flow_connection, managed_datagram_connector_flow_from_resume,
    open_hysteria2_udp_packet_path_build,
};
pub use model::{
    Hysteria2AuthenticatedInboundProfile, Hysteria2AuthenticatedQuicConnection,
    Hysteria2InboundTcpResponseProtocol, Hysteria2ManagedDatagramFlowResume,
    Hysteria2ManagedUdpFlowConfig, Hysteria2ManagedUdpFlowPlan,
    Hysteria2ManagedUdpPacketPathCarrierBuild, Hysteria2ManagedUdpPacketPathCarrierDescriptor,
    Hysteria2ManagedUdpPacketPathPlan, Hysteria2TransportLeaf,
};
pub use model::{Hysteria2QuicProfile, QuicConnectionOptions};
pub use options::{
    Hysteria2InboundBindOptionsRef, Hysteria2InboundOptionsRef, Hysteria2InboundUserRef,
    Hysteria2OutboundOptionsRef,
};
pub use stream::Hysteria2Stream;

pub(crate) fn outbound_quic_alpn_protocols() -> Vec<Vec<u8>> {
    vec![b"h3".to_vec(), b"hysteria2".to_vec()]
}

pub(crate) fn inbound_quic_alpn_protocols() -> Vec<Vec<u8>> {
    vec![b"h3".to_vec(), b"hysteria2".to_vec()]
}

async fn open_authenticated_hysteria2_quic_connection(
    server: &str,
    port: u16,
    profile: &crate::Hysteria2OutboundProfile,
) -> Result<Arc<Hysteria2AuthenticatedConnection>, RuntimeError> {
    let quic_profile = Hysteria2QuicProfile::from_parts(profile.client_fingerprint());
    let conn = open_quic_connection(QuicConnectionOptions {
        server,
        port,
        alpn: outbound_quic_alpn_protocols(),
        quic_profile,
        datagram_receive_buffer_size: Some(65536),
    })
    .await?;

    if connection::negotiated_alpn(&conn).as_deref() == Some(b"h3") {
        auth::authenticate_http3(conn, profile.password())
            .await
            .map(Arc::new)
    } else {
        let (send, recv) = conn.open_bi().await.map_err(|error| {
            RuntimeError::Io(io::Error::other(format!("hysteria2 open_bi: {error}")))
        })?;
        let mut stream = Hysteria2Stream::new(send, recv);
        profile
            .authenticate_connection(&conn, &mut stream)
            .await
            .map_err(RuntimeError::Core)?;
        Ok(Arc::new(Hysteria2AuthenticatedConnection::legacy(conn)))
    }
}

pub async fn connect_hysteria2_tcp_outbound(
    session: &Session,
    server: &str,
    port: u16,
    password: &str,
    client_fingerprint: Option<&str>,
) -> Result<zero_transport::TcpRelayStream, RuntimeError> {
    let profile = crate::outbound_profile_from_config_password(password, client_fingerprint);
    let conn = open_authenticated_hysteria2_quic_connection(server, port, &profile).await?;
    let (send, recv) = conn.connection().open_bi().await.map_err(|error| {
        RuntimeError::Io(io::Error::other(format!("hysteria2 open_bi: {error}")))
    })?;
    let mut stream = Hysteria2Stream::with_connection_guard(send, recv, conn);
    crate::Hysteria2Outbound
        .establish_tcp_connect(&mut stream, session)
        .await
        .map_err(RuntimeError::Core)?;
    Ok(zero_transport::TcpRelayStream::new(stream))
}
pub use auth::Hysteria2AuthenticatedConnection;
