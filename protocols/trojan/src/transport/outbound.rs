use std::future::Future;
use std::path::{Path, PathBuf};

use zero_platform_tokio::TokioSocket;
use zero_traits::{GrpcTransportProfile, WebSocketTransportProfile};
use zero_transport::outbound_stack::{
    connect_relay_transport_stack, connect_socket_transport_stack, StreamTransportStack,
};
use zero_transport::profile::{
    OwnedClientTlsProfile, OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile,
    OwnedWebSocketProfile,
};
use zero_transport::RuntimeError;
use zero_transport::TcpRelayStream;

pub(super) type TrojanTcpStreamOpen = crate::outbound::TrojanErasedTcpStreamOpen;

#[derive(Debug, Clone)]
pub struct OwnedTrojanOutboundTlsPlan {
    server: String,
    port: u16,
    source_dir: Option<PathBuf>,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
}

impl OwnedTrojanOutboundTlsPlan {
    pub(in crate::transport) fn from_parts<TWs, TGrpc>(
        source_dir: Option<&Path>,
        server: &str,
        port: u16,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
    ) -> Self
    where
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self {
            server: server.to_owned(),
            port,
            source_dir: source_dir.map(PathBuf::from),
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
        }
    }

    fn server(&self) -> &str {
        &self.server
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn source_dir(&self) -> Option<&Path> {
        self.source_dir.as_deref()
    }

    pub(super) async fn open_direct_with_profile<OpenSocket, OpenSocketFut, E>(
        &self,
        open_socket: OpenSocket,
        tls_profile: crate::outbound::OwnedTrojanResolvedTlsProfile,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenSocket: FnOnce(&str, u16) -> OpenSocketFut,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>>,
        E: Into<RuntimeError>,
    {
        let upstream = open_socket(self.server(), self.port())
            .await
            .map_err(Into::into)?;
        let tls_profile = self.tls_profile_for_transport(tls_profile);
        connect_socket_transport_stack(
            upstream,
            StreamTransportStack {
                tls: Some(&tls_profile),
                ws: self.ws.as_ref(),
                grpc: self.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
                http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                source_dir: self.source_dir(),
            },
            self.server(),
            self.port(),
            "trojan: ws and grpc are mutually exclusive",
        )
        .await
    }

    pub(super) async fn open_relay_with_profile(
        &self,
        stream: TcpRelayStream,
        tls_profile: crate::outbound::OwnedTrojanResolvedTlsProfile,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let tls_profile = self.tls_profile_for_transport(tls_profile);
        connect_relay_transport_stack(
            stream,
            StreamTransportStack {
                tls: Some(&tls_profile),
                ws: self.ws.as_ref(),
                grpc: self.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
                http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                source_dir: self.source_dir(),
            },
            self.server(),
            self.port(),
            "trojan: ws and grpc are mutually exclusive",
        )
        .await
    }

    fn tls_profile_for_transport(
        &self,
        tls_profile: crate::outbound::OwnedTrojanResolvedTlsProfile,
    ) -> OwnedClientTlsProfile {
        let tls_profile = if self.grpc.is_some() {
            tls_profile.with_alpn(["h2".to_owned()])
        } else {
            tls_profile
        };
        OwnedClientTlsProfile::from_profile(&tls_profile)
    }
}
