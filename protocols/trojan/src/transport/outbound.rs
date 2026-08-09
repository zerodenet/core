use std::future::Future;
use std::path::{Path, PathBuf};

use zero_platform_tokio::TokioSocket;
use zero_transport::RuntimeError;
use zero_transport::TcpRelayStream;

pub(super) type TrojanTcpStreamOpen = crate::outbound::TrojanErasedTcpStreamOpen;

#[derive(Debug, Clone)]
pub struct OwnedTrojanOutboundTlsPlan {
    server: String,
    port: u16,
    source_dir: Option<PathBuf>,
}

impl OwnedTrojanOutboundTlsPlan {
    pub(in crate::transport) fn from_parts(
        source_dir: Option<&Path>,
        server: &str,
        port: u16,
    ) -> Self {
        Self {
            server: server.to_owned(),
            port,
            source_dir: source_dir.map(PathBuf::from),
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
        open_trojan_tls_stream_with_profile(upstream, self.source_dir(), self.server(), tls_profile)
            .await
    }

    pub(super) async fn open_relay_with_profile(
        &self,
        stream: TcpRelayStream,
        tls_profile: crate::outbound::OwnedTrojanResolvedTlsProfile,
    ) -> Result<TcpRelayStream, RuntimeError> {
        open_trojan_tls_relay_stream_with_profile(
            stream,
            self.source_dir(),
            self.server(),
            tls_profile,
        )
        .await
    }
}

async fn open_trojan_tls_stream_with_profile(
    socket: TokioSocket,
    source_dir: Option<&Path>,
    server: &str,
    tls_profile: crate::outbound::OwnedTrojanResolvedTlsProfile,
) -> Result<TcpRelayStream, RuntimeError> {
    zero_transport::tls::connect_tls_upstream_with_profile(socket, &tls_profile, source_dir, server)
        .await
}

async fn open_trojan_tls_relay_stream_with_profile(
    stream: TcpRelayStream,
    source_dir: Option<&Path>,
    server: &str,
    tls_profile: crate::outbound::OwnedTrojanResolvedTlsProfile,
) -> Result<TcpRelayStream, RuntimeError> {
    zero_transport::tls::connect_tls_stream_with_profile(stream, &tls_profile, source_dir, server)
        .await
}
