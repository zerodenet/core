//! Handshake dialing retains DNS/egress services, never a protocol registry.
use super::InboundConnectionContext;
use zero_platform_tokio::TcpRelayStream;
impl InboundConnectionContext {
    pub(crate) fn handshake_target_connector(&self) -> zero_transport::handshake_target::Connector {
        let upstream = self.runtime.upstream_services();
        zero_transport::handshake_target::Connector::new(move |endpoint| {
            let upstream = upstream.clone();
            Box::pin(async move {
                match endpoint {
                    zero_traits::FallbackEndpoint::Tcp { server, port } => upstream
                        .connect_upstream_owned(server, port)
                        .await
                        .map(TcpRelayStream::from)
                        .map_err(std::io::Error::other),
                    zero_traits::FallbackEndpoint::Unix { path } => {
                        #[cfg(unix)]
                        {
                            tokio::net::UnixStream::connect(path)
                                .await
                                .map(TcpRelayStream::new)
                        }
                        #[cfg(not(unix))]
                        {
                            let _ = path;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Unsupported,
                                "Unix handshake targets require Unix",
                            ))
                        }
                    }
                }
            })
        })
    }
}
