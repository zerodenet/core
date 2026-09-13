use super::*;

impl OwnedVlessOutboundTransportPlan {
    pub(super) async fn open_browser_carrier(&self) -> Result<TcpRelayStream, RuntimeError> {
        let access = self.browser_dialer.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "Browser Dialer runtime is unavailable",
            )
        })?;
        let dialer = access.dialer().await?;
        let secure = self.transport.tls.is_some();
        if self.browser_ws {
            let profile = self.transport.ws.as_ref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Browser Dialer WebSocket profile is missing",
                )
            })?;
            return Ok(TcpRelayStream::new(
                zero_transport::ws::connect_ws_with_browser(
                    &dialer,
                    profile,
                    self.server(),
                    self.port(),
                    secure,
                    None,
                )
                .await?,
            ));
        }
        if self.browser_xhttp {
            let profile = self.transport.split_http.as_ref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Browser Dialer XHTTP profile is missing",
                )
            })?;
            let server = self.server();
            let authority = if (secure && self.port() == 443) || (!secure && self.port() == 80) {
                server.to_owned()
            } else if server.contains(':') && !server.starts_with('[') {
                format!("[{server}]:{}", self.port())
            } else {
                format!("{server}:{}", self.port())
            };
            return Ok(TcpRelayStream::new(
                zero_transport::split_http::connect_split_http_with_browser(
                    &dialer,
                    profile,
                    if secure { "https" } else { "http" },
                    &authority,
                )
                .await?,
            ));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Browser Dialer has no supported carrier",
        )
        .into())
    }

    pub(super) fn reject_browser_relay(&self) -> Result<(), RuntimeError> {
        if self.uses_browser_dialer() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Browser Dialer cannot run over a relay prefix",
            )
            .into());
        }
        Ok(())
    }
}
