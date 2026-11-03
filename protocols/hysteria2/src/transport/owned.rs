//! The existing authentication/framing over sealed, originally owned Quinn I/O.
//! The caller retains the endpoint owner, driver receiver and native controller.
use std::sync::Arc;
use zero_platform_tokio::contract::{scope::TaskRegistrar, ChannelError, Impact, Reason, Stage};
use zero_transport::quic::owned::{
    h3::{PendingHttp3, RequestSender},
    OwnedBiStream, OwnedConnection, OwnedEndpointOwner,
};

mod udp;
pub use udp::{OwnedUdpChannel, OwnedUdpDispatcher};

/// Installed in the actual pool entry before authentication can suspend.
/// An interrupted exchange is terminal; cleanup must consume the same original
/// endpoint/driver/native receipts before that entry can be replaced.
pub struct OwnedAuthentication {
    connection: OwnedConnection,
    pending_http3: Option<PendingHttp3>,
    sender: Option<RequestSender>,
    legacy_stream: Option<OwnedBiStream>,
    negotiated: Option<crate::handshake::AuthResponse>,
    attempted: bool,
    udp: Option<OwnedUdpDispatcher>,
}
impl OwnedAuthentication {
    pub fn prepare(
        connection: OwnedConnection,
        owner: &OwnedEndpointOwner,
    ) -> Result<Self, ChannelError> {
        let (_, alpn) = connection.handshake_metadata();
        let pending_http3 = if alpn.as_deref() == Some("h3") {
            Some(owner.prepare_http3(connection.try_clone()?)?)
        } else {
            None
        };
        Ok(Self {
            connection,
            pending_http3,
            sender: None,
            legacy_stream: None,
            negotiated: None,
            attempted: false,
            udp: None,
        })
    }
    pub async fn authenticate(
        &mut self,
        password: &str,
        settings: crate::settings::Settings,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        if self.attempted {
            return Err(error(Stage::Handshake, Reason::Revoked));
        }
        self.attempted = true;
        let negotiated = if let Some(pending) = &mut self.pending_http3 {
            self.sender = Some(pending.ready().await?);
            super::authenticate_http3_request(self.sender.as_mut().unwrap(), password, settings)
                .await
                .map_err(|source| error(Stage::Handshake, Reason::Protocol).with_source(source))?
        } else {
            let (tx, rx) = self.connection.open_bi().await?;
            self.legacy_stream = Some(OwnedBiStream::from_streams(
                self.connection.try_clone()?,
                tx,
                rx,
            ));
            let mut salt = [0; 32];
            self.connection
                .export_keying_material(&mut salt, b"hysteria2 auth", &[])?;
            crate::Hysteria2Outbound
                .authenticate_with_salt(self.legacy_stream.as_mut().unwrap(), password, &salt)
                .await
                .map_err(|source| error(Stage::Handshake, Reason::Protocol).with_source(source))?;
            self.legacy_stream = None;
            crate::handshake::AuthResponse::from_headers(Some("true"), Some("0"))
        };
        self.connection
            .negotiate_rate(settings.client_send_rate(negotiated.receive_bandwidth));
        if negotiated.udp_enabled && self.connection.max_datagram_size().is_some() {
            self.udp = Some(OwnedUdpDispatcher::new(
                self.connection.try_clone()?,
                tasks,
            )?);
        }
        self.negotiated = Some(negotiated);
        Ok(())
    }
    pub fn is_ready(&self) -> bool {
        self.negotiated.is_some() && self.connection.close_reason().is_none()
    }
    pub fn negotiated(&self) -> Option<crate::handshake::AuthResponse> {
        self.negotiated
    }
    pub fn connection(&self) -> &OwnedConnection {
        &self.connection
    }
    pub async fn open_stream(
        &self,
        destination: &zero_core::request::Destination,
    ) -> Result<OwnedBiStream, ChannelError> {
        use tokio::io::AsyncWriteExt;
        if !self.is_ready() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let (tx, rx) = self.connection.open_bi().await?;
        let mut stream = OwnedBiStream::from_streams(self.connection.try_clone()?, tx, rx);
        let header =
            crate::shared::build_tcp_connect_header(&destination.address, destination.port)
                .map_err(|source| error(Stage::Handshake, Reason::Protocol).with_source(source))?;
        stream
            .write_all(&header)
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?;
        crate::Hysteria2Outbound
            .read_connect_response(&mut stream)
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Protocol).with_source(source))?;
        Ok(stream)
    }
    pub fn open_udp(self: &Arc<Self>) -> Result<OwnedUdpChannel, ChannelError> {
        if !self.is_ready() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let dispatch = self
            .udp
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        dispatch.open(self.clone())
    }
    pub fn udp_dispatcher(&self) -> Option<&OwnedUdpDispatcher> {
        self.udp.as_ref()
    }
    pub fn stop(&self) {
        if let Some(udp) = &self.udp {
            udp.stop();
        }
        self.connection.close(0x100u32.into(), b"owner retirement");
    }
}
impl Drop for OwnedAuthentication {
    fn drop(&mut self) {
        self.stop();
    }
}
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
