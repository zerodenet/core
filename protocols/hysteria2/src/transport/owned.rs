//! The existing authentication/framing over sealed, originally owned Quinn I/O.
//! The caller retains the endpoint owner, driver receiver and native controller.
use std::sync::Arc;
use zero_platform_tokio::contract::{scope::TaskRegistrar, ChannelError, Impact, Reason, Stage};
use zero_transport::quic::owned::{
    h3::{PendingHttp3, RequestSender},
    OwnedBiStream, OwnedConnection, OwnedEndpointOwner,
};

mod pool;
mod profile;
pub use pool::{Pool, PoolFactory, PoolOptions, RoleFactory, RoleOptions};
mod udp;
pub use profile::PoolProfile;
pub use udp::{OwnedUdpChannel, OwnedUdpDispatcher};

/// Installed in the actual pool entry before authentication can suspend.
/// An interrupted exchange is terminal; cleanup must consume the same original
/// endpoint/driver/native receipts before that entry can be replaced.
pub struct OwnedAuthentication {
    connection: OwnedConnection,
    native_origin: std::sync::Mutex<
        Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin>,
    >,
    pending_http3: Option<PendingHttp3>,
    sender: Option<RequestSender>,
    legacy_stream: Option<OwnedBiStream>,
    negotiated: Option<crate::handshake::AuthResponse>,
    attempted: bool,
    dns_generation: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    udp: tokio::sync::Mutex<Option<OwnedUdpDispatcher>>,
    udp_cancel: zero_platform_tokio::contract::CancellationToken,
    tasks: Option<zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
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
            native_origin: Default::default(),
            pending_http3,
            sender: None,
            legacy_stream: None,
            negotiated: None,
            attempted: false,
            dns_generation: None,
            udp: Default::default(),
            udp_cancel: zero_platform_tokio::contract::CancellationToken::new(),
            tasks: None,
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
        self.udp_cancel = tasks.cancellation();
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
        self.tasks = Some(tasks.downgrade());
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
        self.open_stream_observed(destination, None, None).await
    }
    pub async fn open_stream_observed(
        &self,
        destination: &zero_core::request::Destination,
        observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<OwnedBiStream, ChannelError> {
        use tokio::io::AsyncWriteExt;
        use zero_platform_tokio::contract::context::ConnectFact;
        if !self.is_ready() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let (tx, rx) = self.connection.open_bi().await?;
        let mut stream = zero_transport::MeteredStream::new(OwnedBiStream::from_streams(
            self.connection.try_clone()?,
            tx,
            rx,
        ));
        let header =
            crate::shared::build_tcp_connect_header(&destination.address, destination.port)
                .map_err(|source| error(Stage::Handshake, Reason::Protocol).with_source(source))?;
        let result = async {
            stream
                .write_all(&header)
                .await
                .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?;
            if let Some(observer) = &observer {
                observer.record(session, ConnectFact::RequestSubmitted);
            }
            crate::Hysteria2Outbound
                .read_connect_response(&mut stream)
                .await
                .map_err(|source| error(Stage::Handshake, Reason::Protocol).with_source(source))
        }
        .await;
        if let Some(observer) = &observer {
            let traffic = stream.drain_traffic();
            // The existing HY2 Session path reports application Flow bytes
            // only. Actual authentication/request bytes remain Outer traffic.
            let _ = traffic;
            observer.protocol_traffic(session, 0, 0);
            match &result {
                Ok(()) => observer.record(session, ConnectFact::PeerConfirmed),
                Err(error) if error.reason == Reason::Protocol => {
                    observer.record(session, ConnectFact::PeerRejected)
                }
                _ => {}
            }
        }
        result?;
        Ok(stream.into_inner())
    }
    pub async fn open_udp(self: &Arc<Self>) -> Result<OwnedUdpChannel, ChannelError> {
        if !self.is_ready() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if !self
            .negotiated
            .is_some_and(|negotiated| negotiated.udp_enabled)
            || self.connection.max_datagram_size().is_none()
        {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        let tasks = self
            .tasks
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        loop {
            // Enter the original scope's existing callback relay outside the
            // install gate. No deferral guard crosses await. Publish the real
            // registration/witness and unlock before allowing external wakers.
            let callbacks = tasks
                .defer_callbacks()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            if let Ok(mut dispatcher) = self.udp.try_lock() {
                let opened = (|| {
                    if dispatcher.is_none() {
                        *dispatcher = Some(OwnedUdpDispatcher::prepare(
                            self.connection.try_clone()?,
                            tasks,
                            self.udp_cancel.child_token(),
                        ));
                    }
                    dispatcher.as_mut().unwrap().start()?;
                    dispatcher.as_ref().unwrap().open(self.clone())
                })();
                drop(dispatcher);
                drop(callbacks);
                return opened;
            }
            drop(callbacks);
            // Contending requests share this same prepared dispatcher. They
            // wait instead of allocating another one or reporting false EOF.
            drop(self.udp.lock().await);
        }
    }
    pub(super) async fn udp_witness(
        &self,
    ) -> Result<Option<zero_platform_tokio::contract::scope::TaskJoinWitness>, ChannelError> {
        match self.udp.lock().await.as_ref() {
            Some(dispatcher) => dispatcher.witness(),
            None => Ok(None),
        }
    }
    pub(super) fn request_udp_stop(&self) {
        self.udp_cancel.cancel();
    }
    pub fn stop(&self) {
        self.request_udp_stop();
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
