//! Authenticated Hysteria carrier on the original host QUIC/H3 task ledger.
//! No native dial, runtime, untracked worker or Drop-based close claim.
use super::*;
use crate::quic::owned::{
    h3::{PendingHttp3, RequestSender},
    OwnedBiStream, OwnedConnection, OwnedEndpointOwner,
};
use tokio::io::AsyncWriteExt;
use zero_core::component::TaskId;
use zero_platform_tokio::{
    contract::{ChannelError, Impact, Reason, Stage},
    controlled_carrier::CarrierProtocolRetirement,
};
pub struct PendingClient {
    connection: OwnedConnection,
    http3: PendingHttp3,
    profile: Profile,
    retirement: CarrierProtocolRetirement,
}
pub struct OwnedClient {
    connection: OwnedConnection,
    _sender: RequestSender,
    task: zero_platform_tokio::contract::scope::TaskJoinWitness,
    retirement: CarrierProtocolRetirement,
}
impl OwnedClient {
    /// The original owner is already installed before H3 can first suspend.
    pub fn prepare(
        owner: &OwnedEndpointOwner,
        connection: OwnedConnection,
        profile: Profile,
        retirement: CarrierProtocolRetirement,
    ) -> Result<PendingClient, ChannelError> {
        let http3 = owner.prepare_http3(connection.try_clone()?)?;
        Ok(PendingClient {
            connection,
            http3,
            profile,
            retirement,
        })
    }
    pub(crate) fn original_task(&self) -> Option<TaskId> {
        self.task.task_id()
    }
    pub(crate) fn names_retirement(&self, original: &CarrierProtocolRetirement) -> bool {
        self.retirement.same_original(original)
    }
    pub(crate) fn names_registration(
        &self,
        actual: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> bool {
        actual.same_registration(&self.task)
    }
    pub(crate) fn names_endpoint(&self, owner: &OwnedEndpointOwner) -> bool {
        owner.owns_hysteria_connection(&self.connection)
    }
    pub fn usable(&self) -> bool {
        !self.retirement.retirement_requested()
            && !self.task.is_joined()
            && self.connection.close_reason().is_none()
            && self
                .retirement
                .original_receiver()
                .is_some_and(|receiver| receiver.protocol_driver().same_registration(&self.task))
    }
    pub async fn open(&self) -> Result<OwnedBiStream, ChannelError> {
        let mut stream = self.connection.try_clone()?.into_bi_stream().await?;
        // The upper protocol owns target framing after this carrier marker.
        stream.write_all(&[0x44, 0x01]).await.map_err(failure)?;
        Ok(stream)
    }
}
impl PendingClient {
    pub async fn ready(&mut self) -> Result<OwnedClient, ChannelError> {
        // This waiter stays in the real initializer Aux. The H3 driver itself
        // was registered above and remains in the endpoint's original ledger.
        let mut sender = self.http3.ready().await?;
        let request = http::Request::builder()
            .method("POST")
            .uri("https://hysteria/auth")
            .header("hysteria-auth", self.profile.auth.as_ref())
            .header("hysteria-cc-rx", self.profile.downlink.to_string())
            .header("hysteria-padding", padding())
            .body(())
            .map_err(failure)?;
        let authenticate = async {
            let mut request = sender.send_request(request).await.map_err(failure)?;
            request.finish().await.map_err(failure)?;
            let response = request.recv_response().await.map_err(failure)?;
            if response.status().as_u16() != 233 {
                return Err(failure(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Hysteria carrier authentication rejected",
                )));
            }
            let downlink = response
                .headers()
                .get("hysteria-cc-rx")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            self.profile.negotiate_owned(&self.connection, downlink);
            Ok::<_, ChannelError>(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), authenticate)
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Handshake, Reason::Timeout, Impact::Session)
                    .with_source(source)
            })??;
        Ok(OwnedClient {
            connection: self.connection.try_clone()?,
            _sender: sender,
            task: self.http3.original_registration().ok_or_else(|| {
                ChannelError::new(Stage::Handshake, Reason::Revoked, Impact::Session)
            })?,
            retirement: self.retirement.clone(),
        })
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Handshake, Reason::Protocol, Impact::Session).with_source(source)
}
