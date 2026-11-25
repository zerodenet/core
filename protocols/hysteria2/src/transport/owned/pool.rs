//! Hysteria2 authenticated QUIC connection pool and Stream/Datagram roles,
//! backed by [`zero_transport::runner::quic_pool`].

mod datagram;

use super::*;
use std::{path::PathBuf, sync::Arc, time::Duration};
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
        context::{ConnectObserver, OpenContext},
        datagram::{DatagramLimits, DatagramRequest, DatagramSession},
        dns::DnsGeneration,
        lifecycle::BuildError,
        network::{UpstreamBinding, UpstreamTransport},
        scope::{TaskJoinWitness, TaskRegistrar},
        BoxFuture, ChannelError, DeliveryState,
    },
    controlled_datagram::metadata_pool::MetadataPoolOrigin,
};
use zero_transport::runner::{
    QuicPool, QuicPoolDatagramProtocol, QuicPoolFactory, QuicPoolGeneration, QuicPoolLimits,
    QuicPoolOptions, QuicPoolProtocol, QuicPoolRoleFactory, QuicPoolRoleOptions,
    QuicPoolRoleProtocol,
};

pub struct Hysteria2QuicPool;

pub type Pool = QuicPool<Hysteria2QuicPool>;
pub type PoolOptions = QuicPoolOptions<Hysteria2QuicPool>;
pub type RoleOptions = QuicPoolRoleOptions<Hysteria2QuicPool>;
pub(super) type Generation = QuicPoolGeneration<Hysteria2QuicPool>;

impl QuicPoolProtocol for Hysteria2QuicPool {
    type Profile = PoolProfile;
    type Pending = OwnedAuthentication;
    type Session = OwnedAuthentication;

    const ENDPOINT_OWNER_NAME: &'static str = "hysteria2.authenticated_pool";
    const POOL_DEBUG_NAME: &'static str = "Hysteria2AuthenticatedPool";
    const DISPLAY_NAME: &'static str = "Hysteria2";

    fn profile_endpoint(profile: &PoolProfile) -> &Destination {
        profile.endpoint()
    }

    fn profile_binding(profile: &PoolProfile) -> &UpstreamBinding {
        profile.binding()
    }

    fn profile_identity(profile: &PoolProfile) -> &Arc<[u8]> {
        &profile.identity
    }

    fn profile_server_name(profile: &PoolProfile) -> &str {
        profile
            .leaf
            .server_name
            .as_deref()
            .unwrap_or(&profile.leaf.server)
    }

    fn profile_port(profile: &PoolProfile) -> u16 {
        profile.leaf.port
    }

    fn profile_ca_cert_path(profile: &PoolProfile) -> Option<PathBuf> {
        profile.ca_cert_path()
    }

    fn profile_udp_hop(profile: &PoolProfile) -> Option<&zero_transport::datagram_hop::Profile> {
        profile.leaf.node.udp_hop.as_ref()
    }

    fn profile_masks(profile: &PoolProfile) -> Vec<zero_transport::finalmask::udp::Mask> {
        profile.masks()
    }

    fn profile_max_idle_timeout(profile: &PoolProfile) -> Duration {
        Duration::from_secs(profile.leaf.settings.quic.max_idle_timeout_secs)
    }

    fn build_client_config(
        profile: &PoolProfile,
        ca_pem: Option<&[u8]>,
    ) -> Result<quinn::ClientConfig, ChannelError> {
        profile.client_config(ca_pem)
    }

    fn prepare_session(
        connection: OwnedConnection,
        owner: &OwnedEndpointOwner,
        _profile: &PoolProfile,
        limits: &QuicPoolLimits,
    ) -> Result<OwnedAuthentication, ChannelError> {
        let mut auth = OwnedAuthentication::prepare(connection, owner)?;
        auth.udp_sessions = limits.udp_sessions;
        auth.udp_pending_bytes = limits.udp_pending_bytes;
        Ok(auth)
    }

    fn authenticate<'a>(
        pending: &'a mut OwnedAuthentication,
        profile: &'a PoolProfile,
        tasks: &'a TaskRegistrar,
    ) -> BoxFuture<'a, Result<(), ChannelError>> {
        Box::pin(pending.authenticate(&profile.leaf.password, profile.leaf.settings, tasks))
    }

    fn finalize_session(
        mut pending: OwnedAuthentication,
        origin: Option<MetadataPoolOrigin>,
        dns_generation: Option<DnsGeneration>,
    ) -> BoxFuture<'static, Result<(Arc<OwnedAuthentication>, Option<TaskJoinWitness>), ChannelError>>
    {
        Box::pin(async move {
            *pending.native_origin.lock().unwrap() = origin;
            pending.dns_generation = dns_generation;
            let auth = Arc::new(pending);
            let witness = auth.udp_witness().await?;
            Ok((auth, witness))
        })
    }

    fn is_session_ready(session: &OwnedAuthentication) -> bool {
        session.is_ready()
    }

    fn request_pending_stop(pending: &OwnedAuthentication) {
        pending.request_udp_stop();
    }

    fn stop_pending(pending: &OwnedAuthentication) {
        pending.stop();
    }

    fn request_session_stop(session: &OwnedAuthentication) {
        session.request_udp_stop();
    }

    fn stop_session(
        session: &Arc<OwnedAuthentication>,
    ) -> BoxFuture<'static, Result<Option<TaskJoinWitness>, ChannelError>> {
        let session = session.clone();
        Box::pin(async move {
            session.stop();
            session.udp_witness().await
        })
    }
}

impl QuicPoolRoleProtocol for Hysteria2QuicPool {
    fn session_native_origin(session: &OwnedAuthentication) -> Option<MetadataPoolOrigin> {
        session.native_origin.lock().unwrap().clone()
    }

    fn open_stream<'a>(
        session: &'a Arc<OwnedAuthentication>,
        destination: &'a Destination,
        observer: Option<Arc<dyn ConnectObserver>>,
        session_id: Option<zero_core::component::SessionId>,
    ) -> BoxFuture<'a, Result<OwnedBiStream, ChannelError>> {
        Box::pin(session.open_stream_observed(destination, observer, session_id))
    }
}

pub struct Hysteria2QuicDatagram;

impl QuicPoolDatagramProtocol<Hysteria2QuicPool> for Hysteria2QuicDatagram {
    type Tx = datagram::Tx;
    type Rx = datagram::Rx;
    type Control = datagram::Control;

    const ENABLED: bool = true;

    fn open_datagram(
        session: Arc<OwnedAuthentication>,
        request: DatagramRequest,
        context: OpenContext,
        generation: Arc<Generation>,
        profile: usize,
    ) -> BoxFuture<'static, Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError>>
    {
        Box::pin(async move {
            let channel = session.open_udp().await?;
            datagram::session(channel, request, context, generation, profile)
        })
    }
}

pub struct PoolFactory;

impl ComponentFactory for PoolFactory {
    type Options = PoolOptions;

    fn describe(&self, options: &PoolOptions) -> ComponentSpec {
        QuicPoolFactory::<Hysteria2QuicPool>::new().describe(options)
    }

    fn build(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        QuicPoolFactory::<Hysteria2QuicPool>::new().build(options, context)
    }
}

impl PoolFactory {
    pub fn build_with_upstreams(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
        upstreams: Vec<UpstreamTransport>,
    ) -> Result<ComponentInstance, BuildError> {
        QuicPoolFactory::<Hysteria2QuicPool>::new()
            .build_with_upstreams(options, context, upstreams)
    }
}

pub struct RoleFactory;

impl ComponentFactory for RoleFactory {
    type Options = RoleOptions;

    fn describe(&self, options: &RoleOptions) -> ComponentSpec {
        QuicPoolRoleFactory::<Hysteria2QuicPool, Hysteria2QuicDatagram>::new().describe(options)
    }

    fn build(
        &self,
        options: Arc<RoleOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        QuicPoolRoleFactory::<Hysteria2QuicPool, Hysteria2QuicDatagram>::new()
            .build(options, context)
    }
}
