//! Pure protocol customization trait and cold limit defaults for [`super::QuicPool`].

use crate::quic::owned::{OwnedConnection, OwnedEndpointOwner};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        dns::DnsGeneration,
        network::UpstreamBinding,
        scope::{TaskJoinWitness, TaskRegistrar},
        BoxFuture, ChannelError,
    },
    controlled_datagram::{metadata::MetadataLimits, metadata_pool::MetadataPoolOrigin},
};

pub(super) fn n(v: usize) -> NonZeroUsize {
    NonZeroUsize::new(v).unwrap()
}

pub fn default_metadata_limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: n(1),
        probe_sockets: n(2),
        probe_attempts_per_initialization: n(8),
        concurrent_send_calls: n(2),
        write_pollers: n(2),
    }
}

/// Cold limits configured on [`super::QuicPoolOptions`].
#[derive(Clone, Copy, Debug)]
pub struct QuicPoolLimits {
    pub entry_limit: NonZeroUsize,
    pub udp_sessions: NonZeroUsize,
    pub udp_pending_bytes: NonZeroUsize,
    pub metadata_limits: MetadataLimits,
    pub endpoint_users: NonZeroUsize,
}

/// Output of [`QuicPoolProtocol::finalize_session`].
pub type FinalizedPoolSession<S> = Result<(Arc<S>, Option<TaskJoinWitness>), ChannelError>;

/// Pure protocol customization surface for [`super::QuicPool`] and [`super::QuicPoolFactory`].
pub trait QuicPoolProtocol: Send + Sync + 'static {
    type Profile: Clone + Send + Sync + 'static;
    type Pending: Send + 'static;
    type Session: Send + Sync + 'static;

    const ENDPOINT_OWNER_NAME: &'static str;
    const POOL_DEBUG_NAME: &'static str;
    const DISPLAY_NAME: &'static str;

    fn profile_endpoint(profile: &Self::Profile) -> &Destination;
    fn profile_binding(profile: &Self::Profile) -> &UpstreamBinding;
    fn profile_identity(profile: &Self::Profile) -> &Arc<[u8]>;
    fn profile_server_name(profile: &Self::Profile) -> &str;
    fn profile_port(profile: &Self::Profile) -> u16;

    fn profile_ca_cert_path(_profile: &Self::Profile) -> Option<PathBuf> {
        None
    }

    fn profile_udp_hop(_profile: &Self::Profile) -> Option<&crate::datagram_hop::Profile> {
        None
    }

    fn profile_masks(_profile: &Self::Profile) -> Vec<crate::finalmask::udp::Mask> {
        Vec::new()
    }

    fn profile_max_idle_timeout(profile: &Self::Profile) -> Duration;

    fn build_client_config(
        profile: &Self::Profile,
        ca_pem: Option<&[u8]>,
    ) -> Result<quinn::ClientConfig, ChannelError>;

    fn prepare_session(
        connection: OwnedConnection,
        owner: &OwnedEndpointOwner,
        profile: &Self::Profile,
        limits: &QuicPoolLimits,
    ) -> Result<Self::Pending, ChannelError>;

    fn authenticate<'a>(
        pending: &'a mut Self::Pending,
        profile: &'a Self::Profile,
        tasks: &'a TaskRegistrar,
    ) -> BoxFuture<'a, Result<(), ChannelError>>;

    fn finalize_session(
        pending: Self::Pending,
        origin: Option<MetadataPoolOrigin>,
        dns_generation: Option<DnsGeneration>,
    ) -> BoxFuture<'static, FinalizedPoolSession<Self::Session>>;

    fn is_session_ready(session: &Self::Session) -> bool;

    fn is_session_idle(session: &Arc<Self::Session>) -> bool {
        Arc::strong_count(session) == 1
    }

    fn request_pending_stop(pending: &Self::Pending);
    fn stop_pending(pending: &Self::Pending);
    fn request_session_stop(session: &Self::Session);

    fn stop_session(
        session: &Arc<Self::Session>,
    ) -> BoxFuture<'static, Result<Option<TaskJoinWitness>, ChannelError>>;
}
