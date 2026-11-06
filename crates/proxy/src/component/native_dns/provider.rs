use super::*;
mod admission;
mod custody;
mod detour;
mod ech;
#[cfg(feature = "dns")]
pub(super) mod quic;
pub(super) mod retirement;
mod role;
use std::{net::SocketAddr, sync::Mutex};
use zero_dns::{native::NativeDnsProvider, OwnedDnsQueryTasks};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::TaskJoinWitness,
        BoxFuture, CancellationToken,
    },
    controlled_carrier::{
        loan::{DatagramLoan, StreamLoan},
        CarrierGrants, NativeCarrierOpener,
    },
};
pub(super) struct Provider {
    tasks: OwnedDnsQueryTasks,
    budget: Arc<super::Budget>,
    registrar: TaskRegistrar,
    cancel: CancellationToken,
    peers: Vec<(SocketAddr, bool, CarrierGrants)>,
    roots: Vec<(crate::inventory::service::ServiceRoot, CarrierGrants)>,
    ech: Vec<(zero_dns::EchDnsEndpoint, CarrierGrants)>,
    prepared_ech: Mutex<Vec<Arc<ech::Prepared>>>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    epoch: tokio::sync::Mutex<u64>,
    network: Arc<BackgroundNetwork>,
    instance: zero_core::component::ComponentInstanceId,
    users: Arc<Mutex<Vec<Arc<custody::NativeUse>>>>,
    pending: Arc<Mutex<Vec<Arc<custody::NativeUse>>>>,
    #[cfg(feature = "dns")]
    quic: Vec<(
        SocketAddr,
        zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolGrants,
    )>,
    #[cfg(feature = "dns")]
    quic_users: Mutex<Vec<Arc<quic::Query>>>,
}
impl Provider {
    pub(super) fn new(
        context: &BuildContext,
        options: &Options,
        owners: &[CarrierOwner],
        egress: zero_platform_tokio::EgressInterfaceControl,
        network: Arc<BackgroundNetwork>,
        #[cfg(feature="dns")]
        metadata: &[zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOwner],
    ) -> Self {
        Self {
            tasks: OwnedDnsQueryTasks::new_critical_with_budget(
                &context.tasks,
                NonZeroUsize::new(
                    options
                        .budget
                        .limits()
                        .queries
                        .get()
                        .max(options.budget.limits().blocking_jobs.get())
                        .max(options.budget.limits().h2_drivers.get()),
                )
                .unwrap(),
                options.budget.tasks(),
            ),
            budget: options.budget.clone(),
            registrar: context.tasks.clone(),
            cancel: context.cancel.clone(),
            peers: options
                .peers
                .iter()
                .zip(owners)
                .map(|((peer, packet), owner)| (*peer, *packet, owner.grants()))
                .collect(),
            roots: options
                .roots
                .iter()
                .cloned()
                .zip(
                    owners
                        .iter()
                        .skip(options.peers.len())
                        .map(|owner| owner.grants()),
                )
                .collect(),
            ech: options
                .ech
                .iter()
                .cloned()
                .zip(
                    owners
                        .iter()
                        .skip(options.peers.len() + options.roots.len())
                        .map(|owner| owner.grants()),
                )
                .collect(),
            prepared_ech: Mutex::default(),
            epoch: tokio::sync::Mutex::new(egress.generation()),
            egress,
            network,
            instance: context.instance(),
            users: Arc::default(),
            pending: Arc::default(),
            #[cfg(feature = "dns")]
            quic: options
                .quic
                .iter()
                .copied()
                .zip(metadata.iter().map(|owner| owner.grants()))
                .collect(),
            #[cfg(feature = "dns")]
            quic_users: Mutex::default(),
        }
    }
    pub(super) fn stop(&self) {
        self.cancel.cancel();
        #[cfg(feature = "dns")]
        for query in self
            .quic_users
            .lock()
            .expect("DNS QUIC users poisoned")
            .clone()
        {
            query.stop();
        }
    }
    pub(super) async fn close_queries(&self, end: tokio::time::Instant) -> std::io::Result<()> {
        self.tasks.wait_closed(end).await
    }
    async fn grant(
        &self,
        peer: SocketAddr,
        packet: bool,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<(
        zero_platform_tokio::controlled_carrier::CarrierGrant,
        zero_platform_tokio::controlled_carrier::CarrierUserView,
        NativeCarrierOpener,
        UpstreamBinding,
    )> {
        let epoch = self.admit(&caller, &cancel, end, 1).await?;
        let current = *epoch;
        let grants = &self
            .peers
            .iter()
            .find(|(address, kind, _)| *address == peer && *kind == packet)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "undeclared native DNS endpoint",
                )
            })?
            .2;
        let capacity = self.budget.reserve(1)?;
        let mut grant = grants.reserve().map_err(std::io::Error::other)?;
        let view = grant
            .claim_transferred_view()
            .map_err(std::io::Error::other)?;
        self.users
            .lock()
            .expect("DNS users poisoned")
            .push(Arc::new(custody::NativeUse {
                task: caller,
                cancel,
                original: grant.adoption(),
                pending_roles: Arc::from([]),
                logical: Default::default(),
                capacity: Mutex::new(Some(capacity)),
            }));
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: zero_core::component::NetworkGeneration(current),
        };
        let opener = NativeCarrierOpener::new(
            grant.instance(),
            binding.clone(),
            peer,
            Arc::new(self.egress.clone()),
            None,
        )
        .map_err(std::io::Error::other)?;
        Ok((grant, view, opener, binding))
    }
    fn retained(&self) -> usize {
        let retained = self
            .peers
            .iter()
            .map(|(_, _, grants)| {
                grants
                    .registration()
                    .observe()
                    .map_or(self.budget.limits().originals.get(), |state| {
                        state.retained()
                    })
            })
            .fold(0, usize::saturating_add);
        let retained = self
            .roots
            .iter()
            .map(|(_, grants)| {
                grants
                    .registration()
                    .observe()
                    .map_or(self.budget.limits().originals.get(), |state| {
                        state.retained()
                    })
            })
            .fold(retained, usize::saturating_add);
        let retained = self.ech.iter().fold(retained, |count, (_, grants)| {
            count.saturating_add(
                grants
                    .registration()
                    .observe()
                    .map_or(self.budget.limits().originals.get(), |state| {
                        state.retained()
                    }),
            )
        });
        #[cfg(feature = "dns")]
        let retained = retained.saturating_add(
            self.quic
                .iter()
                .map(|(_, grants)| {
                    grants
                        .retained()
                        .unwrap_or(self.budget.limits().originals.get())
                        .saturating_mul(2)
                })
                .fold(0, usize::saturating_add),
        );
        retained
    }
}

pub(super) fn destination(peer: SocketAddr) -> zero_core::request::Destination {
    zero_core::request::Destination::new(
        match peer.ip() {
            std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
pub(super) fn error(source: std::io::Error) -> BuildError {
    zero_platform_tokio::contract::ChannelError::new(
        zero_platform_tokio::contract::Stage::Close,
        zero_platform_tokio::contract::Reason::Io,
        zero_platform_tokio::contract::Impact::Component,
    )
    .with_source(source)
}
