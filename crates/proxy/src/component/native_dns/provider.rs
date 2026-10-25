use super::*;
mod admission;
mod custody;
#[cfg(feature = "dns")]
pub(super) mod quic;
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
    registrar: TaskRegistrar,
    cancel: CancellationToken,
    peers: Vec<(SocketAddr, bool, CarrierGrants)>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    epoch: tokio::sync::Mutex<u64>,
    users: Mutex<Vec<Arc<custody::NativeUse>>>,
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
        #[cfg(feature="dns")]
        metadata: &[zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOwner],
    ) -> Self {
        Self {
            tasks: OwnedDnsQueryTasks::new_critical(
                &context.tasks,
                NonZeroUsize::new(LIMIT).unwrap(),
            ),
            registrar: context.tasks.clone(),
            cancel: context.cancel.clone(),
            peers: options
                .peers
                .iter()
                .zip(owners)
                .map(|((peer, packet), owner)| (*peer, *packet, owner.grants()))
                .collect(),
            epoch: tokio::sync::Mutex::new(egress.generation()),
            egress,
            users: Mutex::new(Vec::new()),
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
                    .map_or(LIMIT, |state| state.retained())
            })
            .fold(0, usize::saturating_add);
        #[cfg(feature = "dns")]
        let retained = retained.saturating_add(
            self.quic
                .iter()
                .map(|(_, grants)| grants.retained().unwrap_or(LIMIT).saturating_mul(2))
                .fold(0, usize::saturating_add),
        );
        retained
    }
}
impl NativeDnsProvider for Provider {
    #[cfg(feature = "dns")]
    fn quic(
        &self,
        peer: SocketAddr,
        server_name: String,
        client: quinn::ClientConfig,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<zero_dns::native::NativeDnsQuicLoan>> {
        Box::pin(self.open_quic(peer, server_name, client, caller, cancel, end))
    }
    fn tasks(&self) -> OwnedDnsQueryTasks {
        self.tasks.clone()
    }
    fn stream(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<StreamLoan>> {
        Box::pin(async move {
            let (grant, view, opener, binding) =
                self.grant(peer, false, caller, cancel.clone(), end).await?;
            let carrier = opener
                .prepare_controlled_stream(
                    &binding,
                    destination(peer),
                    CarrierContext {
                        session: None,
                        deadline: Some(end),
                        cancel,
                    },
                    grant,
                )
                .map_err(std::io::Error::other)?
                .open(end)
                .await
                .map_err(std::io::Error::other)?;
            carrier
                .into_parts()
                .0
                .into_stream_loan(view)
                .map_err(std::io::Error::other)
        })
    }
    fn datagram(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<DatagramLoan>> {
        Box::pin(async move {
            let (grant, view, opener, binding) =
                self.grant(peer, true, caller, cancel.clone(), end).await?;
            let carrier = opener
                .prepare_controlled_datagram(
                    &binding,
                    destination(peer),
                    CarrierContext {
                        session: None,
                        deadline: Some(end),
                        cancel,
                    },
                    grant,
                )
                .map_err(std::io::Error::other)?;
            carrier
                .into_datagram_loan(view, zero_core::component::AssociationId(0))
                .map_err(std::io::Error::other)
        })
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
