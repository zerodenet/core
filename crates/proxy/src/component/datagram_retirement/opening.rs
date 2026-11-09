//! Install the actual association's control before its protocol can await I/O.
//! The existing Entry and retirement poller retain this control throughout open.
mod control;
mod ledger;
mod observer;
mod storage;
use super::*;
use crate::runtime::current_services::CurrentServiceSource;
use std::sync::Weak;
pub(crate) use storage::PendingOpenWeak;
use storage::{PendingOpenRef, ProtocolControlRef};
use zero_core::component::RoleInstanceId;
use zero_platform_tokio::{
    contract::{
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        BoxFuture,
    },
    controlled_carrier::CarrierAdoption,
};

pub(crate) struct DatagramOpening {
    pending: PendingOpenRef,
    association: zero_core::component::AssociationId,
    lease: Option<zero_platform_tokio::contract::lifecycle::BoxChannelControl>,
}
pub(crate) struct PendingOpen {
    plan: Weak<crate::component::catalog::ComponentPlan>,
    source: Weak<CurrentServiceSource>,
    roles: Vec<RoleInstanceId>,
    _roles_memory: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    limit: usize,
    stop: zero_platform_tokio::contract::CancellationToken,
    state: Mutex<OpeningState>,
    cleanup: AsyncMutex<()>,
    memory: Option<zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    caller: Option<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    protocols: Option<
        zero_traits::packet_metadata::PacketMetadataArc<
            zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren,
        >,
    >,
}
struct OpeningState {
    preparing: bool,
    retired: bool,
    pending: Vec<CarrierAdoption>,
    protocol: Option<ProtocolControlRef>,
    protocol_memory: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    report: CloseReport,
    observers: Vec<UpstreamTransport>,
    _observers_memory: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
}
struct Control(PendingOpenRef);
struct Observer {
    pending: PendingOpenWeak,
    anchor: std::sync::OnceLock<PendingCarrierObserver>,
}

impl DatagramReservation {
    pub(crate) fn begin_open(
        self,
        execution: &crate::protocol_registry::TcpExecutionServices,
        roles: Vec<RoleInstanceId>,
        stop: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<DatagramOpening, ChannelError> {
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| crate::component::instance::retained(source.clone()))?;
        let limit = plan.pending_carrier_capacity(&roles)?;
        let actual = crate::component::stream_bridge::actual_protocol_caller(execution)?;
        let (memory, caller) = match actual {
            Some((memory, caller)) => (Some(memory), Some(caller)),
            None => (None, None),
        };
        let protocols = memory
            .as_ref()
            .map(|memory| {
                let ceiling = plan.protocol_original_capacity(&roles)?;
                let credit = memory
                    .reserve(
                        zero_traits::packet_metadata::PacketMetadataArc::<
                            zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren,
                        >::layout()
                        .map_err(|source| admission_error(Reason::Capacity).with_source(source))?,
                    )
                    .map_err(|source| admission_error(Reason::Capacity).with_source(source))?;
                zero_traits::packet_metadata::PacketMetadataArc::from_prepaid(
                    zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren::new(
                        memory.clone(),
                        ceiling,
                    ),
                    credit,
                )
                .map_err(|(source, _, _)| admission_error(Reason::Capacity).with_source(source))
            })
            .transpose()?;
        let roles_memory = memory
            .as_ref()
            .map(|memory| {
                memory
                        .reserve(
                            zero_traits::packet_metadata::PacketMetadataLayout::array::<
                                RoleInstanceId,
                            >(roles.capacity())
                            .map_err(|source| {
                                admission_error(Reason::Capacity).with_source(source)
                            })?,
                        )
                        .map_err(|source| admission_error(Reason::Capacity).with_source(source))
            })
            .transpose()?;
        let observers_limit = roles
            .len()
            .checked_add(1)
            .ok_or_else(|| admission_error(Reason::Capacity))?;
        let observers_memory = memory
            .as_ref()
            .map(|memory| {
                memory
                    .reserve(
                        zero_traits::packet_metadata::PacketMetadataLayout::array::<
                            UpstreamTransport,
                        >(observers_limit)
                        .map_err(|source| admission_error(Reason::Capacity).with_source(source))?,
                    )
                    .map_err(|source| admission_error(Reason::Capacity).with_source(source))
            })
            .transpose()?;
        let protocol_memory = memory
            .as_ref()
            .map(|memory| {
                memory
                    .reserve(
                        zero_traits::packet_metadata::PacketMetadataArc::<
                            AsyncMutex<zero_platform_tokio::contract::lifecycle::BoxChannelControl>,
                        >::layout()
                        .map_err(|source| admission_error(Reason::Capacity).with_source(source))?,
                    )
                    .map_err(|source| admission_error(Reason::Capacity).with_source(source))
            })
            .transpose()?;
        let mut observers = Vec::new();
        if observers_memory.is_some() {
            observers
                .try_reserve_exact(observers_limit)
                .map_err(|source| admission_error(Reason::Capacity).with_source(source))?;
        }
        let pending = PendingOpenRef::new(
            PendingOpen {
                plan: Arc::downgrade(plan),
                source: execution.current_source.clone(),
                roles,
                _roles_memory: roles_memory,
                limit,
                stop,
                state: Mutex::new(OpeningState {
                    preparing: true,
                    retired: false,
                    pending: Vec::new(),
                    protocol: None,
                    protocol_memory,
                    report: CloseReport::default(),
                    observers,
                    _observers_memory: observers_memory,
                }),
                cleanup: Default::default(),
                memory: memory.clone(),
                caller,
                protocols,
            },
            memory.as_ref(),
        )?;
        // adopt is synchronous: cancellation after this point retires this
        // actual Entry instead of discarding an unacquired reservation.
        let association = self.association();
        let lease = self.adopt(Box::new(Control(pending.clone())).into());
        Ok(DatagramOpening {
            pending,
            association,
            lease: Some(lease),
        })
    }
}
impl DatagramOpening {
    pub(crate) fn association(&self) -> zero_core::component::AssociationId {
        self.association
    }
    pub(crate) fn pending(&self) -> PendingOpenWeak {
        self.pending.downgrade()
    }
    pub(crate) fn adopt(
        mut self,
        protocol: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
    ) -> zero_platform_tokio::contract::lifecycle::BoxChannelControl {
        let mut state = self.pending.state.lock().unwrap();
        assert!(state.preparing && state.protocol.is_none());
        state.protocol = Some(ProtocolControlRef::new(
            protocol,
            state.protocol_memory.take(),
        ));
        state.preparing = false;
        drop(state);
        self.lease.take().expect("same pre-I/O association lease")
    }
}
impl Drop for DatagramOpening {
    fn drop(&mut self) {
        self.pending.state.lock().unwrap().preparing = false;
        // The real Lease field requests retirement; its existing Entry and
        // waiter continue owning all originals after this opening future exits.
    }
}
