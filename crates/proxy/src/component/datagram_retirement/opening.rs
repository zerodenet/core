//! Install the actual association's control before its protocol can await I/O.
//! The existing Entry and retirement poller retain this control throughout open.
mod control;
mod ledger;
mod observer;
use super::*;
use crate::runtime::current_services::CurrentServiceSource;
use std::sync::Weak;
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
    pending: Arc<PendingOpen>,
    association: zero_core::component::AssociationId,
    lease: Option<zero_platform_tokio::contract::lifecycle::BoxChannelControl>,
}
pub(crate) struct PendingOpen {
    plan: Weak<crate::component::catalog::ComponentPlan>,
    source: Weak<CurrentServiceSource>,
    roles: Arc<[RoleInstanceId]>,
    limit: usize,
    stop: zero_platform_tokio::contract::CancellationToken,
    state: Mutex<OpeningState>,
    cleanup: AsyncMutex<()>,
}
struct OpeningState {
    preparing: bool,
    retired: bool,
    pending: Vec<CarrierAdoption>,
    protocol: Option<Arc<AsyncMutex<zero_platform_tokio::contract::lifecycle::BoxChannelControl>>>,
    report: CloseReport,
    observers: Vec<Arc<Observer>>,
}
struct Control(Arc<PendingOpen>);
struct Observer {
    pending: Weak<PendingOpen>,
    binding: UpstreamBinding,
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
        let pending = Arc::new(PendingOpen {
            plan: Arc::downgrade(plan),
            source: execution.current_source.clone(),
            roles: roles.into(),
            limit,
            stop,
            state: Mutex::new(OpeningState {
                preparing: true,
                retired: false,
                pending: Vec::new(),
                protocol: None,
                report: CloseReport::default(),
                observers: Vec::new(),
            }),
            cleanup: Default::default(),
        });
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
    pub(crate) fn pending(&self) -> Weak<PendingOpen> {
        Arc::downgrade(&self.pending)
    }
    pub(crate) fn adopt(
        mut self,
        protocol: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
    ) -> zero_platform_tokio::contract::lifecycle::BoxChannelControl {
        let mut state = self.pending.state.lock().unwrap();
        assert!(state.preparing && state.protocol.is_none());
        state.protocol = Some(Arc::new(AsyncMutex::new(protocol)));
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
