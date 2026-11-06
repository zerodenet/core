//! Native UDP peers reuse the original accepted task/resource ledger, not a TCP entry.
use super::*;
use crate::contract::{
    inbound::udp::{DatagramPeerAdmission, PeerControl},
    lifecycle::{ChannelControl, CloseReport, ResourceLifecycle, Unfinished},
    scope::TaskJoined,
};
use std::sync::atomic::AtomicUsize;

pub(super) struct Ledger {
    pub(super) origin: AcceptedOrigin,
    reports: Mutex<resources::ChildReports>,
    custody: AtomicUsize,
    pub(super) failure: Mutex<Option<Arc<ChannelError>>>,
    pub(super) user_failure: Mutex<Option<TaskJoined>>,
}
/// The native source issues this owner from a real peer admission and the
/// exact original host registration. It owns no task set or native socket.
#[derive(Clone)]
pub struct AcceptedPeerScope {
    pub(super) users: Arc<Users>,
    task: Arc<OnceLock<(TaskId, TaskJoinWitness)>>,
    admission: Option<DatagramPeerAdmission>,
    control: Option<PeerControl>,
}
impl DatagramPeerAdmission {
    pub fn bind_scope(
        &self,
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
    ) -> Result<AcceptedPeerScope, ChannelError> {
        if !witness
            .scope_identity()
            .same_scope(&registrar.scope_identity())
            || !registrar.registration_open()
            || witness.task_id() != Some(task)
            || witness.is_joined()
            || cancel.is_cancelled()
            || self.retirement().is_released()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "peer differs from original Scope registration",
            ));
        }
        if !self.issue_scope() {
            return Err(failure(
                Stage::Prepare,
                Reason::Capacity,
                "peer already has its original task scope",
            ));
        }
        let authority = Authority {
            scope: registrar.scope_identity(),
            registrar: registrar.clone(),
            max_tasks: self.limits().max_tasks_per_peer,
            max_resources: self.limits().max_resources_per_peer,
        };
        let mut users = Users::new(authority, Weak::new(), Weak::new(), cancel);
        users.peer = Some(Arc::new(Ledger {
            origin: AcceptedOrigin::Datagram,
            reports: Mutex::default(),
            custody: AtomicUsize::new(0),
            failure: Mutex::new(None),
            user_failure: Mutex::new(None),
        }));
        if let Some(local) = self.facts().local_endpoint {
            let _ = users.local.set(local);
        }
        users.state().primary = true;
        users.state().tasks.insert(
            task,
            Node {
                witness: witness.clone(),
                primary: true,
                process: None,
                process_helper: false,
            },
        );
        let origin = Arc::new(OnceLock::new());
        let _ = origin.set((task, witness));
        Ok(AcceptedPeerScope {
            users: Arc::new(users),
            task: origin,
            admission: Some(self.clone()),
            control: Some(self.control()),
        })
    }
}
impl AcceptedPeerScope {
    pub fn origin(&self) -> AcceptedOrigin {
        self.users
            .peer
            .as_ref()
            .expect("original resource ledger")
            .origin
    }
    /// A host-owned logical association uses the same retained resource ledger
    /// and exact Scope witness, without fabricating a native TCP/UDP admission.
    pub fn for_association(
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
        max_tasks: NonZeroUsize,
        max_resources: usize,
        local: std::net::SocketAddr,
    ) -> Result<Self, ChannelError> {
        if !witness
            .scope_identity()
            .same_scope(&registrar.scope_identity())
            || !registrar.registration_open()
            || witness.task_id() != Some(task)
            || witness.is_joined()
            || cancel.is_cancelled()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "association differs from actual host registration",
            ));
        }
        let authority = Authority {
            scope: registrar.scope_identity(),
            registrar: registrar.clone(),
            max_tasks,
            max_resources,
        };
        let mut users = Users::new(authority, Weak::new(), Weak::new(), cancel);
        users.peer = Some(Arc::new(Ledger {
            origin: AcceptedOrigin::Association,
            reports: Mutex::default(),
            custody: AtomicUsize::new(0),
            failure: Mutex::new(None),
            user_failure: Mutex::new(None),
        }));
        let _ = users.local.set(local);
        users.state().primary = true;
        users.state().tasks.insert(
            task,
            Node {
                witness: witness.clone(),
                primary: true,
                process: None,
                process_helper: false,
            },
        );
        let origin = Arc::new(OnceLock::new());
        let _ = origin.set((task, witness));
        Ok(Self {
            users: Arc::new(users),
            task: origin,
            admission: None,
            control: None,
        })
    }
    pub fn for_carrier(
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
        max_tasks: NonZeroUsize,
        max_resources: usize,
        local: std::net::SocketAddr,
    ) -> Result<Self, ChannelError> {
        let mut graph = Self::for_association(
            registrar,
            task,
            witness,
            cancel,
            max_tasks,
            max_resources,
            local,
        )?;
        let ledger = Arc::get_mut(&mut graph.users)
            .expect("unpublished carrier graph")
            .peer
            .as_mut()
            .expect("actual carrier ledger");
        Arc::get_mut(ledger)
            .expect("unpublished carrier ledger")
            .origin = AcceptedOrigin::Carrier;
        Ok(graph)
    }
    pub fn tasks(&self) -> AcceptedTasks {
        AcceptedTasks {
            users: Arc::downgrade(&self.users),
            task: self.task.clone(),
        }
    }
    pub fn observe_joined(&self, task: TaskId) -> Option<TaskJoined> {
        self.users.consume_joined(task)
    }
    pub async fn retire_logical_resources(&self, deadline: Instant) -> Result<(), ChannelError> {
        self.users.retire_joined_logical_resources(deadline).await?;
        if !self.users.retire_joined_carriers(deadline).await {
            return Err(self.users.first_failure().map_or_else(
                || {
                    failure(
                        Stage::Close,
                        Reason::Timeout,
                        "peer carrier cleanup remains pending",
                    )
                },
                retained,
            ));
        }
        Ok(())
    }
}
impl ResourceLifecycle for AcceptedPeerScope {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        self.tasks().caller()?.check_open()
    }
    fn revoke(&self, reason: crate::contract::CloseReason) {
        self.users.cancel.cancel();
        if let Some(control) = &self.control {
            control.cancel(reason);
        }
        self.users.revoke_resources();
        self.users.revoke_carriers();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(crate::contract::CloseReason::Shutdown);
        if !self.users.retire(deadline).await {
            if let Some(source) = self.users.first_failure() {
                return Err(retained(source));
            }
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "native peer originals or Scope joins remain pending".into(),
                )],
                ..Default::default()
            });
        }
        let mut report = match &mut self.control {
            Some(control) => control.wait_closed(deadline).await?,
            None => CloseReport::default(),
        };
        if report.is_complete() {
            report.append(
                self.users
                    .peer
                    .as_ref()
                    .unwrap()
                    .reports
                    .lock()
                    .unwrap()
                    .take(),
            );
            if let Some(admission) = &self.admission {
                admission.retirement().release_after_cleanup();
            }
        }
        // Native listener closure is supplied separately by its original provider.
        Ok(report)
    }
}
impl Users {
    pub(super) fn first_failure(&self) -> Option<Arc<ChannelError>> {
        self.peer
            .as_ref()
            .and_then(|p| p.failure.lock().unwrap().clone())
            .or_else(|| self.entry.upgrade().and_then(|e| e.state().failure.clone()))
    }
    pub(super) fn append_report(&self, ordinal: usize, report: CloseReport) -> bool {
        if let Some(peer) = &self.peer {
            peer.reports.lock().unwrap().append(ordinal, report);
            true
        } else if let Some(pool) = self.pool.upgrade() {
            pool.state().child_reports.append(ordinal, report);
            true
        } else {
            false
        }
    }
    pub(super) fn logical_charge(&self, acquire: bool) {
        let charge = |counter: &AtomicUsize| {
            if acquire {
                counter.fetch_add(1, Ordering::AcqRel);
            } else {
                counter.fetch_sub(1, Ordering::AcqRel);
            }
        };
        if let Some(peer) = &self.peer {
            charge(&peer.custody);
        } else if let Some(pool) = self.pool.upgrade() {
            charge(&pool.logical_custody);
        }
    }
}
