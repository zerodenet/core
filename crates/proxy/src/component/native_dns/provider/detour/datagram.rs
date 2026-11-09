//! The selected device lends its actual socket; DNS retains its logical owner.
use super::*;
use crate::{inventory::service::ServiceDatagram, protocol_registry::TcpExecutionServices};
use zero_core::component::{AssociationId, NetworkGeneration};

impl Provider {
    pub(super) async fn open_datagram_detour(
        &self,
        execution: &TcpExecutionServices,
        node: ServiceDatagram,
        selections: ServiceSelections,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        call: CarrierContext,
    ) -> std::io::Result<DatagramLoan> {
        let end = call.deadline.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "DNS datagram requires its actual attempt deadline",
            )
        })?;
        let cancel = &call.cancel;
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| std::io::Error::other(source.clone()))?;
        let role = node.slot.role_id();
        let selected = selections
            .chosen(&[node.tag.as_deref()])
            .ok_or_else(|| std::io::Error::other("DNS datagram selection cannot be captured"))?;
        let admission = node
            .slot
            .service_admission()
            .ok_or_else(|| std::io::Error::other("DNS datagram owner is not admitted"))?;
        let generation = NetworkGeneration(self.egress.generation());
        let binding = UpstreamBinding {
            prefix: vec![role],
            dial: Default::default(),
            network_generation: generation,
        };
        let root_binding = UpstreamBinding {
            prefix: vec![],
            ..binding.clone()
        };
        let grants = &self
            .peers
            .iter()
            .find(|(address, packet, _)| *address == peer && *packet)
            .ok_or_else(|| std::io::Error::other("DNS datagram endpoint was not declared"))?
            .2;
        let network = node.tag.as_deref().map_or_else(
            || execution.upstream(),
            |tag| execution.upstream_observed(tag),
        );
        let root = Arc::new(
            root::Root::new(
                network,
                destination(peer),
                root_binding.clone(),
                grants.clone(),
                self.budget.clone(),
                self.users.clone(),
                self.pending.clone(),
                caller.clone(),
                cancel.clone(),
                call.clone(),
                execution.current_source.clone(),
                binding.prefix.clone(),
                self.cleanup_receiver(),
            )
            .map_err(std::io::Error::other)?
            .observer_only(),
        );
        root.track();
        plan.authorize_carrier_prefix(self.instance, &binding.prefix)
            .map_err(std::io::Error::other)?;
        let epoch = self.admit(&caller, cancel, end, 1).await?;
        drop(epoch);
        let capacity = self.budget.reserve(1)?;
        let mut grant = grants.reserve().map_err(std::io::Error::other)?;
        let view = grant
            .claim_transferred_view()
            .map_err(std::io::Error::other)?;
        let entry = Arc::new(custody::NativeUse {
            receiver: self.cleanup_receiver(),
            pending_report: Mutex::default(),
            protocol_originals: Mutex::default(),
            task: caller.clone(),
            cancel: cancel.clone(),
            original: grant.adoption(),
            pending_roles: Arc::from([]),
            logical: Default::default(),
            capacity: Mutex::new(Some(capacity)),
        });
        self.users
            .lock()
            .expect("DNS users poisoned")
            .push(entry.clone());
        root.bind_query_entry(&entry)?;
        let attempt = async {
            let loan = crate::runtime::tcp_dispatch::service::connect_carrier_datagram(
                execution,
                &destination(peer),
                &node,
                &call,
                UpstreamTransport::new(root_binding, root),
            )
            .await
            .map_err(std::io::Error::other)?;
            let origin = loan.owner.original().clone();
            // Install the real donor controller before validation or another
            // await. Any rejection still leaves cleanup in this query ledger.
            let io = grant
                .adopt_datagram_loan(&binding, destination(peer), &call, loan)
                .map_err(std::io::Error::other)?;
            self.release_installed_pending(&caller);
            plan.validate_carrier_origin(role, &origin)
                .map_err(std::io::Error::other)?;
            admission.check_current().map_err(std::io::Error::other)?;
            for selection in &selected {
                selection.check_current().map_err(std::io::Error::other)?;
            }
            if self.egress.generation() != generation.0 || call.cancel.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "DNS datagram selection changed during acquisition",
                ));
            }
            io.into_datagram_loan(view, AssociationId(0))
                .map_err(std::io::Error::other)
        }
        .await;
        match attempt {
            Ok(loan) => Ok(loan),
            Err(opening) => match self.close_attempt_originals(&caller, end).await {
                Ok(()) => Err(opening),
                Err(cleanup) => Err(std::io::Error::new(
                    cleanup.kind(),
                    custody::OpenCleanupFailure { opening, cleanup },
                )),
            },
        }
    }
}
