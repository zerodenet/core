//! One actual ECH endpoint's Node candidates remain in the existing provider.
use super::*;
use zero_platform_tokio::controlled_carrier::{
    CarrierCandidates, CarrierGrant, CarrierUserView, ResolvingCarrierOpener,
};

pub(super) struct Prepared {
    caller: TaskJoinWitness,
    target: zero_dns::EchDnsEndpoint,
    candidates: CarrierCandidates,
}
impl Provider {
    pub(super) async fn prepare_ech(
        &self,
        target: zero_dns::EchDnsEndpoint,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<Vec<SocketAddr>> {
        let epoch = self.admit(&caller, &cancel, end, 0).await?;
        let generation = *epoch;
        drop(epoch);
        if self.tasks.ech_endpoint_for(&caller).as_ref() != Some(&target) {
            return Err(denied(
                "ECH target is not bound to this actual DNS registration",
            ));
        }
        let grants = &self
            .ech
            .iter()
            .find(|(declared, _)| *declared == target)
            .ok_or_else(|| denied("ECH endpoint was not declared by this actual configuration"))?
            .1;
        let endpoint = super::super::ech::destination(&target);
        let binding = UpstreamBinding {
            prefix: Vec::new(),
            dial: Default::default(),
            network_generation: zero_core::component::NetworkGeneration(generation),
        };
        let dns = self
            .network
            .resolver
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| denied("actual Node resolver retired"))?;
        let opener = ResolvingCarrierOpener::new(
            grants.registration().instance(),
            binding.clone(),
            endpoint.clone(),
            dns.native_node_service(),
            Arc::new(self.egress.clone()),
            None,
        )
        .map_err(std::io::Error::other)?;
        // No global admission mutex/native permit is held during bootstrap. Its
        // actual child query/blocking originals settle before the ECH original.
        let candidates = opener
            .resolve_controlled_carrier(
                &binding,
                endpoint,
                CarrierContext {
                    session: None,
                    deadline: Some(end),
                    cancel: cancel.clone(),
                },
            )
            .await
            .map_err(std::io::Error::other)?;
        self.tasks.close_descendants(&caller, end).await?;
        drop(self.admit(&caller, &cancel, end, 0).await?);
        let addresses = (0..candidates.len())
            .filter_map(|index| candidates.peer(index))
            .collect();
        let mut prepared = self
            .prepared_ech
            .lock()
            .expect("DNS ECH candidates poisoned");
        prepared.retain(|entry| !entry.caller.is_joined());
        if let Some(old) = prepared
            .iter()
            .find(|entry| entry.caller.same_registration(&caller))
        {
            if old.target != target {
                return Err(denied(
                    "actual DNS caller already prepared another ECH endpoint",
                ));
            }
            return Ok((0..old.candidates.len())
                .filter_map(|index| old.candidates.peer(index))
                .collect());
        }
        prepared.push(Arc::new(Prepared {
            caller,
            target,
            candidates,
        }));
        Ok(addresses)
    }
    async fn ech_grant(
        &self,
        peer: SocketAddr,
        packet: bool,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<(CarrierGrant, CarrierUserView, Arc<Prepared>, usize)> {
        let target = self
            .tasks
            .ech_endpoint_for(&caller)
            .ok_or_else(|| denied("undeclared native DNS endpoint"))?;
        if target.is_datagram() != packet {
            return Err(denied("ECH original has a different wire kind"));
        }
        let prepared = {
            self.prepared_ech
                .lock()
                .expect("DNS ECH candidates poisoned")
                .iter()
                .find(|entry| entry.caller.same_registration(&caller))
                .cloned()
        };
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => {
                // The persistent H2 driver is its own original task. It resolves
                // with its own cancellation/generation, never the waiting query's.
                self.prepare_ech(target.clone(), caller.clone(), cancel.clone(), end)
                    .await?;
                self.prepared_ech
                    .lock()
                    .expect("DNS ECH candidates poisoned")
                    .iter()
                    .find(|entry| entry.caller.same_registration(&caller))
                    .cloned()
                    .ok_or_else(|| denied("actual ECH candidates were lost"))?
            }
        };
        let index = (0..prepared.candidates.len())
            .find(|index| prepared.candidates.peer(*index) == Some(peer))
            .ok_or_else(|| denied("ECH peer is outside its actual Node answer"))?;
        let epoch = self.admit(&caller, &cancel, end, 1).await?;
        let grants = &self
            .ech
            .iter()
            .find(|(declared, _)| *declared == target)
            .ok_or_else(|| denied("ECH endpoint declaration retired"))?
            .1;
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
        drop(epoch);
        Ok((grant, view, prepared, index))
    }
    pub(super) async fn open_ech_stream(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<StreamLoan> {
        let (grant, view, prepared, index) =
            self.ech_grant(peer, false, caller, cancel, end).await?;
        let original = grant.adoption();
        let result = async {
            prepared
                .candidates
                .prepare_stream(index, grant)
                .map_err(std::io::Error::other)?
                .open(end)
                .await
                .map_err(std::io::Error::other)?
                .into_parts()
                .0
                .into_stream_loan(view)
                .map_err(std::io::Error::other)
        }
        .await;
        match result {
            Ok(loan) => Ok(loan),
            Err(source) => Err(self.failed_open(source, &original, end).await),
        }
    }
    pub(super) async fn open_ech_datagram(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<DatagramLoan> {
        let (grant, view, prepared, index) =
            self.ech_grant(peer, true, caller, cancel, end).await?;
        let original = grant.adoption();
        let result = prepared
            .candidates
            .prepare_datagram(index, grant)
            .map_err(std::io::Error::other)
            .and_then(|carrier| {
                carrier
                    .into_datagram_loan(view, zero_core::component::AssociationId(0))
                    .map_err(std::io::Error::other)
            });
        match result {
            Ok(loan) => Ok(loan),
            Err(source) => Err(self.failed_open(source, &original, end).await),
        }
    }
}
fn denied(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::PermissionDenied, message)
}
