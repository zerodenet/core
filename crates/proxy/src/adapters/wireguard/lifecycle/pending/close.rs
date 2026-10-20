//! Close only this candidate's declared peers, in their actual dependency order.
use super::*;
use crate::runtime::udp_dispatch::packet_owner::PacketCarrierCloseReport;

pub(in crate::adapters::wireguard) struct PendingPeerCloseReport {
    pub(in crate::adapters::wireguard) workers_drained: bool,
    pub(in crate::adapters::wireguard) owners_current: bool,
    pub(in crate::adapters::wireguard) native: Vec<PacketCarrierCloseReport>,
    pub(in crate::adapters::wireguard) failures: Vec<Arc<EngineError>>,
}
impl PendingPeerCloseReport {
    pub(in crate::adapters::wireguard) fn is_complete(&self) -> bool {
        self.workers_drained
            && self.owners_current
            && self
                .native
                .iter()
                .all(PacketCarrierCloseReport::is_complete)
    }
}
impl PendingLinkedEndpoint {
    /// The host has already stopped the affected original listener before this
    /// phase. These are client stacks/outer carriers, not its native close ACK.
    /// Partial candidates remain in the same pending map until this completes.
    pub(in crate::adapters::wireguard) async fn stop_peer_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> PendingPeerCloseReport {
        if !self.owned {
            return PendingPeerCloseReport {
                workers_drained: false,
                owners_current: false,
                native: Vec::new(),
                failures: vec![Arc::new(super::super::invalid(
                    "warm legacy linked graph has no candidate resource owner",
                ))],
            };
        }
        for stack in &self.peer_stacks {
            stack.revoke();
        }
        for carrier in &self.peer_carriers {
            carrier.revoke();
        }
        let carriers = self
            .ready()
            .map(|link| link.outer_carriers())
            .unwrap_or_default();
        let waits: Vec<_> = carriers
            .iter()
            .map(|carrier| carrier.stop_background())
            .collect();
        let mut failures = Vec::new();
        for waiter in waits {
            let result = match waiter {
                Some(waiter) => tokio::time::timeout_at(deadline, waiter)
                    .await
                    .map_err(|source| {
                        EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source))
                    })
                    .and_then(|result| result),
                None => Err(super::super::invalid(
                    "outer carrier has no original background task owner",
                )),
            };
            if let Err(error) = result {
                failures.push(Arc::new(error));
            }
        }
        for stack in &self.peer_stacks {
            if let Err(error) = stack.stop(deadline).await {
                failures.push(Arc::new(error));
            }
        }
        let workers_drained = self.peer_stacks.iter().all(|stack| stack.is_drained())
            && carriers.iter().all(|carrier| carrier.background_drained());
        let owners_current = self
            .ready()
            .is_none_or(|link| link.carrier_owners_match(&self.peer_carriers));
        if !owners_current {
            failures.push(Arc::new(super::super::invalid(
                "outer carrier no longer belongs to the candidate's original native owner",
            )));
        }
        // An actual joined task failure is preserved but does not hide the
        // original native handles from cleanup. A timeout never authorizes it.
        let native = if workers_drained {
            self.peer_carriers
                .iter()
                .map(|carrier| carrier.close_native())
                .collect()
        } else {
            Vec::new()
        };
        PendingPeerCloseReport {
            workers_drained,
            owners_current,
            native,
            failures,
        }
    }
}
