//! Actual accepted claims retain their global connection charge across awaits.
use super::*;
use std::{collections::BTreeMap, sync::Mutex};
use zero_platform_tokio::AcceptedTcpClaim;
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
#[derive(Default)]
pub(crate) struct Connections {
    acquiring: Mutex<Option<ControlBudgetPermit>>,
    entries: Mutex<BTreeMap<u64, (AcceptedTcpClaim, ControlBudgetPermit)>>,
}
impl Connections {
    pub(crate) fn before(&self, budget: &Arc<ControlBudget>) -> Result<(), ChannelError> {
        let mut acquiring = self
            .acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if acquiring.is_some() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        *acquiring = Some(budget.reserve(ControlCharge::Connection).map_err(|error| {
            ChannelError::new(
                Stage::Prepare,
                if error.kind() == io::ErrorKind::WouldBlock {
                    Reason::Capacity
                } else {
                    Reason::Revoked
                },
                Impact::Session,
            )
            .with_source(error)
        })?);
        Ok(())
    }
    pub(crate) fn no_native(&self) {
        self.acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
    pub(crate) fn installed(&self, claim: AcceptedTcpClaim) {
        let permit = self
            .acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("charge installed before native accept");
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(claim.id(), (claim, permit));
    }
    pub(crate) async fn reap(
        &self,
        receiver: &zero_platform_tokio::AcceptedTcpTaskReceiver,
    ) -> io::Result<()> {
        let claims = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|(claim, _)| claim.clone())
            .collect::<Vec<_>>();
        for claim in claims {
            if let Some(closed) = receiver
                .close_if_joined(&claim, deadline())
                .await
                .map_err(io::Error::other)?
            {
                if !closed.matches(&claim) {
                    return Err(io::Error::other("foreign gRPC accepted close"));
                }
                self.entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&claim.id());
            }
        }
        Ok(())
    }
    /// Called only after this same listener's entire native/accepted report and
    /// actual Scope joins are complete. Unknown accept charges stay until then.
    pub(crate) fn confirm_all_closed(&self) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.no_native();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    #[tokio::test]
    async fn actual_accept_returns_pending_charge_and_full_root_budget_does_not_accept_native() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let budget = ControlBudget::new(zero_runtime::configuration::ControlBudgetLimits {
                connections: 1,
                subscriptions: 1,
                queued_event_bytes: 1024,
            });
            let charges = Connections::default();
            let (mut scope, mut original) = cold("127.0.0.1:0".parse().unwrap()).unwrap();
            let mut listener = original
                .bind_listener("127.0.0.1:0".parse().unwrap())
                .unwrap();
            let receiver = original.accepted_task_receiver().unwrap();
            let mut pending =
                Box::pin(listener.accept_owned_with_admission(
                    || charges.before(&budget),
                    || charges.no_native(),
                ));
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(
                budget.usage().connections,
                0,
                "known Pending has no native original"
            );
            drop(pending);
            let held = budget.reserve(ControlCharge::Connection).unwrap();
            let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let error = listener
                .accept_owned_with_admission(|| charges.before(&budget), || charges.no_native())
                .await
                .err()
                .unwrap();
            assert_eq!(error.reason, Reason::Capacity);
            assert_eq!(
                original.accepted_evidence().unwrap().retained,
                0,
                "full root capacity rejects before native accept"
            );
            assert_eq!(
                original
                    .accepted_evidence()
                    .unwrap()
                    .confirmed_native_closes,
                0
            );
            drop(held);
            let (accepted, _) = listener
                .accept_owned_with_admission(|| charges.before(&budget), || charges.no_native())
                .await
                .unwrap();
            charges.installed(accepted.claim());
            assert_eq!(budget.usage().connections, 1);
            accepted
                .spawn_handler_bound(TaskClass::Auxiliary, |socket, _| async move {
                    drop(socket);
                    Ok(())
                })
                .unwrap();
            let exit = scope.next_exit().await.unwrap();
            assert!(exit.result.is_ok());
            receiver.observe_joined(exit.id);
            charges.reap(&receiver).await.unwrap();
            assert_eq!(
                budget.usage().connections,
                0,
                "only actual joined users plus matching native close return charge"
            );
            drop(client);
            drop(listener);
            assert!(scope.shutdown(deadline()).await.is_complete());
            assert!(original
                .wait_closed(deadline())
                .await
                .unwrap()
                .is_complete());
        })
        .await
        .unwrap();
    }
}
