//! Pure layout of this Scope's concrete witness/join/error Arc allocations.
use super::{ComponentError, Joined, JoinedState, TaskJoinWitness, TaskJoined};
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataLayout};

impl TaskJoinWitness {
    /// Peak liability for the actual optional witness, consumed join and failed
    /// result allocations issued by `new`/`complete` in this same module.
    ///
    /// This computes storage only: it neither reserves credit nor observes an
    /// allocation's destruction. The actual receiver Book must prepay and keep
    /// the credit for its proven original/header lifetime, including registration
    /// Weak custody. Releasing/reusing a delivery cell alone proves none of that.
    /// Foreign source payloads, notification/queue allocations and arbitrary
    /// existing public raw Arc/Weak escape are outside these three layouts.
    pub(crate) fn task_failure_header_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLayout::arc::<JoinedState>()?
            .checked_add(PacketMetadataLayout::arc::<Joined>()?)?
            .checked_add(PacketMetadataLayout::arc::<ComponentError>()?)
    }
}

impl TaskJoined {
    /// Borrow this same original result for the closed receiver delivery view.
    /// No Arc/Weak handle leaves its actual Book through this projection.
    pub(crate) fn error_ref(&self) -> Option<&ComponentError> {
        self.0.result.as_ref().err().map(std::sync::Arc::as_ref)
    }
}

impl TaskJoinWitness {
    /// Consume only exclusive, successful, actually released metadata originals.
    /// No surviving observer can see a completed witness become uncompleted.
    /// The caller must retain its prepaid liability and call outside Book locks.
    /// Failure returns both original handles for that same bounded retirement
    /// responsibility; success reports allocation destruction only, never IO,
    /// native closure, task exit, local failure acceptance or Host ACK.
    pub(crate) fn try_retire_success_headers(
        mut witness: Self,
        mut joined: TaskJoined,
    ) -> Result<(), (Self, TaskJoined)> {
        if joined.0.result.is_err() || !joined.registration_released() {
            return Err((witness, joined));
        }
        let matches = witness
            .0
            .joined
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|actual| actual.same_receipt(&joined));
        if !matches {
            return Err((witness, joined));
        }
        // get_mut atomically excludes every other strong/weak handle. Separate
        // count snapshots would race a concurrent Weak::upgrade/drop/downgrade.
        let Some(state) = std::sync::Arc::get_mut(&mut witness.0) else {
            return Err((witness, joined));
        };
        let original = state
            .joined
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("same unique witness already owns its actual join");
        // The input still strongly owns this same Joined allocation, so this
        // decrement cannot destroy its body or run any foreign destructor.
        drop(original);
        if std::sync::Arc::get_mut(&mut joined.0).is_none() {
            // This state is exclusive and unobservable. Restore the identical
            // receipt before returning either handle to any possible observer.
            *state
                .joined
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(joined.clone());
            return Err((witness, joined));
        }
        // Both actual headers are now exclusive, with no Weak upgrade path.
        // Real deallocation precedes the caller's eventual credit return.
        drop(witness);
        drop(joined);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::scope::{HostedExitReceipt, HostedScope, TaskClass, TaskScope};
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    async fn success() -> (HostedScope, TaskJoinWitness, HostedExitReceipt, TaskJoined) {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(NonZeroUsize::MIN));
        scope.enable_registration().unwrap();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let joined = witness.try_joined().unwrap();
        (scope, witness, receipt, joined)
    }

    #[tokio::test]
    async fn successful_headers_wait_for_real_release_and_every_original_strong_and_weak_observer()
    {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut scope, witness, receipt, joined) = success().await;
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(witness.is_joined() && witness.matches_joined(&joined));
            assert!(!joined.registration_released());
            scope.acknowledge(&receipt).unwrap();
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(witness.is_joined() && joined.registration_released());
            drop(receipt); // the actual offered receipt also owns this Joined

            let observer = witness.clone();
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(observer.is_joined() && observer.matches_joined(&joined));
            drop(observer);
            let weak_witness = Arc::downgrade(&witness.0);
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(witness.is_joined() && weak_witness.upgrade().is_some());
            drop(weak_witness);

            let observer = joined.clone();
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(witness.is_joined() && observer.same_receipt(&joined));
            drop(observer);
            let weak_joined = Arc::downgrade(&joined.0);
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(witness.is_joined() && weak_joined.upgrade().is_some());
            drop(weak_joined);
            scope.begin_shutdown();
            assert!(scope.is_drained());
            // No native or task result is manufactured: only the two exclusive
            // existing allocation headers can now actually be destroyed.
            TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn failed_or_foreign_original_receipts_return_unchanged_completed_witnesses() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut scope, witness, receipt, joined) = success().await;
            scope.acknowledge(&receipt).unwrap();
            drop(receipt);
            let (mut other, foreign, receipt, foreign_joined) = success().await;
            other.acknowledge(&receipt).unwrap();
            drop(receipt);
            let (witness, foreign_joined) =
                TaskJoinWitness::try_retire_success_headers(witness, foreign_joined).unwrap_err();
            assert!(witness.is_joined() && witness.matches_joined(&joined));
            assert!(foreign.is_joined() && foreign.matches_joined(&foreign_joined));
            TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap();
            TaskJoinWitness::try_retire_success_headers(foreign, foreign_joined).unwrap();
            scope.begin_shutdown();
            other.begin_shutdown();
            assert!(scope.is_drained() && other.is_drained());

            let mut scope = TaskScope::suspended_hosted(
                tokio::runtime::Handle::current(),
                Some(NonZeroUsize::MIN),
            );
            scope.enable_registration().unwrap();
            let (_, witness) = scope
                .registrar()
                .spawn_tracked(TaskClass::Auxiliary, async {
                    Err(crate::contract::ChannelError::new(
                        crate::contract::Stage::Receive,
                        crate::contract::Reason::Io,
                        crate::contract::Impact::Session,
                    ))
                })
                .unwrap();
            let receipt = scope.next_receipt().await.unwrap();
            let actual = receipt.result().unwrap_err();
            let joined = witness.try_joined().unwrap();
            witness.acknowledge_hosted_failure(&joined).unwrap();
            scope.acknowledge(&receipt).unwrap();
            drop(receipt);
            let (witness, joined) =
                TaskJoinWitness::try_retire_success_headers(witness, joined).unwrap_err();
            assert!(witness.is_joined() && witness.matches_joined(&joined));
            assert!(Arc::ptr_eq(&joined.result().unwrap_err(), &actual));
            scope.begin_shutdown();
            assert!(scope.is_drained());
        })
        .await
        .unwrap();
    }
}
