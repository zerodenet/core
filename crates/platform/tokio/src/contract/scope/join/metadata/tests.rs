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
