use super::*;
use crate::{
    coordinator::{QueryCoordinator, QueryKey, QueryScope},
    DnsQueryRole,
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{runtime::Handle, sync::Barrier, time::Instant};
use zero_platform_tokio::contract::scope::TaskScope;
#[path = "faults/ack.rs"]
mod ack;
#[path = "faults/late_ack.rs"]
mod late_ack;
#[tokio::test]
async fn ordinary_resolution_errors_do_not_accumulate_but_original_worker_faults_are_finite_and_retained_after_host_ack(
) {
    tokio::time::timeout(Duration::from_secs(5),async {
        let n=NonZeroUsize::new(3).unwrap();
        let mut scope=TaskScope::suspended_hosted(Handle::current(),Some(n));
        scope.enable_registration().unwrap();
        let tasks=OwnedDnsQueryTasks::new_critical(&scope.registrar(),n);
        let coordinator=QueryCoordinator::<Vec<u8>>::with_graph_and_tasks(Default::default(),Some(tasks.clone()));
        for i in 0..24 {
            let key=QueryKey::new(&format!("failure-{i}.test"),1,QueryScope::new(DnsQueryRole::Default,0));
            let query=coordinator.resolve(key,async {Err(io::Error::new(io::ErrorKind::ConnectionRefused,"actual upstream resolution error"))});
            tokio::pin!(query);
            let mut receipt=None;
            let result=loop {
                tokio::select! { biased;
                    result=&mut query=>break result,
                    next=scope.next_receipt()=>{let next=next.unwrap();assert!(next.result().is_ok());scope.acknowledge(&next).unwrap();receipt=Some(next);},
                }
            };
            assert_eq!(result.unwrap_err().kind(),io::ErrorKind::ConnectionRefused);
            if receipt.is_none() {
                let next=scope.next_receipt().await.unwrap();assert!(next.result().is_ok());scope.acknowledge(&next).unwrap();
            }
            tasks.reap();
            assert!(tasks.retained_faults().is_none(),"caller-received DNS errors are not ownership faults");
            assert!(tasks.ensure_open().is_ok());
        }
        let gate=Arc::new(Barrier::new(n.get()+1));
        for _ in 0..n.get() {
            let gate=gate.clone();
            tasks.spawn(async move {gate.wait().await;panic!("actual DNS ownership worker failure");},CancellationToken::new()).unwrap();
        }
        let originals=tasks.observe().into_iter().map(|record|record.task).collect::<Vec<_>>();
        assert_eq!(originals.len(),n.get());
        gate.wait().await;
        let mut receipts=Vec::new();
        for _ in 0..n.get() {
            let actual=scope.next_receipt().await.unwrap();assert!(actual.result().is_err());receipts.push(actual);
        }
        tasks.reap();
        let received=tasks.retained_faults().unwrap();
        assert_eq!(received.faults().len(),n.get());
        for fault in received.faults() {
            let actual=receipts.iter().find(|actual|fault.query.matches_joined(actual.joined())).unwrap();
            assert!(Arc::ptr_eq(&fault.error,&actual.result().unwrap_err()));
            assert!(fault.joined.failure_accepted());
            assert!(!fault.joined.registration_released(),"local failure acceptance is not Host budget ACK");
            assert!(originals.iter().any(|original|original.same_registration(&fault.query)));
        }
        assert!(tasks.close_descendants(&originals[0], Instant::now()+Duration::from_secs(1)).await.is_err(),
            "canonical local acceptance without actual Host ACK cannot erase fault-delivery debt");
        for receipt in receipts {scope.acknowledge(&receipt).unwrap();}
        tasks.close_descendants(&originals[0], Instant::now()+Duration::from_secs(1)).await.unwrap();
        assert!(tasks.ensure_open().is_err(),
            "confirmed descendant retirement does not reopen this genuinely failed owner");
        let executed=Arc::new(AtomicBool::new(false));
        for _ in 0..128 {
            let executed=executed.clone();
            assert!(tasks.spawn(async move {executed.store(true,Ordering::Release);},CancellationToken::new()).is_err());
        }
        assert!(!executed.load(Ordering::Acquire));
        let retained=tasks.retained_faults().unwrap();
        assert_eq!(retained.faults().len(),n.get(),"fatal owner stops admission, no lifetime-query fault history");
        for fault in retained.faults() {
            let original=received.faults().iter().find(|original|original.query.same_registration(&fault.query)).unwrap();
            assert!(Arc::ptr_eq(&original.error,&fault.error));
            assert!(fault.joined.registration_released());
        }
        assert!(tasks.shutdown(Instant::now()+Duration::from_secs(1)).await.is_err());
        assert_eq!(tasks.retained_faults().unwrap().faults().len(),n.get());
        scope.begin_shutdown();assert!(scope.is_drained());
    }).await.unwrap();
}
