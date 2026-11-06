//! A warm real Process job must retire even when a larger counter would fit it.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[path = "process_accounting/fixtures.rs"]
mod fixtures;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn process_limits_replace_the_actual_warm_job_before_install_preserving_live_users_and_the_same_security_book(
) {
    tokio::time::timeout(std::time::Duration::from_secs(15),async {
        let directory=tempfile::tempdir().unwrap();let marker=directory.path().join("actual-child");
        let catalog=fixtures::catalog(&marker);
        let initial=Arc::new(RuntimeConfig::parse(r#"{"runtime":{"process_max_jobs":1,"process_max_jobs_per_instance":1},"route":{"final":{"type":"direct"}}}"#).unwrap());
        catalog.start(initial.clone(),end()).await.unwrap();catalog.commit(&initial,||Ok(())).unwrap();
        let plan=catalog.current(&initial).unwrap();let(process,replay)=fixtures::dependencies(&plan);
        let root=catalog.original_budgets();
        assert!(process.is_unbuilt_demand_process());assert!(!marker.exists());assert_eq!(root.process.snapshot().charged_jobs,0);
        catalog.drive_supervised(process.start_owned(end()).unwrap()).await.unwrap();
        let old=process.ready_process_endpoint(0).unwrap();assert!(old.closed().is_none());assert!(marker.exists());assert_eq!(root.process.snapshot().charged_jobs,1);
        let direct=plan.direct(None).unwrap();let users=direct.own_user_root();
        let lease=crate::component::instance::users::Lease::enter(direct.id,std::slice::from_ref(&users),None).unwrap().unwrap();
        let(io,mut peer)=tokio::io::duplex(16);let mut admitted=lease.stream(Box::pin(io));
        admitted.write_all(b"before").await.unwrap();let mut bytes=[0;6];peer.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"before");
        let mut next=(*initial).clone();next.runtime.process_max_jobs=Some(2);next.runtime.process_max_jobs_per_instance=Some(2);let next=Arc::new(next);
        let candidate=catalog.prepare(next.clone()).unwrap();let(new_process,new_replay)=fixtures::dependencies(&candidate);
        assert_eq!(process.recipe.spec(),new_process.recipe.spec(),"protocol Options/static identity are unchanged");
        assert!(!Arc::ptr_eq(&process,&new_process),"Host Process dimensions require a fresh actual original owner");
        assert!(Arc::ptr_eq(&replay,&new_replay),"security history/maintenance is unrelated to Process budget");
        let refused=catalog.start(next.clone(),end()).await.unwrap_err();assert_eq!(refused.reason,Reason::Capacity);
        assert!(Arc::ptr_eq(&plan,&catalog.current(&initial).unwrap()));assert!(old.closed().is_none());assert_eq!(root.process.snapshot().charged_jobs,1);
        peer.write_all(b"alive!").await.unwrap();admitted.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"alive!");
        drop(admitted);assert_eq!(peer.read(&mut[0;1]).await.unwrap(),0);
        let prepared=catalog.prepare_resource_start(next.clone(),end()).await.unwrap();
        assert!(Arc::ptr_eq(prepared.plan(),&candidate));
        assert!(!candidate.started.load(Ordering::Acquire));
        assert!(old.closed().unwrap().matches(&old));
        assert_eq!(root.process.snapshot().charged_jobs,0,"new cap installs only after the old native job, actor and actual Host ACK refund");
        assert_eq!(root.process.snapshot().limits.max_jobs,2);assert_eq!(root.process.snapshot().limits.max_jobs_per_instance,2);
        assert!(Arc::ptr_eq(&root,&catalog.original_budgets()));assert!(new_process.is_unbuilt_demand_process());
        prepared.start(end()).await.unwrap();
        catalog.commit(&next,||Ok(())).unwrap();
        let current=catalog.current(&next).unwrap();let(accepted_process,accepted_replay)=fixtures::dependencies(&current);
        assert!(Arc::ptr_eq(&new_process,&accepted_process));assert!(Arc::ptr_eq(&replay,&accepted_replay));
        catalog.drive_supervised(accepted_process.start_owned(end()).unwrap()).await.unwrap();
        let new_endpoint=accepted_process.ready_process_endpoint(0).unwrap();assert_ne!(old.instance(),new_endpoint.instance());assert!(new_endpoint.closed().is_none());assert_eq!(root.process.snapshot().charged_jobs,1);
        catalog.shutdown(end()).await.unwrap();assert!(new_endpoint.closed().unwrap().matches(&new_endpoint));assert_eq!(root.process.snapshot().charged_jobs,0);
    }).await.unwrap();
}
