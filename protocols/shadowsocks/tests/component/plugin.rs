//! The one real Scope task spans native child startup and readiness monitoring.
//! This standalone Child seam is not process-tree closure qualification.
use super::*;
use std::sync::atomic::AtomicUsize;
use zero_core::{component::ResourceGeneration, request::Destination};
use zero_platform_tokio::contract::{
    packet::BoxPacketSession,
    scope::{SpawnError, TaskScope},
    BoxFuture,
};
mod grant;
use grant::Grant;
fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
fn build(
    scope: &TaskScope,
    grant: Arc<Grant>,
    resource: ShadowsocksPluginResource,
) -> ComponentInstance {
    PluginFactory
        .build(
            Arc::new(PluginOptions::new(resource)),
            BuildContext {
                component: ComponentId(770),
                generation: ResourceGeneration(1),
                dependencies: Arc::from([]),
                roles: Default::default(),
                resources: grant,
                tasks: scope.registrar(),
                cancel: CancellationToken::new(),
                background_carrier: None,
            },
        )
        .unwrap()
}
#[tokio::test]
async fn actual_actor_capacity_is_reserved_before_any_child_and_probe_io() {
    let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
    let grant = Arc::new(Grant::default());
    let state = crate::transport::ShadowsocksTransportState::default();
    let resource = state.plugin_resource(
        "127.0.0.1",
        9999,
        crate::transport::plugin::fixtures::config(),
    );
    let mut owner = build(&scope, grant.clone(), resource.clone());
    assert_eq!(
        owner.lifecycle.start(deadline()).await.unwrap_err().reason,
        Reason::Capacity
    );
    assert_eq!(grant.starts.load(Ordering::SeqCst), 0);
    assert!(owner
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
    assert_eq!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::NotStarted
    );
}
#[tokio::test]
async fn actual_single_actor_keeps_one_child_through_readiness_and_matches_its_real_scope_exit() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let grant = Arc::new(Grant::default());
        let state = crate::transport::ShadowsocksTransportState::default();
        let resource = state.plugin_resource(
            "127.0.0.1",
            9999,
            crate::transport::plugin::fixtures::config(),
        );
        let mut owner = build(&scope, grant.clone(), resource.clone());
        owner.lifecycle.start(deadline()).await.unwrap();
        assert_eq!(grant.starts.load(Ordering::SeqCst), 1);
        assert!(matches!(
            scope.registrar().spawn(
                zero_platform_tokio::contract::lifecycle::TaskClass::Critical,
                async { Ok(()) }
            ),
            Err(SpawnError::Capacity)
        ));
        let original_endpoint = resource.start().await.unwrap();
        assert_eq!(resource.start().await.unwrap(), original_endpoint);
        assert_eq!(grant.starts.load(Ordering::SeqCst), 1);
        let (native, tasks) = tokio::join!(
            owner.lifecycle.shutdown(deadline()),
            scope.shutdown(deadline())
        );
        assert!(native.unwrap().is_complete());
        assert!(tasks.is_complete());
        assert_eq!(tasks.exited_tasks.len(), 1);
        assert!(matches!(
            resource.shutdown(deadline()).await.unwrap(),
            PluginTermination::Exited(_)
        ));
        assert!(resource.start().await.is_err());
        assert_eq!(grant.starts.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn same_actor_closes_declared_process_reservations_and_probe_before_the_same_job_is_ready() {
    tokio::time::timeout(std::time::Duration::from_secs(8),async {
        let mut scope=TaskScope::new(tokio::runtime::Handle::current(),NonZeroUsize::new(1).unwrap());
        let grant=Arc::new(Grant::default());
        let state=crate::transport::ShadowsocksTransportState::default();
        let options=state.plugin_component_options("127.0.0.1",9999,crate::transport::plugin::fixtures::config()).with_process_endpoint(zero_core::component::NetworkGeneration(0));
        let spec=PluginFactory.describe(&options);
        assert_eq!(spec.task_limit.unwrap().get(),1);
        assert!(matches!(&spec.resources[0],ResourceDeclaration::Process{max_starts,..} if max_starts.get()==1));
        assert!(matches!(&spec.resources[1],ResourceDeclaration::ProcessEndpoint{process_ordinal:0,spec} if spec.stream && !spec.datagram && spec.network_generation.0==0));
        let mut owner=PluginFactory.build(Arc::new(options),BuildContext{component:ComponentId(770),generation:ResourceGeneration(1),dependencies:Arc::from([]),roles:Default::default(),resources:grant.clone(),tasks:scope.registrar(),cancel:CancellationToken::new(),background_carrier:None}).unwrap();
        owner.lifecycle.start(deadline()).await.unwrap();
        assert_eq!(grant.starts.load(Ordering::SeqCst),1);
        let original=grant.originals.lock().unwrap()[0].clone();
        let actor=grant.actor.lock().unwrap().clone().unwrap();assert!(!actor.is_joined());assert!(original.closed().is_none());
        let endpoint=grant.endpoint.lock().unwrap().clone().unwrap();
        let reservations=endpoint.close_reservations(deadline()).await.unwrap();
        assert!(reservations.matches(&endpoint));assert_eq!(reservations.tcp_originals,1);assert_eq!(reservations.udp_originals,0);
        let probe_closed=endpoint.close_probe(deadline()).await.unwrap().expect("actual successful readiness probe was explicitly closed");assert!(probe_closed.had_native_acquisition());
        assert!(matches!(scope.registrar().spawn(zero_platform_tokio::contract::scope::TaskClass::Auxiliary,async{Ok(())}),Err(SpawnError::Capacity)));
        let user=tokio::net::TcpStream::connect(endpoint.address().unwrap()).await.unwrap();drop(user);
        let (native,tasks)=tokio::join!(owner.lifecycle.shutdown(deadline()),scope.shutdown(deadline()));
        assert!(native.unwrap().is_complete());assert!(tasks.is_complete());assert_eq!(tasks.exited_tasks.len(),1);assert!(actor.is_joined());
        assert!(original.closed().unwrap().matches(&original));
        assert!(endpoint.shutdown(deadline()).await.unwrap().unwrap().matches(&endpoint));
        assert_eq!(grant.starts.load(Ordering::SeqCst),1);
    }).await.unwrap();
}
