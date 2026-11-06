//! Actual same-pool generation transition, including all original ACKs.
use super::*;
#[tokio::test]
async fn background_cold_caps_replace_only_after_same_pool_native_users_and_actual_scope_ack() {
    timeout(Duration::from_secs(12),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();
        let (finish,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let mut tasks=tokio::task::JoinSet::new();
            for _ in 0..2 {
                let (io,_)=listener.accept().await.unwrap();
                tasks.spawn(async move {
                    let incoming=crate::grpc::accept_grpc_connection(io,&crate::profile::OwnedGrpcProfile{service_names:vec!["owned".into()],..Default::default()}).unwrap();
                    let mut stream=incoming.accept().await.unwrap();
                    let mut byte=[0;1];while let Ok(1)=stream.read(&mut byte).await {stream.write_all(&byte).await.unwrap();stream.flush().await.unwrap();}
                    incoming.close();
                });
            }
            finished.await.unwrap();while let Some(task)=tasks.join_next().await{task.unwrap();}
        });
        let pool=GrpcPool::default();
        let first_instance=ComponentInstanceId{component:ComponentId(940),generation:ResourceGeneration(1)};
        let next_instance=ComponentInstanceId{generation:ResourceGeneration(2),..first_instance};
        let mut old=Fixture::background(pool.clone(),peer,1,2,first_instance);old.start().await;
        let mut next=Fixture::background(pool.clone(),peer,2,3,next_instance);
        assert_ne!(old.options.identity,next.options.identity);
        assert_eq!(next.traffic.boundaries.load(Ordering::SeqCst),0);
        let mut rejected=Fixture::background(pool.clone(),peer,2,3,next_instance);
        assert_eq!(rejected.owner.lifecycle.start(deadline()).await.unwrap_err().reason,Reason::StalePlan);
        assert_eq!(rejected.traffic.boundaries.load(Ordering::SeqCst),0);
        // The first real initializer consumes its own N=1 Scope credit.
        let options=old.options.clone();let demand=old.demand();let mut opening=Box::pin(options.open(old.actual,demand));
        let prepared=tokio::select!{exit=old.scope.next_exit()=>exit.unwrap(),result=&mut opening=>panic!("actual auxiliary must precede driver: {}",result.is_ok())};
        assert!(prepared.result.is_ok());
        let mut stream=opening.await.unwrap();stream.write_all(b"a").await.unwrap();stream.flush().await.unwrap();let mut byte=[0;1];stream.read_exact(&mut byte).await.unwrap();assert_eq!(byte,*b"a");
        let managed=old.managed();let weak=Arc::downgrade(&managed);drop(managed);
        assert!(!pool.compact_closed_owner(),"a live original cannot be compacted");
        let physical=old.managed().records.lock().unwrap()[0].clone();let witness=physical.witness.lock().unwrap().clone().unwrap();
        assert!(pool.has_host_owner());assert!(physical.adoption.closed().is_none());assert!(pool.validate_mode_candidate(false,true).is_err());
        assert_eq!(next.traffic.boundaries.load(Ordering::SeqCst),0);
        drop(stream);
        let (native,tasks)=old.close().await;assert!(native.is_complete()&&tasks.is_complete());
        let joined=witness.wait_registration_released().await;assert!(witness.matches_joined(&joined)&&joined.registration_released());
        assert!(physical.closed());assert!(physical.adoption.closed().unwrap().matches(&physical.adoption));assert!(physical.adoption.users_closed().unwrap().matches(&physical.adoption));
        assert!(pool.compact_closed_owner());
        // Only removal after real matching closure/ACK releases the heavy
        // protocol owner. This Weak assertion is memory evidence, not closure.
        drop(old);assert!(weak.upgrade().is_none());
        assert!(pool.validate_mode_candidate(false,true).is_ok());next.start().await;assert_eq!(next.traffic.boundaries.load(Ordering::SeqCst),0);
        let options=next.options.clone();let demand=next.demand();let mut opening=Box::pin(options.open(next.actual,demand));
        let prepared=tokio::select!{exit=next.scope.next_exit()=>exit.unwrap(),result=&mut opening=>panic!("same real setup owner: {}",result.is_ok())};assert!(prepared.result.is_ok());
        let mut stream=opening.await.unwrap();stream.write_all(b"b").await.unwrap();stream.flush().await.unwrap();stream.read_exact(&mut byte).await.unwrap();assert_eq!(byte,*b"b");
        assert_eq!(next.traffic.boundaries.load(Ordering::SeqCst),1);drop(stream);let (native,tasks)=next.close().await;assert!(native.is_complete()&&tasks.is_complete());
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn background_recipe_caps_are_immutable_and_domain_uses_the_actual_node_endpoint() {
    let pool = GrpcPool::default();
    let settings = NativePoolSettings {
        server: "node.example".into(),
        port: 443,
        grpc: crate::profile::OwnedGrpcProfile {
            service_names: vec!["owned".into()],
            ..Default::default()
        },
        tls: None,
        dial: Default::default(),
    };
    let first = NativePoolOptions::for_background_pool(
        &pool,
        settings.clone(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap();
    let same = NativePoolOptions::for_background_pool(
        &pool,
        settings.clone(),
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(first.identity, same.identity);
    assert_eq!(
        first.endpoint(),
        &Destination::new(zero_core::Address::Domain("node.example".into()), 443)
    );
    let next = NativePoolOptions::for_background_pool(
        &pool,
        settings.clone(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(3).unwrap(),
    )
    .unwrap();
    assert_ne!(first.identity, next.identity);
    assert_eq!(first.carrier_limit.get(), 1);
    assert_eq!(next.carrier_limit.get(), 2);
    assert!(matches!(
        *pool.0.ownership.lock().unwrap(),
        super::super::state::Ownership::Pristine
    ));
    assert!(pool.0.cached.lock().unwrap().is_none());
    assert!(NativePoolOptions::from_pool(
        &pool,
        settings,
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(2).unwrap()
    )
    .is_err());
}
