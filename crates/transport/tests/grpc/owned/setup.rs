//! N=1 proves initializer completion is consumed before driver registration.
use super::*;
#[tokio::test]
async fn real_scoped_preparation_returns_same_task_credit_before_the_original_h2_driver() {
    timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming = crate::grpc::accept_grpc_connection(io,
                &crate::profile::OwnedGrpcProfile {service_names:vec!["owned".into()],..Default::default()}
            ).unwrap();
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let mut stream = incoming.accept().await.unwrap();
                workers.spawn(async move {
                    let mut byte = [0;1];
                    while let Ok(1) = stream.read(&mut byte).await {
                        stream.write_all(&byte).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                });
            }
            finished.await.unwrap();incoming.close();
            while let Some(result) = workers.join_next().await {result.unwrap();}
        });
        let mut fixture = Fixture::scoped(peer,2);
        fixture.start().await;
        let options = fixture.options.clone();let actual=fixture.actual;let demand=fixture.demand();
        let mut opening = Box::pin(options.open(actual,demand));
        let prepared = tokio::select! {
            exit=fixture.scope.next_exit()=>exit.expect("the actual initializer exit"),
            result=&mut opening=>panic!("driver must await the actual Scope consumption: {}",result.is_ok()),
        };
        assert!(prepared.result.is_ok());
        assert_eq!(prepared.class,zero_platform_tokio::contract::scope::TaskClass::Auxiliary);
        let mut first=opening.await.unwrap();
        first.write_all(b"1").await.unwrap();first.flush().await.unwrap();
        let mut byte=[0;1];first.read_exact(&mut byte).await.unwrap();assert_eq!(byte,*b"1");
        let managed=fixture.managed();let record=managed.records.lock().unwrap()[0].clone();
        let witness=record.witness.lock().unwrap().clone().unwrap();
        assert_ne!(prepared.id,witness.task_id().expect("actual H2 driver has its registered TaskId"));
        assert!(!witness.is_joined());assert!(record.adoption.closed().is_none());
        let mut second=fixture.options.open(fixture.actual,fixture.demand()).await.unwrap();
        second.write_all(b"2").await.unwrap();second.flush().await.unwrap();second.read_exact(&mut byte).await.unwrap();assert_eq!(byte,*b"2");
        assert_eq!(managed.records.lock().unwrap().len(),1);
        assert!(Arc::ptr_eq(&first.life.physical,&second.life.physical));
        drop(first);drop(second);
        let (native,tasks)=fixture.close().await;
        assert!(native.is_complete()&&tasks.is_complete());
        assert!(record.closed());
        let joined=witness.wait_registration_released().await;
        assert!(witness.matches_joined(&joined)&&joined.registration_released());
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}
