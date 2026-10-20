//! Actual Scope caller ownership and real refused/healthy native TCP flows.
use super::*;
use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
};

#[tokio::test]
async fn real_refused_business_flow_retains_its_caller_source_without_stopping_the_next_wire_flow()
{
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("local-business", &proxy.protocols, 2, stop)
        .unwrap();
    let group = installed
        .child_tasks(NonZeroUsize::new(2).unwrap())
        .unwrap();
    let host = installed.tasks();
    let owner = installed.owner();
    let native = NativeObligations::new(2, 0, 0);
    installed.register(native.resource()).unwrap();
    let (stop_parent, parent_stopped) = oneshot::channel();
    owner
        .spawn_driver(async move {
            parent_stopped.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    let monitor = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.observe_driver().await })
    };
    // Reserve the exact real TCP port without listening. The OS refuses
    // connect while this socket prevents another fixture stealing that port.
    let refusing = TcpSocket::new_v4().unwrap();
    refusing.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let refused_address = refusing.local_addr().unwrap();
    let mut failed = native.reserve(ChildKind::Tcp).unwrap();
    let failed_registration = failed.registration();
    let (binding_tx, binding_rx) = oneshot::channel();
    group.spawn_bound(&host, move |binding| async move {
        let task = binding.wait_bound().await.map_err(std::io::Error::other)?;
        failed.bind_caller(binding.clone()).map_err(std::io::Error::other)?;
        failed.enter().map_err(std::io::Error::other)?;
        binding_tx.send((task, binding)).unwrap();
        let result = TcpStream::connect(refused_address).await.map(|_| ()).map_err(zero_engine::EngineError::Io);
        assert!(matches!(&result, Err(zero_engine::EngineError::Io(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused));
        failed.consume_business_result(result)
    }).unwrap();
    let (failed_task, failed_binding) = binding_rx.await.unwrap();
    let failed_join = failed_binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(failed_join.task(), failed_task);
    assert_eq!(
        failed_join.class(),
        zero_platform_tokio::contract::scope::TaskClass::Critical
    );
    assert!(
        failed_join.failure().is_none(),
        "the owning business receiver consumed this ordinary result"
    );
    let source = {
        let state = native.0.lock().unwrap();
        assert!(
            state.first_source.is_none(),
            "ordinary business errors are not native owner faults"
        );
        let record = state.records.get(&failed_registration.id).unwrap();
        assert!(Arc::ptr_eq(
            record.caller.as_ref().unwrap(),
            &failed_binding
        ));
        assert!(record.entered && record.retiring);
        record.business_failure.clone().unwrap()
    };
    let zero_engine::EngineError::Io(os) = source.as_ref() else {
        panic!("original typed native error");
    };
    assert_eq!(os.kind(), std::io::ErrorKind::ConnectionRefused);
    assert!(os.raw_os_error().is_some());
    let actual = failed_registration
        .caller_witness()
        .unwrap()
        .try_joined()
        .unwrap();
    assert_eq!(actual.id(), failed_task);
    assert_eq!(actual.class(), failed_join.class());
    assert!(actual.result().is_ok());
    assert!(failed_binding.cancellation().is_cancelled());
    assert!(!host.cancellation().is_cancelled());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let echo = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut input = Vec::new();
        io.read_to_end(&mut input).await.unwrap();
        assert_eq!(input, b"same original listener, next flow");
        io.write_all(b"reverse after half close").await.unwrap();
        io.shutdown().await.unwrap();
    });
    let mut healthy = native.reserve(ChildKind::Tcp).unwrap();
    let healthy_registration = healthy.registration();
    let (next_tx, next_rx) = oneshot::channel();
    group
        .spawn_bound(&host, move |binding| async move {
            let task = binding.wait_bound().await.map_err(std::io::Error::other)?;
            healthy
                .bind_caller(binding.clone())
                .map_err(std::io::Error::other)?;
            healthy.enter().map_err(std::io::Error::other)?;
            next_tx.send((task, binding)).unwrap();
            let result = async {
                let mut io = TcpStream::connect(address).await?;
                io.write_all(b"same original listener, next flow").await?;
                io.shutdown().await?;
                let mut reply = Vec::new();
                io.read_to_end(&mut reply).await?;
                assert_eq!(reply, b"reverse after half close");
                Ok(())
            }
            .await;
            healthy.consume_business_result(result)
        })
        .unwrap();
    let (healthy_task, healthy_binding) = next_rx.await.unwrap();
    let healthy_join = healthy_binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_ne!(healthy_task, failed_task);
    assert!(healthy_join.failure().is_none());
    assert_eq!(
        healthy_registration
            .caller_witness()
            .unwrap()
            .try_joined()
            .unwrap()
            .id(),
        healthy_task
    );
    timeout(Duration::from_secs(2), echo)
        .await
        .unwrap()
        .unwrap();
    assert!(!host.cancellation().is_cancelled());
    assert!(Arc::ptr_eq(
        &source,
        native
            .0
            .lock()
            .unwrap()
            .records
            .get(&failed_registration.id)
            .unwrap()
            .business_failure
            .as_ref()
            .unwrap()
    ));
    stop_parent.send(()).unwrap();
    timeout(Duration::from_secs(2), monitor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Real task joins/ordinary result handling are not explicit native close.
    assert!(registry
        .confirm_shutdown("local-business", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert_eq!(native.0.lock().unwrap().records.len(), 2);
    assert!(native.0.lock().unwrap().first_source.is_none());
    let mut inspecting = native.clone();
    assert!(!inspecting
        .shutdown(Instant::now())
        .await
        .unwrap()
        .is_complete());
    assert!(owner.terminal_for_test().is_none());
}
