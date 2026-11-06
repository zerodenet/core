//! The concrete System TUN reservation uses one real bounded task registrar.
//! TCP sockets exercise real Pending I/O here; this is not native TUN qualification.
use super::*;
use crate::{
    component::ComponentRecipe,
    inbound::tun::source::{SystemTunSourceFactory, SystemTunSourceOptions},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::component::PacketDeviceBinding;
#[derive(Debug)]
struct NoNativeIo;
impl zero_traits::IoObserver for NoNativeIo {
    fn received(&self, _: usize) {
        panic!("no native TUN was acquired")
    }
    fn sent(&self, _: usize) {
        panic!("no native TUN was acquired")
    }
    fn error(&self) {
        panic!("no native TUN was acquired")
    }
    fn dropped(&self) {
        panic!("no native TUN was acquired")
    }
}
async fn socket() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (io, _) = listener.accept().await.unwrap();
    (io, remote)
}
fn pending_read(
    mut io: tokio::net::TcpStream,
) -> (
    impl Future<Output = Result<(), EngineError>> + Send + 'static,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (entered, pending) = tokio::sync::oneshot::channel();
    (
        async move {
            let mut entered = Some(entered);
            let read = io.read_u8();
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                    }
                }
                result
            })
            .await?;
            Ok(())
        },
        pending,
    )
}
#[tokio::test]
async fn system_tun_small_total_accepts_real_pending_children_and_refunds_only_actual_joins() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(
            r#"{"runtime":{"system_tun_task_limit":2},"route":{"final":{"type":"direct"}}}"#,
        )
        .unwrap(),
    )
    .unwrap();
    let limit = NonZeroUsize::new(
        proxy
            .engine()
            .config()
            .runtime
            .effective_system_tun_task_limit(10_000),
    )
    .unwrap();
    let addresses = vec![(
        "10.66.0.1".parse().unwrap(),
        "255.255.255.0".parse().unwrap(),
    )];
    let options = SystemTunSourceOptions::new(
        "budget.original".into(),
        PacketDeviceBinding::Create {
            name: None,
            addresses: addresses.clone(),
            mtu: 1500,
        },
        addresses,
        1500,
        "budget".into(),
        limit,
        Arc::new(NoNativeIo),
    )
    .unwrap();
    let metadata = options.state();
    let (stop, _) = watch::channel(false);
    let registry = proxy.inbound_owners.clone();
    let lease = registry
        .reserve_system_tun(
            &proxy.protocols,
            ComponentRecipe::from_validated(Arc::new(SystemTunSourceFactory), options),
            stop,
        )
        .unwrap();
    let owner = lease.owner();
    let group = owner
        .install_child_group(proxy.protocols.clone(), NonZeroUsize::new(4096).unwrap())
        .unwrap();
    assert_eq!(
        group.declared_limit(),
        4096,
        "old child ceiling is retained"
    );
    let tasks = owner.task_access();
    let (main_io, mut main_remote) = socket().await;
    let (main_work, main_pending) = pending_read(main_io);
    owner.spawn_driver(main_work).unwrap();
    timeout(Duration::from_secs(2), main_pending)
        .await
        .unwrap()
        .unwrap();
    let (child_io, mut child_remote) = socket().await;
    let (child_work, child_pending) = pending_read(child_io);
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    assert!(group
        .spawn_bound(&tasks, |binding| {
            binding_tx.send(binding.clone()).unwrap();
            async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                child_work.await
            }
        })
        .unwrap());
    let binding = binding_rx.await.unwrap();
    let original = binding.wait_bound().await.unwrap();
    timeout(Duration::from_secs(2), child_pending)
        .await
        .unwrap()
        .unwrap();
    let failure = group
        .spawn_bound(&tasks, |_| async {
            panic!("rejected task must never be polled")
        })
        .unwrap_err();
    assert!(format!("{failure:?}").contains("Capacity"), "{failure:?}");
    assert_eq!(
        group.occupied(),
        1,
        "clean rejection retained no accepted TaskId"
    );
    assert!(!owner.cancel.is_cancelled());
    child_remote.write_u8(1).await.unwrap();
    // Native completion/body return alone cannot release registrar capacity.
    assert!(matches!(
        owner.registrar.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(zero_platform_tokio::contract::scope::SpawnError::Capacity)
    ));
    let exit = timeout(Duration::from_secs(2), owner.scope.lock().await.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit.id, original);
    assert!(!owner.record(exit));
    let joined = binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(joined.task(), original);
    assert_eq!(group.occupied(), 0);
    let (next_io, mut next_remote) = socket().await;
    let (next_work, next_pending) = pending_read(next_io);
    assert!(
        group.spawn_bound(&tasks, |_| next_work).unwrap(),
        "same live group accepts after real refund"
    );
    timeout(Duration::from_secs(2), next_pending)
        .await
        .unwrap()
        .unwrap();
    next_remote.write_u8(1).await.unwrap();
    let exit = timeout(Duration::from_secs(2), owner.scope.lock().await.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(exit.id, original);
    assert!(!owner.record(exit));
    main_remote.write_u8(1).await.unwrap();
    let exit = timeout(Duration::from_secs(2), owner.scope.lock().await.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(&exit.id), owner.driver.get());
    assert!(!owner.record(exit));
    assert!(metadata.driver().is_none());
    assert!(metadata.original().is_none());
    drop(lease); // Seal/stop only; no join or native-close proof is taken from Drop.
    let receipt = registry
        .confirm_shutdown(
            crate::runtime::inbound_owners::system_tun::SYSTEM_TUN_OWNER,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().identity(), owner.identity());
    assert_eq!(receipt.terminal().joined_tasks(), 3);
    assert_eq!(receipt.terminal().closed_io_receipts(), 0);
    registry.release_closed(receipt, |_| Ok(())).unwrap();
}
