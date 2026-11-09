//! The actual raw binding receives helper errors without making them fatal.
use super::*;

#[tokio::test]
async fn raw_metadata_helper_uses_same_paid_receiver_and_unique_ordinary_scope_after_read_failure()
{
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let instance = proxy.protocols.allocate_resource_identity().unwrap();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    let registrar = scope.registrar();
    let originals = proxy.protocols.runtime_host().original_budgets();
    let root =
        zero_platform_tokio::controlled_carrier::CarrierOriginalTotals::new(NonZeroUsize::MIN);
    let book = zero_platform_tokio::AcceptedTaskFailures::prepare(
        instance,
        &registrar,
        &originals.packet.driver_metadata_view(),
    )
    .unwrap();
    let binding = Arc::new(HostPeerBinding::new(
        instance,
        CancellationToken::new(),
        registrar.downgrade(),
        Arc::downgrade(&root),
    ));
    let mut reserved = book.reserve().unwrap();
    reserved.begin_registration();
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (id, caller) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            let read = reader.read_u8();
            tokio::pin!(read);
            let mut entered = Some(entered);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .map_err(io_error)?;
            Ok(())
        })
        .unwrap();
    binding.install_task_failure(reserved.registered(&caller).unwrap());
    binding.bind(id, caller.clone());
    pending.await.unwrap();
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client = std::net::TcpStream::connect(tcp.local_addr().unwrap()).unwrap();
    let (_server, _) = tcp.accept().unwrap();
    let source = client.local_addr().unwrap();
    for count in 1..=2 {
        let error = binding
            .lookup_local_process(source, LocalProcessTransport::Tcp, NonZeroUsize::MIN)
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Capacity);
        let original = binding.process.lock().unwrap().clone().unwrap();
        let witness = original.witness().unwrap();
        let event = scope.next_exit().await.unwrap();
        assert_eq!(event.id, witness.task_id().unwrap());
        let joined = witness.wait().await;
        assert!(joined.registration_released());
        assert!(!joined.failure_accepted());
        assert_eq!(root.snapshot().charged, 1);
        // This is precisely the consumer called by Source.record_scoped.
        binding
            .receive_process_exit(&scope, &event)
            .unwrap()
            .unwrap();
        assert!(joined.failure_accepted());
        assert_eq!(book.len(), count);
        assert!(book.iter().any(|actual| actual.matches_joined(&joined)));
        assert!(root.failures().is_empty());
        original
            .wait_retired(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(root.snapshot().charged, 0);
        assert!(!caller.is_joined());
    }
    let info = binding
        .lookup_local_process(
            source,
            LocalProcessTransport::Tcp,
            NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.pid, std::process::id());
    let event = scope.next_exit().await.unwrap();
    event.result.as_ref().unwrap();
    binding
        .receive_process_exit(&scope, &event)
        .unwrap()
        .unwrap();
    assert!(binding.process_retired());
    assert_eq!(root.snapshot().charged, 0);
    assert_eq!(
        book.len(),
        2,
        "both independent original failures remain received"
    );
    writer.write_u8(1).await.unwrap();
    let event = scope.next_exit().await.unwrap();
    assert_eq!(event.id, id);
    binding
        .receive_primary_joined(id)
        .unwrap()
        .result()
        .unwrap();
    assert!(scope.shutdown(Instant::now()).await.is_complete());
}
