use super::*;
#[tokio::test]
async fn timeout_after_actual_pending_read_retains_elapsed_source_and_unknown_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let connector = RelayStreamConnector::new_service_bound(
        "pending-deadline".into(),
        binding(),
        (endpoint.ip().to_string(), endpoint.port()),
        Arc::new(move |host, port, _| {
            Box::pin(async move {
                let mut stream = TcpStream::connect((host.as_str(), port)).await?;
                stream.write_all(&[1]).await?;
                stream.read_u8().await?;
                Ok(TcpRelayStream::new(stream))
            })
        }),
    );
    let mut context = demand();
    context.deadline = Some(Instant::now() + Duration::from_secs(2));
    let cancel = context.cancel.clone();
    let mut work = connector.connect_with_context(
        &binding(),
        endpoint.ip().to_string(),
        endpoint.port(),
        context,
    );
    let (mut peer, _) = tokio::select! {
        result=listener.accept()=>result.unwrap(),
        result=&mut work=>panic!("carrier ended before actual peer: {}",result.err().unwrap()),
    };
    tokio::select! {
        result=peer.read_u8()=>assert_eq!(result.unwrap(),1),
        result=&mut work=>panic!("carrier ended before actual pending read: {}",result.err().unwrap()),
    }
    std::future::poll_fn(|cx| {
        assert!(work.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let error = work.await.err().unwrap();
    let RuntimeError::Io(outer) = error else {
        panic!("deadline class changed")
    };
    assert_eq!(outer.kind(), std::io::ErrorKind::TimedOut);
    let classified = outer
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    assert_eq!(classified.delivery, DeliveryState::Unknown);
    assert_eq!(classified.reason, Reason::Timeout);
    assert!(classified
        .source()
        .unwrap()
        .downcast_ref::<tokio::time::error::Elapsed>()
        .is_some());
    assert!(
        !cancel.is_cancelled(),
        "deadline does not mutate the owner's cancellation token"
    );
}
