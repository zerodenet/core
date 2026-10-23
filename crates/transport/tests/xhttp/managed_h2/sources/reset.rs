use super::*;

#[tokio::test]
async fn stream_reset_keeps_its_original_hyper_h2_error_and_sibling_succeeds_on_the_same_connection(
) {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let cancel = CancellationToken::new();
        let mut pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(8, cancel.clone()),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            let (_, mut first) = h2.accept().await.unwrap().unwrap();
            first.send_reset(h2::Reason::REFUSED_STREAM);
            let (_, mut second) = h2.accept().await.unwrap().unwrap();
            let mut response = second
                .send_response(http::Response::new(()), false)
                .unwrap();
            response
                .send_data(Bytes::from_static(b"same physical sibling"), true)
                .unwrap();
            while let Some(result) = h2.accept().await {
                if result.is_err() {
                    break;
                }
            }
        });
        let mut sender = pending.ready().await.unwrap();
        let reset = match sender.send_request(request(b"")).await {
            Err(source) => source,
            Ok(_) => panic!("real RST_STREAM must fail only its request"),
        };
        assert_eq!(reset.reason, Reason::Protocol);
        assert_eq!(reset.impact, Impact::Session);
        let local = reset
            .source()
            .unwrap()
            .downcast_ref::<H2ConnectionFailure>()
            .unwrap();
        let hyper = local
            .hyper_source()
            .expect("the actual typed Hyper request error");
        let h2 = hyper.source().unwrap().downcast_ref::<h2::Error>().unwrap();
        assert!(h2.is_reset());
        assert_eq!(h2.reason(), Some(h2::Reason::REFUSED_STREAM));
        assert!(std::ptr::eq(h2, local.h2_source().unwrap()));
        assert!(std::ptr::eq(
            hyper.as_ref(),
            local
                .source()
                .unwrap()
                .downcast_ref::<hyper::Error>()
                .unwrap()
        ));
        assert!(local.io_source().is_none());
        assert!(pending.failure_evidence().io_source().is_none());
        assert!(
            pending.failure_evidence().h2_source().is_none(),
            "normal stream reset never invokes the fatal ConnMapErr observer"
        );
        assert!(
            pending.failure_evidence().hyper_source().is_none(),
            "request-local error is not a whole-carrier failure"
        );
        sender.ready().await.unwrap();
        let response = sender.send_request(request(b"")).await.unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"same physical sibling"[..]
        );
        assert_eq!(fixture.owner.retained(), 1);
        cancel.cancel();
        finish(&mut fixture, &mut scope, &adoption, pending.task_id()).await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}
