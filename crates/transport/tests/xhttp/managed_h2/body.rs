use super::*;
use crate::split_http::body::Body;

#[tokio::test]
async fn delegate_rejection_wakes_an_already_pending_response_body_with_the_same_first_arc() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        // ConnTask plus one Pipe/Send. The second original request uses an
        // actually pending upload body, so Pipe fills the remaining slot and
        // Send is rejected by the shared active+queued budget.
        let mut pending = spawn::<Body>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(2, CancellationToken::new()),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            let (_, mut respond) = h2.accept().await.unwrap().unwrap();
            let held = respond
                .send_response(http::Response::new(()), false)
                .unwrap();
            // Keep the genuine first response stream open while the second
            // demand provokes the real executor rejection.
            while let Some(result) = h2.accept().await {
                if result.is_err() {
                    break;
                }
            }
            drop(held);
        });
        let mut sender = pending.ready().await.unwrap();
        let mut other = sender.clone();
        let response = sender
            .send_request(
                http::Request::builder()
                    .uri("https://fixture.test/first")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut body = response.into_body();
        let mut reading = Box::pin(body.frame());
        pending_once(reading.as_mut()).await;
        let (keep_upload_pending, receiver) = tokio::sync::mpsc::channel(1);
        let upload = Body {
            receiver,
            task: None,
            life: None,
            progress: None,
        };
        let mut sending = Box::pin(
            other.send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://fixture.test/second")
                    .body(upload)
                    .unwrap(),
            ),
        );
        pending_once(sending.as_mut()).await;
        let (request, frame) = tokio::join!(sending, reading);
        let source = match request {
            Err(source) => source,
            Ok(_) => panic!("finite delegate budget must reject"),
        };
        assert_eq!(source.reason, Reason::Capacity);
        let body_source = frame
            .expect("pending body receives the connection failure")
            .unwrap_err();
        assert!(Arc::ptr_eq(&source, &body_source));
        assert!(Arc::ptr_eq(&source, &sender.ready().await.unwrap_err()));
        drop(keep_upload_pending);
        assert_eq!(scope.next_exit().await.unwrap().id, pending.task_id());
        assert!(adoption.closed().is_none());
        assert!(adoption.retire(end()).await.unwrap().is_complete());
        server.await.unwrap();
        assert!(scope.shutdown(end()).await.is_complete());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}
