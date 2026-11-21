use super::*;
#[tokio::test]
async fn standard_grpc_keeps_multiple_real_streams_in_the_original_h2_scope() {
    timeout(Duration::from_secs(10), async {
        let mut host = fixture::Host::new(true).await;
        let (socket, accepted) = host.accept().await;
        let (request, context) = accepted.await.unwrap();
        let AcceptedRequest::Multiplex(mut source) = request else {
            panic!("gRPC source")
        };
        let profile = OwnedGrpcProfile {
            service_names: vec!["zero.vless/Tun".into()],
            ..Default::default()
        };
        let pool = zero_transport::grpc::GrpcPool::default();
        let mut first = pool
            .open(&profile, "localhost", || async {
                Ok::<_, RuntimeError>(socket)
            })
            .await
            .unwrap();
        let mut second = pool
            .open::<TcpStream, _, _>(&profile, "localhost", || async {
                Err(std::io::Error::other("must reuse actual H2 connection").into())
            })
            .await
            .unwrap();
        for (client, expected) in [(&mut first, b"one"), (&mut second, b"two")] {
            client.write_all(&header(1)).await.unwrap();
            client.write_all(expected).await.unwrap();
            client.flush().await.unwrap();
            let pending = source
                .next(AcceptContext {
                    deadline: None,
                    cancel: host.stop.clone(),
                })
                .await
                .unwrap()
                .unwrap();
            let AcceptedRequest::Stream(request) = pending.complete(context.clone()).await.unwrap()
            else {
                panic!("VLESS stream")
            };
            let mut stream = request
                .respond(ReplyOutcome::LocalChannelReady)
                .await
                .unwrap()
                .unwrap();
            let mut response = [0; 2];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(response, [0, 0]);
            let mut payload = [0; 3];
            stream.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, expected);
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
            client.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, expected);
            // Finish both actual logical halves before retiring their shared
            // H2 Source. Dropping live halves sends resets instead of EOF.
            client.shutdown().await.unwrap();
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
            stream.shutdown().await.unwrap();
            assert_eq!(client.read(&mut [0; 1]).await.unwrap(), 0);
            drop(stream);
        }
        drop(first);
        drop(second);
        // Request graceful H2 admission stop while its real client transport
        // is still alive. Retain the exact original driver exit before native
        // cancellation; Source exhaustion alone is not a driver join.
        let original_caller = context
            .accepted
            .as_ref()
            .unwrap()
            .task_join_witness()
            .unwrap();
        drop(source);
        host.receive_original_task().await;
        assert!(
            !original_caller.is_joined(),
            "the same original caller stays live while its H2 child drains"
        );
        pool.retire();
        host.close(2).await;
    })
    .await
    .unwrap();
}
