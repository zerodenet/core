use super::*;
#[tokio::test]
async fn real_prost_repeated_string_input_refuses_before_handler_then_same_book_growth_recovers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut limits = budget().limits();
        limits.messages.request_bytes_total = 32 * 1024;
        let book = ControlBudget::new(limits);
        let service = Commands::default();
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            GrpcServerSecurity::default(),
            book.clone(),
        )
        .unwrap();
        owner.start(service).await.unwrap();
        let mut client = client(&owner).await;
        let request = || {
            tonic::Request::new(crate::SubscribeRequest {
                event_types: vec![String::new(); 1024],
            })
        };
        let result: Result<tonic::Response<tonic::codec::Streaming<crate::PbEvent>>, _> = client
            .server_streaming(
                request(),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/zero.api.v1.Control/Subscribe",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await;
        assert_eq!(result.err().unwrap().code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            book.usage().subscriptions,
            0,
            "original repeated DTO is rejected before subscribing"
        );
        let mut target = book.limits();
        target.messages.request_bytes_total = 512 * 1024;
        book.prepare(target).unwrap().commit().unwrap();
        let response: tonic::Response<tonic::codec::Streaming<crate::PbEvent>> = client
            .server_streaming(
                request(),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/zero.api.v1.Control/Subscribe",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        assert_eq!(book.usage().subscriptions, 1);
        assert!(book.usage().request_bytes > 32 * 1024);
        let worker = owner.originals.subscriptions.jobs.lock().unwrap()[0].clone();
        drop(response);
        drop(client);
        owner.shutdown().await.unwrap();
        assert!(worker.joined_successfully());
        assert!(owner.retirement_complete());
        assert_eq!(book.usage().request_bytes, 0);
        book.close_admission();
        book.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
