use super::*;
#[tokio::test]
async fn native_subscription_consumes_the_same_actual_engine_original_and_joins_before_refund() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let engine = engine();
        let budget = crate::budget::defaults();
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            Default::default(),
            budget.clone(),
        )
        .unwrap();
        // ViewOnly.subscribe panics: production must select the supplied Engine.
        owner
            .start_with_engine(ViewOnly, engine.clone())
            .await
            .unwrap();
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://{}", owner.local_addr()))
                .unwrap()
                .connect()
                .await
                .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let response: tonic::Response<tonic::codec::Streaming<crate::PbEvent>> = client
            .server_streaming(
                tonic::Request::new(crate::SubscribeRequest {
                    event_types: vec![zero_api::event_type::STATS_SCOPES_SAMPLED.into()],
                }),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/zero.api.v1.Control/Subscribe",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        let worker = owner.originals.subscriptions.jobs.lock().unwrap()[0].clone();
        engine.inner().push_stats_sampled();
        let mut stream = response.into_inner();
        let actual = stream.message().await.unwrap().unwrap();
        assert_eq!(
            actual.event_type,
            zero_api::event_type::STATS_SCOPES_SAMPLED
        );
        let _: serde_json::Value = serde_json::from_slice(&actual.payload).unwrap();
        assert_eq!(budget.usage().subscriptions, 1);
        drop(stream);
        loop {
            let changed = budget.capacity_changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if budget.usage().subscriptions == 0 {
                break;
            }
            changed.await;
        }
        assert!(worker.joined_successfully());
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
