use super::PendingEvents;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
#[tokio::test]
async fn disconnected_native_subscriber_reaps_the_actual_job_without_a_new_rpc_or_server_stop() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut limits = crate::budget::defaults().limits();
        limits.subscriptions = 1;
        let budget = zero_runtime::configuration::ControlBudget::new(limits);
        let dropped = Arc::new(AtomicUsize::new(0));
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            crate::GrpcServerSecurity::default(),
            budget.clone(),
        )
        .unwrap();
        owner.start(PendingEvents(dropped.clone())).await.unwrap();
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
                    event_types: Vec::new(),
                }),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/zero.api.v1.Control/Subscribe",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        let worker = owner.originals.subscriptions.jobs.lock().unwrap()[0].clone();
        assert_eq!(budget.usage().subscriptions, 1);
        drop(response);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let changed = budget.capacity_changed();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if budget.usage().subscriptions == 0 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
        assert!(
            worker.joined_successfully(),
            "body-exit notification is followed by the same actual JoinHandle result"
        );
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        assert_eq!(
            budget.usage().connections,
            1,
            "the actual original connection stays open throughout idle cleanup"
        );
        assert_eq!(
            owner
                .originals
                .original
                .lock()
                .await
                .confirmed_close_count(),
            0
        );
        assert!(!owner.retirement_complete());
        client.ready().await.unwrap();
        let second: tonic::Response<tonic::codec::Streaming<crate::PbEvent>> = client
            .server_streaming(
                tonic::Request::new(crate::SubscribeRequest {
                    event_types: Vec::new(),
                }),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/zero.api.v1.Control/Subscribe",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        assert_eq!(budget.usage().subscriptions, 1);
        drop(second);
        drop(client);
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete());
        assert_eq!(dropped.load(Ordering::Acquire), 2);
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
