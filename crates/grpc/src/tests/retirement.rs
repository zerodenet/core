use super::*;
use crate::{budget, pb::SubscribeRequest, GrpcServerSecurity};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
#[tokio::test]
async fn actual_retired_grpc_server_fault_keeps_its_source_and_allows_a_new_owner_on_the_same_endpoint(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let original_budget = budget::defaults();
        let old = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            GrpcServerSecurity::default(),
            original_budget.clone(),
        )
        .unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        old.start(PendingEvents(dropped.clone())).await.unwrap();
        let address = old.local_addr();
        original_budget.close_admission();
        let pending = tokio::net::TcpStream::connect(address).await.unwrap();
        let fault = old.wait_terminated().await.unwrap_err();
        assert!(!old.retirement_complete());
        let retired = old.shutdown().await.unwrap_err();
        assert!(old.retirement_complete());
        let prior = fault
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        let actual = retired
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(Arc::ptr_eq(prior, actual));
        {
            let mut original = old.originals.original.lock().await;
            assert!(original
                .wait_closed(crate::native::deadline())
                .await
                .unwrap()
                .is_complete());
            assert_eq!(
                original.confirmed_close_count(),
                1,
                "the same native listener was actually closed"
            );
            assert!(!original.native_unconfirmed());
        }
        original_budget.confirm_released().unwrap();
        drop(pending);
        let fresh_budget = budget::defaults();
        let fresh = crate::prepare_with_budget(
            address,
            GrpcServerSecurity::default(),
            fresh_budget.clone(),
        )
        .unwrap();
        fresh.start(PendingEvents(dropped.clone())).await.unwrap();
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let response: tonic::Response<tonic::codec::Streaming<crate::PbEvent>> = client
            .server_streaming(
                tonic::Request::new(SubscribeRequest {
                    event_types: Vec::new(),
                }),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/zero.api.v1.Control/Subscribe",
                ),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        assert_eq!(fresh_budget.usage().subscriptions, 1);
        assert!(!fresh.retirement_complete());
        drop(response);
        drop(client);
        fresh.shutdown().await.unwrap();
        assert!(fresh.retirement_complete());
        assert_eq!(dropped.load(Ordering::Acquire), 1);
        fresh_budget.close_admission();
        fresh_budget.confirm_released().unwrap();
        assert_eq!(
            fresh
                .originals
                .original
                .lock()
                .await
                .confirmed_close_count(),
            1
        );
    })
    .await
    .unwrap();
}
