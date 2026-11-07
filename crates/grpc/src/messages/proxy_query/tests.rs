use std::time::Duration;
use zero_api::{CommandService, EventSource, QueryService};
#[derive(Clone)]
struct NoDto;
impl QueryService for NoDto {
    fn query(&self, _: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        panic!("actual Proxy production view must not query a copied DTO")
    }
}
impl CommandService for NoDto {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!("query-only fixture")
    }
}
impl EventSource for NoDto {
    type Stream = <zero_engine::EngineHandle as EventSource>::Stream;
    fn subscribe(&self, _: zero_api::EventFilter) -> zero_api::ApiResult<Self::Stream> {
        unreachable!()
    }
    fn latest(
        &self,
        _: usize,
        _: zero_api::EventFilter,
    ) -> zero_api::ApiResult<Vec<zero_api::RawApiEvent>> {
        unreachable!()
    }
    fn since(
        &self,
        _: u64,
        _: usize,
        _: zero_api::EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        unreachable!()
    }
}
#[tokio::test]
async fn native_proxy_query_uses_same_original_status_and_recovers_from_output_budget_refusal() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let proxy = zero_proxy::Proxy::new(
            serde_json::from_value(serde_json::json!({
                "route":{"final":{"type":"direct"}}
            }))
            .unwrap(),
        )
        .unwrap();
        let handle = zero_proxy::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        let request = zero_api::QueryRequest::TunStatus(Default::default());
        let expected = serde_json::to_value(handle.query(request.clone()).unwrap()).unwrap();
        let book = crate::budget::defaults();
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            Default::default(),
            book.clone(),
        )
        .unwrap();
        owner.start_with_proxy(NoDto, handle.clone()).await.unwrap();
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://{}", owner.local_addr()))
                .unwrap()
                .connect()
                .await
                .unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        for total in [128 * 1024, 16 * 1024, 128 * 1024] {
            while book.usage().grpc_requests != 0 {
                tokio::task::yield_now().await;
            }
            let mut limits = book.limits();
            limits.messages.response_bytes_total = total;
            book.prepare(limits).unwrap().commit().unwrap();
            client.ready().await.unwrap();
            let response: Result<tonic::Response<crate::QueryResponse>, tonic::Status> = client
                .unary(
                    tonic::Request::new(crate::QueryRequest {
                        method: "query".into(),
                        payload: serde_json::to_vec(&request).unwrap(),
                    }),
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/zero.api.v1.Control/Query",
                    ),
                    tonic::codec::ProstCodec::default(),
                )
                .await;
            if total == 16 * 1024 {
                assert_eq!(response.unwrap_err().code(), tonic::Code::ResourceExhausted);
            } else {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(
                        &response.unwrap().into_inner().payload
                    )
                    .unwrap(),
                    expected
                );
            }
            // The synchronous original guards returned before any network wait.
            assert!(handle.control_proxy_query(&request).is_some());
        }
        for request in [
            zero_api::QueryRequest::Health(Default::default()),
            zero_api::QueryRequest::Capabilities(Default::default()),
        ] {
            let expected = serde_json::to_value(handle.query(request.clone()).unwrap()).unwrap();
            client.ready().await.unwrap();
            let response: tonic::Response<crate::QueryResponse> = client
                .unary(
                    tonic::Request::new(crate::QueryRequest {
                        method: "query".into(),
                        payload: serde_json::to_vec(&request).unwrap(),
                    }),
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/zero.api.v1.Control/Query",
                    ),
                    tonic::codec::ProstCodec::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.into_inner().payload)
                    .unwrap(),
                expected
            );
        }
        drop(client);
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete());
        book.close_admission();
        book.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
