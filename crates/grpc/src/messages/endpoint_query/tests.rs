//! Real HTTP/2 requests must use the concrete production Proxy owner, not H DTOs.
use zero_api::{CommandService, EventSource, QueryService};
#[derive(Clone)]
struct NoDto;
impl QueryService for NoDto {
    fn query(&self, _: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        panic!("production endpoint view fell back to an owned DTO")
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
async fn query(
    client: &mut tonic::client::Grpc<tonic::transport::Channel>,
    request: &zero_api::QueryRequest,
) -> Result<tonic::Response<crate::QueryResponse>, tonic::Status> {
    client.ready().await.unwrap();
    client
        .unary(
            tonic::Request::new(crate::QueryRequest {
                method: "query".into(),
                payload: serde_json::to_vec(request).unwrap(),
            }),
            tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Query"),
            tonic::codec::ProstCodec::default(),
        )
        .await
}
#[tokio::test]
async fn actual_endpoint_unary_wire_capacity_recovery_and_original_missing_errors_never_materialize_h_dtos(
) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let proxy = zero_proxy::Proxy::new(
            serde_json::from_value(serde_json::json!({"route":{"final":{"type":"direct"}}}))
                .unwrap(),
        )
        .unwrap();
        let book = proxy.application_control_budget();
        let handle = zero_proxy::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
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
        let request = zero_api::QueryRequest::Endpoints(Default::default());
        let expected = serde_json::to_value(handle.query(request.clone()).unwrap()).unwrap();
        for total in [128 * 1024, 16 * 1024, 128 * 1024] {
            while book.usage().grpc_requests != 0 {
                tokio::task::yield_now().await;
            }
            let mut limits = book.limits();
            limits.messages.response_bytes_total = total;
            book.prepare(limits).unwrap().commit().unwrap();
            let response = query(&mut client, &request).await;
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
        }
        for request in [
            zero_api::QueryRequest::Endpoint(zero_api::EndpointGetQuery {
                endpoint_id: "missing-雪".into(),
            }),
            zero_api::QueryRequest::EndpointDetails(zero_api::EndpointGetQuery {
                endpoint_id: "missing-雪".into(),
            }),
        ] {
            let error = query(&mut client, &request).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(
                error.message(),
                "not_found: endpoint `missing-雪` was not found"
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
