use super::*;
use std::{num::NonZeroUsize, time::Duration};
use zero_api::QueryService;
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits};

#[derive(Clone)]
struct ViewOnly;
impl zero_api::QueryService for ViewOnly {
    fn query(&self, _: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        panic!("production config query must borrow its supplied Engine")
    }
}
impl zero_api::CommandService for ViewOnly {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!("query-only fixture")
    }
}
impl zero_api::EventSource for ViewOnly {
    type Stream = <zero_engine::EngineHandle as zero_api::EventSource>::Stream;
    fn subscribe(&self, _: zero_api::EventFilter) -> zero_api::ApiResult<Self::Stream> {
        unreachable!("query-only fixture")
    }
    fn latest(
        &self,
        _: usize,
        _: zero_api::EventFilter,
    ) -> zero_api::ApiResult<Vec<zero_api::RawApiEvent>> {
        unreachable!("query-only fixture")
    }
    fn since(
        &self,
        _: u64,
        _: usize,
        _: zero_api::EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        unreachable!("query-only fixture")
    }
}

fn engine() -> zero_engine::EngineHandle {
    zero_engine::EngineHandle::new(zero_engine::Engine::new(serde_json::from_value(serde_json::json!({
        "outbounds":[{"tag":"leaf","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}}],
        "outbound_groups":[{"tag":"pick","type":"selector","outbounds":["leaf"]}],
        "route":{"final":{"type":"direct"}}
    })).unwrap()).unwrap())
}

#[tokio::test]
async fn ordinary_native_grpc_uses_the_same_engine_view_and_original_error_shape() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let engine = engine();
        let budget = crate::budget::defaults();
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            Default::default(),
            budget.clone(),
        )
        .unwrap();
        // Its QueryService is unreachable: these production queries must use
        // the supplied actual Engine view, including the missing-policy error.
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
        for query in [
            zero_api::QueryRequest::Config(Default::default()),
            zero_api::QueryRequest::Policies(Default::default()),
            zero_api::QueryRequest::Runtime(Default::default()),
            zero_api::QueryRequest::Stats(Default::default()),
            zero_api::QueryRequest::Diagnostics(Default::default()),
            zero_api::QueryRequest::ActiveFlows(zero_api::FlowListQuery {
                limit: None,
                filter: Default::default(),
            }),
            zero_api::QueryRequest::RecentFlows(zero_api::FlowListQuery {
                limit: None,
                filter: Default::default(),
            }),
            zero_api::QueryRequest::PrincipalFlows(Default::default()),
            zero_api::QueryRequest::Sinks(Default::default()),
            zero_api::QueryRequest::Flow(zero_api::FlowGetQuery {
                flow_id: "not-a-flow-雪".into(),
            }),
            zero_api::QueryRequest::Policy(zero_api::PolicyGetQuery {
                policy_tag: "pick".into(),
            }),
            zero_api::QueryRequest::Policy(zero_api::PolicyGetQuery {
                policy_tag: "missing \"雪\"\n".into(),
            }),
        ] {
            client.ready().await.unwrap();
            let response: Result<tonic::Response<crate::QueryResponse>, tonic::Status> = client
                .unary(
                    tonic::Request::new(crate::QueryRequest {
                        method: "query".into(),
                        payload: serde_json::to_vec(&query).unwrap(),
                    }),
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/zero.api.v1.Control/Query",
                    ),
                    tonic::codec::ProstCodec::default(),
                )
                .await;
            match engine.query(query) {
                Ok(expected) => assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(
                        &response.unwrap().into_inner().payload
                    )
                    .unwrap(),
                    serde_json::to_value(expected).unwrap()
                ),
                Err(expected) => {
                    let actual = response.unwrap_err();
                    assert_eq!(actual.code(), tonic::Code::Internal);
                    assert_eq!(actual.message(), expected.to_string());
                }
            }
        }
        for total in [16 * 1024, 128 * 1024] {
            while budget.usage().grpc_requests != 0 {
                tokio::task::yield_now().await;
            }
            let mut limits = budget.limits();
            limits.messages.response_bytes_total = total;
            budget.prepare(limits).unwrap().commit().unwrap();
            client.ready().await.unwrap();
            let query = zero_api::QueryRequest::Stats(Default::default());
            let result: Result<tonic::Response<crate::QueryResponse>, tonic::Status> = client
                .unary(
                    tonic::Request::new(crate::QueryRequest {
                        method: "query".into(),
                        payload: serde_json::to_vec(&query).unwrap(),
                    }),
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/zero.api.v1.Control/Query",
                    ),
                    tonic::codec::ProstCodec::default(),
                )
                .await;
            if total == 16 * 1024 {
                assert_eq!(
                    result.unwrap_err().code(),
                    tonic::Code::ResourceExhausted,
                    "unary quota refusal must not wait for its own workspace"
                );
            } else {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(
                        &result.unwrap().into_inner().payload
                    )
                    .unwrap(),
                    serde_json::to_value(engine.query(query).unwrap()).unwrap(),
                    "same original connection recovers through configured Root growth"
                );
            }
        }
        drop(client);
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_status_header_clones_keep_credit_after_the_same_request_scope_joins() {
    let budget = ControlBudget::new(ControlBudgetLimits {
        connections: 1,
        subscriptions: 1,
        queued_event_bytes: 1024,
        pipe_workspace_bytes: 2 * 1024 * 1024,
        messages: Default::default(),
    });
    let connection = budget.reserve(ControlCharge::Connection).unwrap();
    let local = connection.connection_budget().unwrap();
    let originals = Arc::new(super::super::Requests::new(budget.clone()));
    let original = originals.reserve(&local).unwrap();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (done, wait) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            wait.await.unwrap();
            Ok(())
        })
        .unwrap();
    assert!(original.bind(witness));
    let held = budget
        .reserve(ControlCharge::ResponseBytes(
            budget.limits().messages.response_bytes_total - super::super::CODEC_BUFFER_SIZE,
        ))
        .unwrap();
    let source =
        zero_api::ApiError::new(zero_api::ApiErrorCode::NotFound, "no \"雪\"\n".repeat(100));
    let error = {
        let pending = original.error_status(tonic::Code::Internal, "", &source);
        tokio::pin!(pending);
        std::future::poll_fn(|cx| {
            use std::future::Future;
            assert!(
                pending.as_mut().poll(cx).is_pending(),
                "the original status storage must wait for actual shared credit"
            );
            std::task::Poll::Ready(())
        })
        .await;
        drop(held);
        pending.await
    };
    assert_eq!(error.message(), source.to_string());
    let mut response = error.into_http();
    original
        .retain_status_header(response.headers_mut())
        .unwrap();
    let header = response.headers()["grpc-message"].clone();
    assert!(budget.usage().response_bytes > super::super::CODEC_BUFFER_SIZE);
    drop((response, original));
    done.send(()).unwrap();
    scope.next_exit().await.unwrap().result.unwrap();
    originals.confirm_released().unwrap();
    assert!(scope
        .shutdown(crate::native::deadline())
        .await
        .is_complete());
    assert_eq!(
        budget.usage().grpc_requests,
        0,
        "cached header storage cannot pin concurrency after the actual request joins"
    );
    assert!(budget.usage().response_bytes > 0);
    drop(header);
    assert_eq!(budget.usage().grpc_requests, 0);
    assert_eq!(budget.usage().response_bytes, 0);
    drop((local, connection));
    budget.close_admission();
    budget.confirm_released().unwrap();
}
