use super::{PendingEvents, PendingStream};
use crate::{
    pb::{ExecuteRequest, ExecuteResponse, QueryRequest, QueryResponse},
    GrpcServerSecurity,
};
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits};
#[path = "operation.rs"]
mod operation;

#[derive(Clone, Default)]
struct Commands {
    pending: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    acknowledged: Arc<AtomicBool>,
    queries: Arc<AtomicUsize>,
}
impl zero_api::CommandService for Commands {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!()
    }
    fn execute_acknowledged(
        &self,
        _: zero_api::CommandRequest,
    ) -> Pin<Box<dyn Future<Output = zero_api::ApiResult<zero_api::CommandResponse>> + Send + '_>>
    {
        Box::pin(async move {
            let released = self.release.notified();
            tokio::pin!(released);
            poll_fn(|cx| match released.as_mut().poll(cx) {
                Poll::Pending => {
                    self.pending.notify_one();
                    Poll::Pending
                }
                Poll::Ready(()) => Poll::Ready(()),
            })
            .await;
            self.acknowledged.store(true, Ordering::Release);
            Ok(zero_api::CommandResponse::accepted())
        })
    }
}
impl zero_api::QueryService for Commands {
    fn query(&self, _: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        self.queries.fetch_add(1, Ordering::AcqRel);
        Ok(zero_api::QueryResponse::Diagnostics(
            serde_json::json!({"ok":true}),
        ))
    }
}
impl zero_api::EventSource for Commands {
    type Stream = PendingStream;
    fn subscribe(&self, filter: zero_api::EventFilter) -> zero_api::ApiResult<Self::Stream> {
        zero_api::EventSource::subscribe(&PendingEvents(Arc::new(AtomicUsize::new(0))), filter)
    }
    fn latest(
        &self,
        _: usize,
        _: zero_api::EventFilter,
    ) -> zero_api::ApiResult<Vec<zero_api::RawApiEvent>> {
        Ok(Vec::new())
    }
    fn since(
        &self,
        after: u64,
        limit: usize,
        filter: zero_api::EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        zero_api::EventSource::since(
            &PendingEvents(Arc::new(AtomicUsize::new(0))),
            after,
            limit,
            filter,
        )
    }
}
fn budget() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits {
        connections: 2,
        subscriptions: 2,
        queued_event_bytes: 1024,
        pipe_workspace_bytes: 2 * 1024 * 1024,
        messages: Default::default(),
    })
}
async fn client(owner: &crate::GrpcHandle) -> tonic::client::Grpc<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", owner.local_addr()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    client
}
#[tokio::test]
async fn disconnected_wire_command_keeps_input_and_command_credit_until_actual_final_ack_and_join()
{
    tokio::time::timeout(Duration::from_secs(5), async {
        let budget = budget();
        let service = Commands::default();
        let owner = crate::prepare_with_budget("127.0.0.1:0".parse().unwrap(), GrpcServerSecurity::default(), budget.clone()).unwrap();
        owner.start(service.clone()).await.unwrap();
        let mut client = client(&owner).await;
        let request = tonic::Request::new(ExecuteRequest { method: "fakeip.clear".into(), payload: br#"{"method":"fakeip.clear","params":{}}"#.to_vec() });
        let mut call = Box::pin(client.unary::<_, ExecuteResponse, _>(request, tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Execute"), tonic::codec::ProstCodec::default()));
        tokio::select! { _=service.pending.notified()=>{}, result=&mut call=>panic!("wire command finished before its final ACK: {result:?}") }
        assert!(!service.acknowledged.load(Ordering::Acquire));
        assert_eq!(budget.usage().pending_commands, 1);
        let held = budget.usage().request_bytes;
        assert!(held > 0);
        drop(call);
        drop(client);
        let mut shutdown = Box::pin(owner.shutdown());
        poll_fn(|cx| { assert!(shutdown.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
        assert!(!owner.retirement_complete());
        assert_eq!(budget.usage().pending_commands, 1);
        assert_eq!(budget.usage().request_bytes, held);
        let mut shrink = budget.limits();
        shrink.messages.request_bytes_total = held - 1;
        assert!(budget.prepare(shrink).is_err());
        service.release.notify_one();
        shutdown.await.unwrap();
        assert!(service.acknowledged.load(Ordering::Acquire));
        assert!(owner.retirement_complete());
        assert!(owner.originals.scope.lock().await.shutdown(crate::native::deadline()).await.is_complete());
        assert_eq!(owner.originals.original.lock().await.confirmed_close_count(), 1);
        budget.command_owner().shutdown(crate::native::deadline()).await.unwrap();
        assert!(budget.command_owner().retirement_complete());
        budget.close_admission();
        budget.confirm_released().unwrap();
    }).await.unwrap();
}
#[tokio::test]
async fn actual_wire_decoder_enforces_configured_limit_before_the_typed_query_runs() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut limits = budget().limits();
        limits.messages.grpc_request_bytes = 32;
        let budget = ControlBudget::new(limits);
        let service = Commands::default();
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            GrpcServerSecurity::default(),
            budget.clone(),
        )
        .unwrap();
        owner.start(service.clone()).await.unwrap();
        let mut client = client(&owner).await;
        let request = tonic::Request::new(QueryRequest {
            method: "health".into(),
            payload: vec![b' '; 128],
        });
        let result: Result<tonic::Response<QueryResponse>, _> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Query"),
                tonic::codec::ProstCodec::default(),
            )
            .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::OutOfRange);
        assert_eq!(service.queries.load(Ordering::Acquire), 0);
        drop(client);
        owner.shutdown().await.unwrap();
        budget
            .command_owner()
            .shutdown(crate::native::deadline())
            .await
            .unwrap();
        assert!(budget.command_owner().retirement_complete());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_wire_query_keeps_the_existing_json_protobuf_and_native_retirement_contract() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let budget = budget();
        let service = Commands::default();
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            GrpcServerSecurity::default(),
            budget.clone(),
        )
        .unwrap();
        owner.start(service.clone()).await.unwrap();
        let mut client = client(&owner).await;
        let request = tonic::Request::new(QueryRequest {
            method: "health".into(),
            payload: br#"{"health":{}}"#.to_vec(),
        });
        let result: tonic::Response<QueryResponse> = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Query"),
                tonic::codec::ProstCodec::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&result.into_inner().payload).unwrap(),
            serde_json::json!({"diagnostics":{"ok":true}})
        );
        assert_eq!(service.queries.load(Ordering::Acquire), 1);
        drop(client);
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete());
        budget
            .command_owner()
            .shutdown(crate::native::deadline())
            .await
            .unwrap();
        assert!(budget.command_owner().retirement_complete());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
