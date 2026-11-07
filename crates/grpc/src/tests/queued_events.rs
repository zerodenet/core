use super::{PendingEvents, PendingStream};
use crate::{pb::control_server::Control, ControlService, Subscriptions};
use futures_core::Stream;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits};

#[derive(Clone)]
struct OneEvent {
    event: Arc<Mutex<Option<zero_api::RawApiEvent>>>,
    taken: Arc<AtomicUsize>,
    received: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicUsize>,
}
struct OneStream {
    source: OneEvent,
    _original: PendingStream,
}
impl zero_api::EventStream for OneStream {
    fn recv(&self) -> Option<zero_api::RawApiEvent> {
        unreachable!("the original subscription must use finite waits")
    }
    fn try_recv(&self) -> Option<zero_api::RawApiEvent> {
        let event = self.source.event.lock().unwrap().take();
        if event.is_some() {
            self.source.taken.fetch_add(1, Ordering::AcqRel);
            self.source.received.notify_one();
        }
        event
    }
}
impl zero_api::EventSource for OneEvent {
    type Stream = OneStream;
    fn subscribe(&self, _: zero_api::EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(OneStream {
            source: self.clone(),
            _original: PendingStream(self.dropped.clone()),
        })
    }
    fn latest(
        &self,
        limit: usize,
        filter: zero_api::EventFilter,
    ) -> zero_api::ApiResult<Vec<zero_api::RawApiEvent>> {
        zero_api::EventSource::latest(&PendingEvents(self.dropped.clone()), limit, filter)
    }
    fn since(
        &self,
        after: u64,
        limit: usize,
        filter: zero_api::EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        zero_api::EventSource::since(&PendingEvents(self.dropped.clone()), after, limit, filter)
    }
}
impl zero_api::QueryService for OneEvent {
    fn query(&self, _: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        unreachable!()
    }
}
impl zero_api::CommandService for OneEvent {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!()
    }
}

async fn oversized_event(disconnect: Option<bool>) {
    let payload = serde_json::json!({"message": "x".repeat(256)});
    let source = OneEvent {
        event: Arc::new(Mutex::new(Some(zero_api::ApiEvent::new(
            "retained-event",
            "engine.warning",
            42,
            payload.clone(),
        )))),
        taken: Arc::new(AtomicUsize::new(0)),
        received: Arc::new(tokio::sync::Notify::new()),
        dropped: Arc::new(AtomicUsize::new(0)),
    };
    let budget = ControlBudget::new(ControlBudgetLimits {
        connections: 1,
        subscriptions: 1,
        queued_event_bytes: 64,
        pipe_workspace_bytes: 2 * 1024 * 1024,
        messages: Default::default(),
    });
    let jobs = Arc::new(Subscriptions::new(budget.clone()));
    let service = ControlService::new(source.clone(), None, jobs.clone());
    let mut stream = service
        .subscribe(tonic::Request::new(crate::SubscribeRequest {
            event_types: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(1), source.received.notified())
        .await
        .unwrap();
    // Poll the actual adapter receiver. An oversize frame neither closes this
    // stream nor allocates a charged serialized frame while awaiting capacity.
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        std::future::poll_fn(|cx| Stream::poll_next(stream.as_mut(), cx)),
    )
    .await
    .is_err());
    assert_eq!(source.taken.load(Ordering::Acquire), 1);
    assert_eq!(source.dropped.load(Ordering::Acquire), 0);
    assert!(!jobs.jobs.lock().unwrap()[0].finished());
    assert_eq!(budget.usage().subscriptions, 1);
    assert_eq!(budget.usage().queued_event_bytes, 0);
    match disconnect {
        None => {
            budget
                .prepare(ControlBudgetLimits {
                    connections: 1,
                    subscriptions: 1,
                    queued_event_bytes: 1024,
                    pipe_workspace_bytes: 2 * 1024 * 1024,
                    messages: Default::default(),
                })
                .unwrap()
                .commit()
                .unwrap();
            let event = tokio::time::timeout(
                Duration::from_secs(1),
                std::future::poll_fn(|cx| Stream::poll_next(stream.as_mut(), cx)),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(event.event_id, "retained-event");
            assert_eq!(event.occurred_at, 42);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&event.payload).unwrap(),
                payload
            );
            assert_eq!(budget.usage().queued_event_bytes, 0);
            drop(stream);
        }
        Some(true) => drop(stream),
        Some(false) => {
            jobs.stop.store(true, Ordering::Release);
            assert!(tokio::time::timeout(
                Duration::from_secs(1),
                std::future::poll_fn(|cx| Stream::poll_next(stream.as_mut(), cx)),
            )
            .await
            .unwrap()
            .is_none());
            drop(stream);
        }
    }
    tokio::time::timeout(Duration::from_secs(1), jobs.join_all())
        .await
        .unwrap()
        .unwrap();
    assert!(jobs.jobs.lock().unwrap()[0].joined_successfully());
    assert_eq!(source.dropped.load(Ordering::Acquire), 1);
    assert_eq!(budget.usage().subscriptions, 0);
    assert_eq!(budget.usage().queued_event_bytes, 0);
}

#[tokio::test]
async fn oversize_grpc_event_survives_until_the_same_root_budget_grows() {
    oversized_event(None).await;
}
#[tokio::test]
async fn disconnected_oversize_grpc_event_joins_the_original_subscriber() {
    oversized_event(Some(true)).await;
}
#[tokio::test]
async fn stopped_oversize_grpc_event_joins_the_original_subscriber() {
    oversized_event(Some(false)).await;
}

#[tokio::test]
async fn actual_native_subscription_transfers_prepaid_event_into_the_original_encoder_and_joins() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let payload = serde_json::json!({"message": "x".repeat(256)});
        let source = OneEvent {
            event: Arc::new(Mutex::new(Some(zero_api::ApiEvent::new(
                "retained-wire-event",
                "engine.warning",
                42,
                payload.clone(),
            )))),
            taken: Arc::new(AtomicUsize::new(0)),
            received: Arc::new(tokio::sync::Notify::new()),
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        let mut limits = crate::budget::defaults().limits();
        limits.queued_event_bytes = 64;
        let budget = ControlBudget::new(limits);
        let owner = crate::prepare_with_budget(
            "127.0.0.1:0".parse().unwrap(),
            crate::GrpcServerSecurity::default(),
            budget.clone(),
        )
        .unwrap();
        owner.start(source.clone()).await.unwrap();
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
        let mut events = response.into_inner();
        source.received.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.message())
                .await
                .is_err()
        );
        assert_eq!(source.taken.load(Ordering::Acquire), 1);
        assert_eq!(budget.usage().subscriptions, 1);
        assert_eq!(budget.usage().grpc_requests, 1);
        assert_eq!(budget.usage().queued_event_bytes, 0);
        assert!(
            budget.usage().response_bytes > 0,
            "the same original encoder has prepaid its existing workspace"
        );
        limits.queued_event_bytes = 1024;
        budget.prepare(limits).unwrap().commit().unwrap();
        let event = events.message().await.unwrap().unwrap();
        assert_eq!(event.event_id, "retained-wire-event");
        assert_eq!(event.occurred_at, 42);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&event.payload).unwrap(),
            payload
        );
        assert_eq!(
            budget.usage().queued_event_bytes,
            0,
            "the real encoder consumed the original raw message"
        );
        let mut shrink = limits;
        shrink.messages.response_bytes_total = 1;
        assert!(
            budget.prepare(shrink).is_err(),
            "the live encoder still retains its actual backing storage bound"
        );
        drop(events);
        drop(client);
        owner.shutdown().await.unwrap();
        assert!(owner.retirement_complete());
        assert_eq!(source.dropped.load(Ordering::Acquire), 1);
        assert!(owner.originals.subscriptions.jobs.lock().unwrap()[0].joined_successfully());
        assert_eq!(
            owner
                .originals
                .original
                .lock()
                .await
                .confirmed_close_count(),
            1
        );
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
