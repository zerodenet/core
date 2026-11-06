use tonic::Request;

use super::GrpcServerAuth;

#[test]
fn bearer_auth_accepts_the_configured_credential() {
    let auth = GrpcServerAuth::single_admin("secret".to_owned());
    let mut request = Request::new(());
    request
        .metadata_mut()
        .insert("authorization", "Bearer secret".parse().expect("metadata"));

    assert!(auth.is_authorized(&request));
}

#[test]
fn bearer_auth_rejects_missing_or_different_credentials() {
    let auth = GrpcServerAuth::single_admin("secret".to_owned());
    assert!(!auth.is_authorized(&Request::new(())));

    let mut different = Request::new(());
    different
        .metadata_mut()
        .insert("authorization", "Bearer other".parse().expect("metadata"));
    assert!(!auth.is_authorized(&different));
}

#[derive(Clone)]
struct PendingEvents(std::sync::Arc<std::sync::atomic::AtomicUsize>);
struct PendingStream(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Drop for PendingStream {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}
impl zero_api::EventStream for PendingStream {
    fn recv(&self) -> Option<zero_api::RawApiEvent> {
        unreachable!("the control bridge must use finite cancellation-aware waits")
    }
    fn try_recv(&self) -> Option<zero_api::RawApiEvent> {
        None
    }
}
impl zero_api::EventSource for PendingEvents {
    type Stream = PendingStream;
    fn subscribe(&self, _: zero_api::EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(PendingStream(self.0.clone()))
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
        _: usize,
        _: zero_api::EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        Ok(zero_api::EventReplay {
            core_instance_id: "pending-subscribe".to_owned(),
            requested_after: after,
            actual_from: after,
            has_gap: false,
            events: Vec::new(),
        })
    }
}
impl zero_api::QueryService for PendingEvents {
    fn query(&self, _: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        unreachable!()
    }
}
impl zero_api::CommandService for PendingEvents {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!()
    }
}

#[tokio::test]
async fn idle_grpc_subscription_disconnect_joins_the_original_without_a_followup_event() {
    use super::pb::control_server::Control;
    let released = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let jobs = std::sync::Arc::new(super::Subscriptions::default());
    let service = super::ControlService::new(PendingEvents(released.clone()), None, jobs.clone());
    let stream = service
        .subscribe(tonic::Request::new(super::SubscribeRequest {
            event_types: Vec::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(jobs.jobs.lock().unwrap().len(), 1);
    assert_eq!(jobs.budget.usage().subscriptions, 1);
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(1), jobs.join_all())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released.load(std::sync::atomic::Ordering::Acquire), 1);
    assert!(jobs.jobs.lock().unwrap()[0].joined_successfully());
    assert_eq!(jobs.budget.usage().subscriptions, 0);
}

mod queued_events;

mod retirement;
