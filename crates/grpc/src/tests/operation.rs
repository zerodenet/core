use super::*;
#[derive(Clone)]
struct ApplyCommands {
    base: Commands,
    apply: Arc<zero_runtime::ApplyOwner>,
    serial: Arc<tokio::sync::Mutex<()>>,
}
impl zero_api::CommandService for ApplyCommands {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!()
    }
    fn execute_acknowledged(
        &self,
        command: zero_api::CommandRequest,
    ) -> Pin<Box<dyn Future<Output = zero_api::ApiResult<zero_api::CommandResponse>> + Send + '_>>
    {
        Box::pin(async move {
            let base = self.base.clone();
            let accepted = self.apply.run(self.serial.clone(), async move {
                let result = zero_api::CommandService::execute_acknowledged(&base, command).await;
                result.map_err(|error| error.to_string())
            });
            // A short fixture caller deadline models the unchanged production
            // deadline without changing its 15-second value or actual owner.
            tokio::time::timeout(Duration::from_millis(20), accepted)
                .await
                .map_err(|_| {
                    zero_api::ApiError::new(
                        zero_api::ApiErrorCode::Internal,
                        "original caller timeout",
                    )
                })?
                .map_err(|error| zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error))
        })
    }
}
impl zero_api::QueryService for ApplyCommands {
    fn query(&self, query: zero_api::QueryRequest) -> zero_api::ApiResult<zero_api::QueryResponse> {
        zero_api::QueryService::query(&self.base, query)
    }
}
impl zero_api::EventSource for ApplyCommands {
    type Stream = PendingStream;
    fn subscribe(&self, filter: zero_api::EventFilter) -> zero_api::ApiResult<Self::Stream> {
        zero_api::EventSource::subscribe(&self.base, filter)
    }
    fn latest(
        &self,
        limit: usize,
        filter: zero_api::EventFilter,
    ) -> zero_api::ApiResult<Vec<zero_api::RawApiEvent>> {
        zero_api::EventSource::latest(&self.base, limit, filter)
    }
    fn since(
        &self,
        after: u64,
        limit: usize,
        filter: zero_api::EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        zero_api::EventSource::since(&self.base, after, limit, filter)
    }
}
#[tokio::test]
async fn actual_wire_timeout_preserves_input_until_the_same_accepted_apply_really_joins() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let budget = budget();
        let service = ApplyCommands { base: Commands::default(), apply: Arc::new(zero_runtime::ApplyOwner::default()), serial: Arc::new(tokio::sync::Mutex::new(())) };
        let owner = crate::prepare_with_budget("127.0.0.1:0".parse().unwrap(), GrpcServerSecurity::default(), budget.clone()).unwrap();
        owner.start(service.clone()).await.unwrap();
        let mut client = client(&owner).await;
        let request = tonic::Request::new(ExecuteRequest { method: "fakeip.clear".into(), payload: br#"{"method":"fakeip.clear","params":{}}"#.to_vec() });
        let mut call = Box::pin(client.unary::<_, ExecuteResponse, _>(request, tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Execute"), tonic::codec::ProstCodec::default()));
        tokio::select! { _=service.base.pending.notified()=>{}, result=&mut call=>panic!("original Apply was not pending: {result:?}") }
        let error = call.await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error.message().contains("original caller timeout"));
        assert!(!service.base.acknowledged.load(Ordering::Acquire));
        assert_eq!(budget.usage().pending_commands, 1, "wire error is not the original Apply join");
        let held = budget.usage().request_bytes;
        assert!(held > 0);
        drop(client);
        let mut shutdown = Box::pin(owner.shutdown());
        poll_fn(|cx| { assert!(shutdown.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
        assert_eq!(budget.usage().request_bytes, held);
        assert_eq!(budget.usage().pending_commands, 1);
        assert!(!owner.retirement_complete());
        service.base.release.notify_one();
        shutdown.await.unwrap();
        assert!(service.base.acknowledged.load(Ordering::Acquire));
        assert!(owner.retirement_complete());
        assert_eq!(owner.originals.original.lock().await.confirmed_close_count(), 1);
        budget.command_owner().shutdown(crate::native::deadline()).await.unwrap();
        assert!(budget.command_owner().retirement_complete());
        budget.close_admission(); budget.confirm_released().unwrap();
    }).await.unwrap();
}
