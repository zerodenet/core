//! A real raw blocking panic stays typed in the actual request and Scope exit.
use super::*;
use std::{num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
use zero_runtime::configuration::ControlCharge;
#[derive(Clone, Default)]
struct RawPanic(Arc<Mutex<Option<Arc<tokio::task::JoinError>>>>);
impl zero_api::CommandService for RawPanic {
    fn execute(
        &self,
        _: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        unreachable!("the existing acknowledged entry owns the actual raw handle")
    }
    fn execute_acknowledged(
        &self,
        _: zero_api::CommandRequest,
    ) -> std::pin::Pin<
        Box<dyn Future<Output = zero_api::ApiResult<zero_api::CommandResponse>> + Send + '_>,
    > {
        Box::pin(async {
            let original =
                tokio::task::spawn_blocking(|| panic!("actual raw blocking command panic"));
            let id = original.id();
            let source = Arc::new(original.await.unwrap_err());
            assert_eq!(source.id(), id);
            zero_runtime::ApplyObservation::record_blocking_outcome(id, Some(source.clone()))
                .unwrap();
            *self.0.lock().unwrap() = Some(source);
            Err(zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                "actual command worker failed",
            ))
        })
    }
}
#[tokio::test]
async fn actual_raw_join_error_is_retained_before_the_same_request_task_reports_cleanup_and_refunds(
) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let budget = crate::budget::defaults();
        let connection = budget.reserve(ControlCharge::Connection).unwrap();
        let local = connection.connection_budget().unwrap();
        let requests = Arc::new(super::super::Requests::new(budget.clone()));
        let original = requests.reserve(&local).unwrap();
        let handler = RawPanic::default();
        let expected = handler.0.clone();
        let held = original.clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (begin, started) = tokio::sync::oneshot::channel();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                started.await.unwrap();
                let wire = held
                    .execute(
                        handler,
                        zero_api::CommandRequest::ModeSet(Default::default()),
                    )
                    .await
                    .unwrap_err();
                assert_eq!(wire.code(), tonic::Code::Internal);
                held.command.drain().await.map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Session).with_source(source)
                })
            })
            .unwrap();
        assert!(original.bind(witness.clone()));
        begin.send(()).unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert!(exit.result.is_err());
        let native_raw = expected.lock().unwrap().clone().unwrap();
        let retained = original.command.cleanup.get().unwrap();
        assert!(Arc::ptr_eq(
            retained
                .get_ref()
                .unwrap()
                .downcast_ref::<Arc<tokio::task::JoinError>>()
                .unwrap(),
            &native_raw
        ));
        assert!(
            original.command.operation.get().is_none(),
            "raw completion cannot invent an Apply TaskJoined"
        );
        let outcome = original.command.observation.blocking_outcome().unwrap();
        assert_eq!(outcome.task_id(), native_raw.id());
        assert!(Arc::ptr_eq(outcome.failure().unwrap(), &native_raw));
        assert!(witness.try_joined().unwrap().registration_released());
        assert_eq!(
            budget.usage().pending_commands,
            1,
            "the actual request still holds command custody"
        );
        requests.confirm_released().unwrap();
        drop(original);
        assert_eq!(budget.usage().pending_commands, 0);
        assert_eq!(budget.usage().grpc_requests, 0);
        drop((local, connection));
        assert!(scope
            .shutdown(crate::native::deadline())
            .await
            .is_complete());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
