//! Real Prost bytes and real task joins; slices retain their original capacity.
use super::{Output, Payload, Requests};
use hyper::body::{Body, Bytes};
use prost::Message;
use std::{future::poll_fn, num::NonZeroUsize, pin::Pin, sync::Arc};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};

#[tokio::test]
async fn actual_encoded_bytes_clones_and_slices_keep_storage_after_the_original_task_join() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let budget = ControlBudget::new(ControlBudgetLimits {
            connections: 1,
            subscriptions: 1,
            queued_event_bytes: 1024,
            pipe_workspace_bytes: 2 * 1024 * 1024,
            messages: Default::default(),
        });
        let connection = budget.reserve(ControlCharge::Connection).unwrap();
        let local = connection.connection_budget().unwrap();
        let originals = Arc::new(Requests::new(budget.clone()));
        let original = originals.reserve(&local).unwrap();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (done, pending) = tokio::sync::oneshot::channel();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                pending.await.unwrap();
                Ok(())
            })
            .unwrap();
        assert!(original.bind(witness.clone()));
        let json = original
            .json(Payload::Query(&zero_api::QueryResponse::Diagnostics(
                serde_json::json!({"ok":true}),
            )))
            .await
            .unwrap();
        let proto = crate::QueryResponse { payload: json }.encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(proto.len() as u32).to_be_bytes());
        frame.extend_from_slice(&proto);
        let body = tonic::body::boxed(http_body_util::Full::new(Bytes::from(frame.clone())));
        let mut output = Output::new(body, original.clone());
        let wire = poll_fn(|cx| Pin::new(&mut output).poll_frame(cx))
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(wire.as_ref(), frame);
        let slice = wire.slice(1..).clone();
        drop((wire, output, original));
        originals.reap();
        assert!(!witness.is_joined());
        assert_eq!(budget.usage().grpc_requests, 1);
        done.send(()).unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(witness.matches_joined(&witness.try_joined().unwrap()));
        assert!(witness.try_joined().unwrap().registration_released());
        originals.confirm_released().unwrap();
        assert!(scope.is_drained());
        assert!(
            budget.usage().response_bytes > 0,
            "the emitted original allocation remains held by its actual Bytes slice"
        );
        assert_eq!(budget.usage().grpc_requests, 1);
        let mut shrink = budget.limits();
        shrink.messages.response_bytes_total = 1;
        assert!(budget.prepare(shrink).is_err());
        drop(slice);
        assert_eq!(budget.usage().response_bytes, 0);
        assert_eq!(budget.usage().grpc_requests, 0);
        drop((local, connection));
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
