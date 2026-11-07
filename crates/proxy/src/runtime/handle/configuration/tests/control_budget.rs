//! Ordinary accepted Proxy transactions own the same control book with or
//! without an application reconciler, while the original caller ACK is held.
use super::*;
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
use zero_runtime::configuration::ControlCharge;
struct RejectTarget;
#[async_trait::async_trait]
impl ConfigApplyReconciler for RejectTarget {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }
    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        if target.runtime.control_max_pending_commands == Some(2) {
            Err("original custom application rejected target".into())
        } else {
            Ok(Default::default())
        }
    }
    async fn retire_candidate(&self, _: Arc<RuntimeConfig>) -> Result<(), String> {
        Ok(())
    }
}
#[tokio::test]
async fn actual_proxy_apply_commits_or_restores_one_root_book_without_waiting_its_caller_ack() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for reject in [false, true] {
            let config = RuntimeConfig::parse(r#"{"runtime":{"control_max_pending_commands":1},"route":{"final":{"type":"direct"}}}"#).unwrap();
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let mut handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
            handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(3)).await.unwrap();
            if reject { handle = handle.with_config_apply_reconciler(Arc::new(RejectTarget)); }
            let book = proxy.application_control_budget();
            let row = book.command_owner().adopt_command(book.reserve(ControlCharge::PendingCommand).unwrap()).unwrap();
            let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), std::num::NonZeroUsize::new(1));
            scope.enable_registration().unwrap();
            let mut target = config;
            target.runtime.control_max_pending_commands = Some(2);
            let actual_row = row.clone();
            let actual_book = book.clone();
            let actual_engine = proxy.engine().clone();
            let (begin, gate) = tokio::sync::oneshot::channel();
            let (_, caller) = scope.registrar().spawn_tracked(TaskClass::Auxiliary, async move {
                gate.await.unwrap();
                let result = actual_row.observation().scope(handle.apply_runtime_config_and_wait(target, Duration::from_secs(3))).await;
                assert_eq!(result.is_err(), reject);
                let expected = if reject { 1 } else { 2 };
                assert_eq!(actual_book.limits().messages.pending_commands, expected);
                assert_eq!(actual_engine.config().runtime.control_max_pending_commands, Some(expected));
                assert!(actual_book.retained_candidate().is_none());
                assert_eq!(actual_book.usage().pending_commands, 1);
                actual_row.finish_inline().await?;
                Ok(())
            }).unwrap();
            assert!(row.register_scoped(caller.clone()));
            begin.send(()).unwrap();
            let receipt = scope.next_receipt().await.unwrap();
            assert!(receipt.result().is_ok());
            assert!(caller.matches_joined(receipt.joined()) && !receipt.joined().registration_released());
            assert_eq!(book.usage().pending_commands, 1);
            scope.acknowledge(&receipt).unwrap();
            while book.usage().pending_commands != 0 { tokio::task::yield_now().await; }
            scope.begin_shutdown();
            assert!(scope.next_receipt().await.is_none() && scope.is_drained());
            running.shutdown().await.unwrap();
            assert!(proxy.embedded_command_owner().retirement_complete());
        }
    }).await.unwrap();
}
