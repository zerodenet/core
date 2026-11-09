use super::*;
use std::{io::Write, num::NonZeroUsize, task::Poll};
use zero_platform_tokio::contract::scope::TaskScope;
#[tokio::test]
async fn same_actual_logger_book_busy_then_grows_at_application_commit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("application.log");
        let mut config = RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
        config.runtime.log.files.push(zero_config::LogFileConfig {
            path: path.to_str().unwrap().into(),
            level: None,
            max_bytes: u64::MAX,
            max_files: 2,
        });
        config.runtime.log.max_pending_bytes = Some(4);
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        let mut logging =
            zero_logging::ScopedTracing::prepare(&config.runtime.log, &scope.registrar()).unwrap();
        scope.enable_registration().unwrap();
        logging.drive(&scope.registrar()).unwrap();
        let held = scope.next_receipt().await.unwrap();
        assert!(held.result().is_ok());
        assert!(logging.ready());
        let mut writer = logging.writer(0).unwrap();
        writer.write_all(b"old\n").unwrap();
        assert_eq!(logging.usage().pending_bytes, 4);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let services = ApplicationServices::start_with_proxy_and_logging(
            &proxy,
            None,
            Some(logging.budget_handle()),
        )
        .await
        .unwrap();
        let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
            .with_config_apply_reconciler(services.clone());
        let mut small = config.clone();
        small.runtime.log.max_pending_bytes = Some(3);
        assert!(handle
            .apply_runtime_config_and_wait(small, Duration::from_secs(3))
            .await
            .is_err());
        assert_eq!(
            proxy.engine().config().runtime.log.max_pending_bytes,
            Some(4)
        );
        assert_eq!(logging.usage().pending_bytes, 4);
        let mut grow = config;
        grow.runtime.log.max_pending_bytes = Some(8);
        handle
            .apply_runtime_config_and_wait(grow.clone(), Duration::from_secs(3))
            .await
            .unwrap();
        assert!(logging
            .budget_handle()
            .limits_match(&grow.runtime.log)
            .unwrap());
        writer.write_all(b"more").unwrap();
        assert_eq!(logging.usage().pending_bytes, 8);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"",
            "actual original ACK is still held"
        );
        scope.acknowledge(&held).unwrap();
        drop(held);
        std::future::poll_fn(|cx| {
            logging.poll_workers(&mut scope, cx).unwrap();
            if logging.drained() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(std::fs::read(&path).unwrap(), b"old\nmore");
        assert_eq!(logging.usage().pending_bytes, 0);
        services.shutdown_control().await.unwrap();
        services.shutdown_status_monitor().await.unwrap();
        services.shutdown_dispatcher().await.unwrap();
        running.shutdown().await.unwrap();
        logging.request_stop();
        std::future::poll_fn(|cx| {
            logging.poll_workers(&mut scope, cx).unwrap();
            if logging.drained() && logging.native_closed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        scope.begin_shutdown();
        std::future::poll_fn(|cx| {
            logging.poll_workers(&mut scope, cx).unwrap();
            if scope.is_drained() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(logging.close_after_workers());
    })
    .await
    .unwrap();
}
