use super::*;
#[tokio::test]
async fn retired_http_server_fault_keeps_its_source_and_same_endpoint_recovery_waits_for_actual_close(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        let budget = zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                &zero_config::RuntimeOptionsConfig::default(),
            ),
        );
        let old = prepare_http_server_with_budget("127.0.0.1:0", None, budget.clone());
        old.start(handle.clone()).await.unwrap();
        let address = old.bound.lock().unwrap().unwrap();
        let pending = tokio::net::TcpStream::connect(address).await.unwrap();
        // Complete the triggering client's handshake before revoking accept.
        // Returning this view also wakes an already accepted handler, so the
        // original server observes the closed admission in either poll order.
        budget.close_admission();
        drop(pending);
        let fault = old.wait_terminated().await.unwrap_err();
        assert!(!old.retirement_complete());
        let retired = old.shutdown().await.unwrap_err();
        assert!(old.retirement_complete());
        let prior = fault
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        let actual = retired
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(Arc::ptr_eq(prior, actual));
        {
            let mut original = old.original.lock().await;
            let actual = original.as_mut().unwrap();
            assert!(actual
                .wait_closed(crate::ipc::owner::close_deadline())
                .await
                .unwrap()
                .is_complete());
            assert_eq!(actual.confirmed_close_count(), 1);
            assert!(!actual.native_unconfirmed());
        }
        budget.confirm_released().unwrap();
        let fresh_budget = zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                &zero_config::RuntimeOptionsConfig::default(),
            ),
        );
        let fresh =
            prepare_http_server_with_budget(&address.to_string(), None, fresh_budget.clone());
        fresh.start(handle).await.unwrap();
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                b"GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "actual recovered HTTP response: {response}"
        );
        fresh.shutdown().await.unwrap();
        assert!(fresh.retirement_complete());
        fresh_budget.close_admission();
        fresh_budget.confirm_released().unwrap();
        assert_eq!(
            fresh
                .original
                .lock()
                .await
                .as_ref()
                .unwrap()
                .confirmed_close_count(),
            1
        );
    })
    .await
    .unwrap();
}
