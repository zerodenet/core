//! Source-facing wire calls retain the real coalesced Host query and cleanup.
use crate::Proxy;
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
#[tokio::test]
async fn cancelled_wire_caller_waits_for_the_same_shared_host_query_and_actual_original_cleanup() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for tcp in [false, true] {
            actual_call(tcp, false).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_host_query_fault_does_not_hide_positive_caller_retirement_or_its_original_typed_errors(
) {
    tokio::time::timeout(Duration::from_secs(10), actual_call(false, true))
        .await
        .unwrap();
}
async fn actual_call(tcp: bool, fault: bool) {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let endpoint = peer.local_addr().unwrap();
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"native_dns_max_originals":1,"dns":{"servers":{"wire":{"type":"udp","host":"127.0.0.1","port":endpoint.port()}},"default_server":"wire"}},
                "route":{"final":{"type":"direct"}}
            }).to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let mut running = Some(proxy.spawn());
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let mut wire = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in ["actual", "example"] {
        wire.push(label.len() as u8);
        wire.extend_from_slice(label.as_bytes());
    }
    wire.extend_from_slice(&[0, 0, 1, 0, 1]);
    let (first, mut request) = if tcp {
        proxy.resolver.prepare_tcp_query(&wire)
    } else {
        proxy.resolver.prepare_udp_query(&wire)
    };
    let mut bytes = [0; 512];
    let (size, address) = tokio::select! {
        result=peer.recv_from(&mut bytes)=>result.unwrap(),
        result=&mut request=>panic!("DNS call returned before actual upstream wire: {result:?}"),
    };
    assert_eq!(&bytes[12..size], &wire[12..]);
    let original = loop {
        if let Some(record) = proxy
            .resolver
            .native_query_observations()
            .into_iter()
            .find(|record| record.io_pending && record.original.is_some())
        {
            break record;
        }
        tokio::task::yield_now().await;
    };
    let native = original.original.as_ref().unwrap();
    assert!(native.closed().is_none());
    assert!(!original.task.is_joined());
    let (second, mut sibling) = if tcp {
        proxy.resolver.prepare_tcp_query(&wire)
    } else {
        proxy.resolver.prepare_udp_query(&wire)
    };
    poll_fn(|cx| {
        assert!(sibling.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    first.cancel();
    assert_eq!(
        request.await.unwrap_err().kind(),
        std::io::ErrorKind::Interrupted
    );
    let mut first_close = Box::pin(first.wait_closed(end()));
    poll_fn(|cx| {
        assert!(first_close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(first.closed().is_none());
    assert!(
        native.closed().is_none(),
        "one cancelled subscriber must not close the shared query's original"
    );
    assert!(!original.task.is_joined());
    let mut actual_fault = None;
    if fault {
        let failed = zero_dns::test_support::panic_native_query_worker(&proxy.resolver).unwrap();
        assert!(failed
            .scope_identity()
            .same_scope(&original.task.scope_identity()));
        let joined = failed.wait().await;
        let source = joined.result().unwrap_err();
        second.cancel();
        assert_eq!(
            sibling.await.unwrap_err().kind(),
            std::io::ErrorKind::Interrupted
        );
        assert!(
            running.take().unwrap().shutdown().await.is_err(),
            "same actual critical fault stays observable"
        );
        assert!(std::sync::Arc::ptr_eq(
            &source,
            &failed.try_joined().unwrap().result().unwrap_err()
        ));
        actual_fault = Some(source);
    } else {
        let response = zero_dns::udp::build_dns_response(
            &bytes[..size],
            &[zero_traits::IpAddress::V4([203, 0, 113, 9])],
        );
        peer.send_to(&response, address).await.unwrap();
        let answered = sibling.await.unwrap();
        assert_eq!(&answered[..2], &wire[..2]);
        assert_eq!(&answered[answered.len() - 4..], &[203, 0, 113, 9]);
    }
    let first_closed = first_close.await.unwrap();
    let second_closed = second.wait_closed(end()).await.unwrap();
    for (call, closed) in [(&first, &first_closed), (&second, &second_closed)] {
        assert!(closed.matches(call));
        let query = closed.query().expect("actual installed DNS Host query");
        assert!(query.same_registration(&original.task));
        assert!(query.matches_joined(closed.joined().unwrap()));
        if fault {
            let original_failure = original.task.try_joined().unwrap().result().unwrap_err();
            assert!(std::sync::Arc::ptr_eq(
                closed.failure().unwrap(),
                &original_failure
            ));
            let cleanup = closed
                .cleanup_failure()
                .expect("retained typed finalizer fault");
            assert!(std::sync::Arc::ptr_eq(
                cleanup,
                call.closed().unwrap().cleanup_failure().unwrap()
            ));
        } else {
            assert!(closed.failure().is_none());
            assert!(closed.cleanup_failure().is_none());
        }
    }
    let native_closed = native
        .closed()
        .expect("same actual native original after query finalizer");
    assert!(native_closed.matches(native) && native_closed.had_native_acquisition());
    assert!(
        proxy.engine().active_sessions().is_empty()
            && proxy.engine().completed_sessions().is_empty()
    );
    if let Some(running) = running {
        running.shutdown().await.unwrap();
    }
    assert_eq!(actual_fault.is_some(), fault);
    assert!(!original.task.scope_identity().resource_open());
    let owner = native.owner();
    assert!(owner.closed().unwrap().matches(native.owner()));
    assert!(native
        .owner()
        .users_closed()
        .unwrap()
        .matches(native.owner()));
}
