//! Actual standard Sources, original Host material and accepted TCP/UDP callers.
use super::framed::{authenticate, exercise, start};
use std::{future::Future, net::SocketAddr, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn half_close(endpoint: SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let payload = vec![0x3a; 131073];
    let expected = payload.clone();
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let original = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut received = vec![0; expected.len()];
        io.read_exact(&mut received).await.unwrap();
        assert_eq!(received, expected);
        let mut tail = Vec::new();
        {
            let mut read = std::pin::pin!(io.read_to_end(&mut tail));
            // Establish suspension of this real original read, before the client
            // sends FIN. A sleep or worker-registration marker would not do that.
            std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(result) => panic!("original read ended before FIN: {result:?}"),
            })
            .await;
            pending_tx.send(()).unwrap();
            read.await.unwrap();
        }
        assert!(tail.is_empty());
        io.write_all(&received).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let mut io = authenticate(endpoint).await;
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend(port.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..2], &[5, 0]);
    io.write_all(&payload).await.unwrap();
    io.flush().await.unwrap();
    pending_rx.await.unwrap();
    io.shutdown().await.unwrap();
    let mut response = Vec::new();
    io.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, payload);
    original.await.unwrap();
}

#[tokio::test]
async fn native_mask_sources_keep_custom_sudoku_fragment_tcp_udp_and_original_close() {
    for carrier in [
        serde_json::json!({}),
        serde_json::json!({"ws":{"path":"/masks"}}),
        serde_json::json!({"h2":{"path":"/masks"}}),
        serde_json::json!({"http_upgrade":{"path":"/masks"}}),
        serde_json::json!({"grpc":{"service_names":["masks/Tun"]}}),
        serde_json::json!({"split_http":{"path":"/masks/","mode":"stream-one"}}),
        serde_json::json!({"split_http":{"path":"/masks/","mode":"stream-up"}}),
        serde_json::json!({"split_http":{"path":"/masks/","mode":"packet-up"}}),
        serde_json::json!({"mux_concurrency":2,"xudp_concurrency":2}),
        serde_json::json!({"ws":{"path":"/masks"},"mux_concurrency":2,"xudp_concurrency":2}),
    ] {
        let masks = serde_json::json!({"tcp":[
            {"type":"header_custom",
             "clients":[[{"content":{"type":"bytes","bytes":[67,76]}}]],
             "servers":[[{"content":{"type":"bytes","bytes":[83,86]}}]]},
            {"type":"fragment","packets":{"minimum":1,"maximum":2},
             "length":{"minimum":2,"maximum":3},"max_splits":{"minimum":2,"maximum":2}},
            {"type":"sudoku","password":"reference","custom_tables":["xxppvvvv","xpxpvvvv"],
             "padding_min":10,"padding_max":20}
        ]});
        let mut inbound = serde_json::json!({"type":"vless","users":[{"id":"host-mask-source"}],"final_mask":masks});
        let mut inbound_carrier = carrier.as_object().unwrap().clone();
        inbound_carrier.remove("mux_concurrency");
        inbound_carrier.remove("xudp_concurrency");
        inbound.as_object_mut().unwrap().extend(inbound_carrier);
        let (remote, remote_running, endpoint) = start(serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":inbound}],
            "route":{"final":{"type":"direct"}}
        })).await;
        let book = remote
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .buffer_budget();
        let material = book.used_bytes();
        assert!(
            material > 0,
            "the actual Source paid material before the first connection"
        );
        let cold = remote
            .protocols
            .prepare_inbound_bind(remote.engine().config().inbounds[0].clone(), None)
            .unwrap()
            .unwrap();
        let recipe = cold.into_recipe();
        let spec = recipe.spec();
        assert!(
            spec.task_limit.is_none(),
            "masks add no private worker or internal task slot"
        );
        let accepted = spec
            .resources
            .iter()
            .find_map(|resource| match resource {
                zero_platform_tokio::contract::component::ResourceDeclaration::TcpListener {
                    accepted,
                    ..
                } => *accepted,
                _ => None,
            })
            .unwrap();
        assert_eq!(
            accepted.max_tasks_per_socket.get(),
            520 + if carrier.get("split_http").is_some() {
                zero_transport::split_http::SplitHttpRegistry::MAX_SESSIONS
            } else {
                0
            },
            "cold registration keeps the real finite session capacity"
        );
        assert_eq!(
            book.used_bytes(),
            material,
            "cold description does not rebuild unpaid tables"
        );
        let mut outbound = serde_json::json!({"type":"vless","server":"127.0.0.1","port":endpoint.port(),
            "id":"host-mask-source","final_mask":masks});
        outbound
            .as_object_mut()
            .unwrap()
            .extend(carrier.as_object().unwrap().clone());
        let (client, client_running, socks) = start(serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"upstream","protocol":outbound}],
            "route":{"final":{"type":"route","outbound":"upstream"}}
        })).await;
        let local_owner = client.system_tun_source_registry().owner("entry").unwrap();
        let remote_owner = remote.system_tun_source_registry().owner("entry").unwrap();
        let scope = remote_owner.original_scope_for_test();
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            exercise(socks).await;
            half_close(socks).await;
        })
        .await;
        client_running.shutdown().await.unwrap();
        remote_running.shutdown().await.unwrap();
        assert!(result.is_ok(), "native masked carrier: {carrier}");
        assert!(!scope.resource_open());
        for owner in [local_owner, remote_owner] {
            let original = owner.terminal_for_test().unwrap();
            assert!(original.failure().is_none());
            assert!(original.closed_io_receipts() > 0 && original.joined_tasks() > 0);
            assert!(
                original.task_failures().is_empty() && original.native_bound_failures().is_empty()
            );
        }
        // The buffer book is an independent material ledger. Native close was
        // established above from those original owners, never from this refund.
        drop(client);
        drop(remote);
        assert_eq!(book.used_bytes(), 0);
    }
}

#[test]
fn xhttp_original_session_capacity_and_explicit_work_caps_share_reload_classification() {
    for requested in [None, Some(64), Some(1544)] {
        let mut value = serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{
                "type":"vless","users":[{"id":"host-mask-source"}],
                "split_http":{"path":"/sessions/","mode":"packet-up"}
            }}],"route":{"final":{"type":"direct"}}
        });
        if let Some(tasks) = requested {
            value["runtime"] = serde_json::json!({"inbound_max_tasks_per_carrier":tasks});
        }
        let config = zero_config::RuntimeConfig::parse(&value.to_string()).unwrap();
        let proxy = crate::Proxy::new(config.clone()).unwrap();
        let cold = proxy
            .protocols
            .prepare_inbound_bind_in_config(config.inbounds[0].clone(), &config)
            .unwrap()
            .unwrap();
        let expected =
            requested.unwrap_or(520 + zero_transport::split_http::SplitHttpRegistry::MAX_SESSIONS);
        assert!(cold.resources().iter().any(|resource| matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::TcpListener {
                accepted: Some(limits), ..
            } if limits.max_tasks_per_socket.get() == expected)));
        let mut explicit = config.clone();
        explicit.runtime.inbound_max_tasks_per_carrier = Some(expected);
        assert!(
            !crate::adapters::inbound_work_budgets::inbound_work_source_changed(
                &config.inbounds[0],
                &config,
                &explicit
            ),
            "same effective actual budget is not a replacement"
        );
        explicit.runtime.inbound_max_tasks_per_carrier = Some(expected + 1);
        assert!(
            crate::adapters::inbound_work_budgets::inbound_work_source_changed(
                &config.inbounds[0],
                &config,
                &explicit
            ),
            "changed actual session task cap needs original retirement"
        );
    }
}
