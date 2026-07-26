use super::*;

#[tokio::test]
async fn relays_tcp_through_urltest_group_after_probe_selects_direct() {
    let echo_port = free_port();
    let probe_port = free_port();
    let outer_port = free_port();
    let unreachable_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });
    let probe_task = spawn_http_probe_server(probe_port);
    wait_for_listener(probe_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "chain-a",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {unreachable_port}
                    }}
                }},
                {{
                    "tag": "direct",
                    "protocol": {{ "type": "direct" }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "proxy",
                    "type": "url_test",
                    "outbounds": ["chain-a", "direct"],
                    "url": "http://127.0.0.1:{probe_port}/",
                    "interval_seconds": 1
                }}
            ],
            "mode": {{
                "type": "global",
                "outbound": "proxy"
            }},
            "route": {{
                "rules": [],
                "final": {{ "type": "reject" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let event_handle = EngineHandle::new(outer_engine.engine().clone());
    let subscriber = event_handle
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to urltest events");
    let outer_handle = spawn_engine(outer_engine);

    wait_for_listener(outer_port).await;
    wait_for_group_selection(&outer_handle, "proxy", "direct").await;

    let startup_event = wait_for_probe_event(&subscriber, "startup").await;
    assert_eq!(startup_event.payload["selected"], "direct");
    assert_eq!(
        startup_event.payload["members"].as_array().unwrap().len(),
        2
    );
    assert!(startup_event.payload["completed_at_unix_ms"]
        .as_u64()
        .is_some());

    let scheduled_event = wait_for_probe_event(&subscriber, "scheduled").await;
    assert_eq!(scheduled_event.payload["selected"], "direct");

    outer_handle
        .engine()
        .trigger_urltest_probe("proxy")
        .expect("trigger manual urltest probe");
    let manual_event = wait_for_probe_event(&subscriber, "manual").await;
    assert_eq!(manual_event.payload["selected"], "direct");

    let status = outer_handle.export_status();
    let group = status
        .config
        .outbound_groups
        .iter()
        .find(|group| group.tag == "proxy")
        .expect("find urltest group");
    assert_eq!(group.selected.as_deref(), Some("direct"));
    assert!(group.latency_ms.is_some());
    assert!(group.last_checked_unix_ms.is_some());
    assert_eq!(
        group.effective_chains,
        vec![vec!["proxy".to_owned(), "direct".to_owned()]]
    );
    assert_eq!(group.url_test_members.len(), 2);
    let chain_a = group
        .url_test_members
        .iter()
        .find(|member| member.member_tag == "chain-a")
        .expect("find chain-a probe");
    assert!(!chain_a.healthy);
    assert!(chain_a.last_error.is_some());
    let direct = group
        .url_test_members
        .iter()
        .find(|member| member.member_tag == "direct")
        .expect("find direct probe");
    assert!(direct.healthy);
    assert!(direct.latency_ms.is_some());
    assert_eq!(direct.effective_chains, vec![vec!["direct".to_owned()]]);

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");

    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        127,
        0,
        0,
        1,
        ((echo_port >> 8) & 0xff) as u8,
        (echo_port & 0xff) as u8,
    ];
    client.write_all(&request).await.expect("write request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response[1], 0x00);

    client.write_all(b"fast").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"fast");

    let config_without_urltest = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "outer-socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [{{
                "tag": "direct",
                "protocol": {{ "type": "direct" }}
            }}],
            "mode": {{ "type": "global", "outbound": "direct" }},
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config without urltest");
    outer_handle
        .engine()
        .reload_config(config_without_urltest)
        .expect("reload config without urltest");
    wait_for("removed urltest trigger to be cleared", || {
        outer_handle
            .engine()
            .trigger_urltest_probe("proxy")
            .is_err()
    })
    .await;

    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    probe_task.abort();
    let _ = echo_task.await;
}

async fn wait_for_probe_event(subscriber: &EventSubscriber, trigger: &str) -> RawApiEvent {
    // A probe may spend up to five seconds on each member. Keep the test
    // window above that production bound so a slow unreachable member does
    // not hide the completion event on platforms with delayed connect errors.
    for _ in 0..600 {
        if let Some(event) = subscriber.try_recv() {
            if event.payload["trigger"] == trigger {
                return event;
            }
        }
        sleep(Duration::from_millis(20)).await;
    }

    panic!("did not receive urltest event with trigger `{trigger}`");
}
