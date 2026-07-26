use super::*;

const NEW_USER_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

#[tokio::test]
async fn vless_user_reload_updates_live_listener_without_rebind() {
    let upstream_port = free_port();
    let proxy_port = free_port();
    let upstream = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", upstream_port))
            .await
            .expect("bind upstream");
        let mut connections = Vec::new();
        loop {
            let (connection, _) = listener.accept().await.expect("accept upstream");
            connections.push(connection);
        }
    });
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "vless-in",
                "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                "protocol": {{
                    "type": "vless",
                    "users": [{{
                        "id": "{USER_ID}",
                        "principal_key": "account:old"
                    }}]
                }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config");
    let directory = tempfile::tempdir().expect("temp dir");
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("serialize operator config"),
    )
    .expect("write operator config");
    let core = zero_engine::Engine::new_with_config_path(config, &config_path)
        .expect("build persistent engine");
    let proxy = Engine::from_engine(core).expect("build proxy");
    let command_handle = zero_proxy::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = spawn_engine(proxy);
    wait_for_listener(proxy_port).await;

    let mut old_client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect old user");
    old_client
        .write_all(&vless_request_for_ipv4(
            USER_ID,
            [127, 0, 0, 1],
            upstream_port,
        ))
        .await
        .expect("write old VLESS request");
    let mut old_response = [0u8; 2];
    old_client
        .read_exact(&mut old_response)
        .await
        .expect("read old VLESS response");
    assert_eq!(old_response, [0, 0]);
    wait_for("old user active session", || {
        !running.active_sessions().is_empty()
    })
    .await;

    let mut updated = (*command_handle.engine_handle().inner().config()).clone();
    let zero_config::InboundProtocolConfig::Vless { users, .. } = &mut updated.inbounds[0].protocol
    else {
        panic!("expected VLESS inbound");
    };
    *users = vec![zero_config::VlessUserConfig {
        id: NEW_USER_ID.to_owned(),
        flow: None,
        principal_key: Some("account:new".to_owned()),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: Some(1),
    }];
    command_handle
        .apply_runtime_config_with_principal_impact_and_wait(
            updated,
            vec!["account:old".to_owned()],
            Vec::new(),
            std::time::Duration::from_secs(5),
        )
        .await
        .expect("reload users");
    let mut closed_byte = [0u8; 1];
    let close_result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        old_client.read(&mut closed_byte),
    )
    .await
    .expect("old user close timeout");
    let old_user_closed = match close_result {
        Ok(0) => true,
        Err(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
        ),
        Ok(_) => false,
    };
    assert!(old_user_closed, "old user connection stayed open");

    assert!(
        handshake(proxy_port, upstream_port, NEW_USER_ID).await,
        "acknowledged replacement user was not accepted"
    );
    assert!(
        !handshake(proxy_port, upstream_port, USER_ID).await,
        "removed user remained authorized"
    );
    let persisted = RuntimeConfig::load_from_path(&config_path).expect("load operator config");
    let zero_config::InboundProtocolConfig::Vless { users, .. } = &persisted.inbounds[0].protocol
    else {
        panic!("expected VLESS inbound");
    };
    assert_eq!(users[0].principal_key.as_deref(), Some("account:old"));

    running.shutdown().await.expect("shutdown proxy");
    upstream.abort();
}

async fn handshake(proxy_port: u16, upstream_port: u16, user_id: &str) -> bool {
    let Ok(mut client) = TcpStream::connect(("127.0.0.1", proxy_port)).await else {
        return false;
    };
    if client
        .write_all(&vless_request_for_ipv4(
            user_id,
            [127, 0, 0, 1],
            upstream_port,
        ))
        .await
        .is_err()
    {
        return false;
    }
    let mut response = [0u8; 2];
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.read_exact(&mut response)
        )
        .await,
        Ok(Ok(_)) if response == [0, 0]
    )
}
