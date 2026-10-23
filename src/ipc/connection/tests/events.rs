use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use zero_api::RawApiEvent;
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

use crate::ipc::connection::handle_ipc_connection;

fn handles() -> (EngineHandle, ProxyHandle) {
    let proxy = Proxy::new(
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let engine = EngineHandle::new(proxy.engine().clone());
    (engine.clone(), ProxyHandle::new(engine, proxy))
}

async fn read_frame(reader: &mut BufReader<tokio::io::DuplexStream>) -> Value {
    let mut line = String::new();
    assert!(reader.read_line(&mut line).await.unwrap() > 0);
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn ipc_event_bursts_drain_in_order_without_delaying_queries() {
    let (engine, handle) = handles();
    let (client, server) = tokio::io::duplex(4096);
    let task = tokio::spawn(handle_ipc_connection(server, handle));
    let mut client = BufReader::new(client);
    client
        .get_mut()
        .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.burst\"]}\n")
        .await
        .unwrap();
    assert_eq!(read_frame(&mut client).await["ok"], true);

    for index in 0..128 {
        engine.emit(RawApiEvent::new(
            format!("burst-{index}"),
            "test.burst",
            0,
            json!({"index": index}),
        ));
    }
    client
        .get_mut()
        .write_all(b"{\"type\":\"ping\",\"id\":\"during-burst\"}\n")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        let mut next = 0;
        let mut ping_seen = false;
        while next < 128 || !ping_seen {
            let frame = read_frame(&mut client).await;
            if frame["id"] == "during-burst" {
                assert_eq!(frame["ok"], true);
                ping_seen = true;
            } else {
                assert_eq!(frame["event_type"], "test.burst");
                assert_eq!(frame["payload"]["index"], next);
                next += 1;
            }
        }
    })
    .await
    .expect("a ready event burst must not be throttled to ten events per second");
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stalled_ipc_event_receiver_can_disconnect_during_a_burst() {
    let (engine, handle) = handles();
    let (client, server) = tokio::io::duplex(256);
    let task = tokio::spawn(handle_ipc_connection(server, handle));
    let mut client = BufReader::new(client);
    client
        .get_mut()
        .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.burst\"]}\n")
        .await
        .unwrap();
    assert_eq!(read_frame(&mut client).await["ok"], true);
    for index in 0..128 {
        engine.emit(RawApiEvent::new(
            format!("burst-{index}"),
            "test.burst",
            0,
            json!({"index": index}),
        ));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("a full event bridge must not prevent connection cleanup")
        .unwrap()
        .unwrap();
}
