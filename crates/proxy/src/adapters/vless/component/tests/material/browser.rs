//! Whole ordinary registry/actual Host/real browser WS and exact Source return.
use super::fixture::{self, Running};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use zero_platform_tokio::controlled_carrier::{
    CarrierOrigin, CarrierOriginObservation, CarrierOwnerRegistration,
};
#[path = "browser/http.rs"]
mod http;
static SERIAL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
const ID: &str = "11111111-2222-3333-4444-555555555555";
// This is an actual accepted configuration address, not a claimed free-port
// reservation. The Source's real Host bind is the only bind; a collision must
// return its real error. No helper probes, drops or rebinds a reserved socket.
const BROWSER: &str = "127.0.0.1:16888";
type Agent = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
async fn browser_url() -> String {
    let mut page = TcpStream::connect(BROWSER).await.unwrap();
    page.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = Vec::new();
    page.read_to_end(&mut body).await.unwrap();
    drop(page);
    let page = String::from_utf8(body).unwrap();
    let marker = "/websocket?token=";
    let start = page.find(marker).unwrap();
    let end = page[start..].find('"').unwrap() + start;
    format!("ws://{BROWSER}{}", &page[start..end])
}
async fn register(url: &str) -> Agent {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", format!("http://{BROWSER}").parse().unwrap());
    connect_async(request).await.unwrap().0
}
async fn browser() -> Agent {
    register(&browser_url().await).await
}
async fn relay_browser(mut browser: Agent) {
    let task = browser.next().await.unwrap().unwrap().into_text().unwrap();
    let task: serde_json::Value = serde_json::from_str(&task).unwrap();
    assert_eq!(task["method"], "WS");
    assert!(task["url"].as_str().unwrap().ends_with("/original"));
    let (mut remote, _) = connect_async(task["url"].as_str().unwrap()).await.unwrap();
    browser.send(Message::Text("ok".into())).await.unwrap();
    loop {
        tokio::select! {
            frame=browser.next()=>match frame {Some(Ok(Message::Binary(bytes)))=>remote.send(Message::Binary(bytes)).await.unwrap(),Some(Ok(Message::Close(_)))|None=>break,Some(Ok(_))=>{},Some(Err(source))=>panic!("actual browser control failed: {source}")},
            frame=remote.next()=>match frame {Some(Ok(Message::Binary(bytes)))=>browser.send(Message::Binary(bytes)).await.unwrap(),Some(Ok(Message::Close(_)))|None=>break,Some(Ok(_))=>{},Some(Err(source))=>panic!("actual remote WS failed: {source}")},
        }
    }
    let _ = browser.close(None).await;
    let _ = remote.close(None).await;
}
struct Actual {
    owner: CarrierOwnerRegistration,
    role: CarrierOriginObservation,
    ordinal: u64,
    source: CarrierOrigin,
}
fn capture(client: &Running) -> Actual {
    let services = client.proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let node = plan.configured("node").unwrap();
    assert!(plan.has_cached_stream_provider(node.role_id()));
    assert!(plan.has_carrier_provider(node.role_id()));
    assert!(
        plan.stream_dependencies_supported(node.role_id()),
        "same installed Source is an actual supported Stream dependency"
    );
    assert_eq!(
        node.recipe.spec().task_limit.unwrap().get(),
        2,
        "original N1 setup plus data-driver ceiling, not a third task"
    );
    let owner = node.original_carrier_owner(0).unwrap();
    assert_eq!(owner.limit().get(), 1);
    let observed = owner.observe().unwrap();
    assert_eq!(observed.retained(), 1);
    let role = &observed.slots()[0];
    assert!(
        role.native().is_none() && role.driver().is_none(),
        "WS transfers the actual Source IO without a fabricated cached driver"
    );
    let setup = role.auxiliary().unwrap();
    let joined = setup.try_joined().unwrap();
    assert!(setup.matches_joined(&joined) && joined.registration_released());
    assert!(setup
        .scope_identity()
        .same_scope(&owner.bound().unwrap().scope_identity()));
    let source = role.borrowed_origin().unwrap().clone();
    let accepted = source.accepted().unwrap();
    assert!(!accepted.is_settled());
    let provider = plan.component(accepted.instance().component).unwrap();
    assert_eq!(provider.instance_id(), accepted.instance());
    assert!(node
        .dependency_instances()
        .contains(&provider.instance_id()));
    assert!(provider.recipe.spec().roles.is_empty());
    let receiver = provider.original_accepted_receiver(0).unwrap();
    assert!(accepted.matches_receiver(&receiver));
    assert!(receiver
        .task_failures()
        .unwrap()
        .scope_identity()
        .same_scope(accepted.scope_identity()));
    assert!(!accepted
        .scope_identity()
        .same_scope(&setup.scope_identity()));
    assert!(source.closed().is_none());
    Actual {
        owner,
        role: role.original().clone(),
        ordinal: role.ordinal(),
        source,
    }
}
async fn returned(actual: &Actual) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if actual.role.closed().is_some()
                && actual.source.accepted().unwrap().is_settled()
                && actual
                    .owner
                    .observe()
                    .is_some_and(|owner| owner.retained() == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let role = actual.role.closed().unwrap();
    assert!(actual.role.matches_closed(&role));
    assert!(!role.had_native_acquisition());
    let source = actual.source.closed().unwrap();
    assert!(source.matches(&actual.source));
    assert!(source.had_native_acquisition());
}
async fn wire(client: &Running, sequence: u8) -> Actual {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = target.local_addr().unwrap();
    let payload = vec![sequence; 1025];
    let expected = payload.clone();
    let (uploaded, ready) = tokio::sync::oneshot::channel();
    let (release, waiting) = tokio::sync::oneshot::channel();
    let target = tokio::spawn(async move {
        let (mut io, _) = target.accept().await.unwrap();
        let mut body = vec![0; expected.len()];
        io.read_exact(&mut body).await.unwrap();
        assert_eq!(body, expected);
        uploaded.send(()).unwrap();
        waiting.await.unwrap();
        io.write_all(&body).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let browser = tokio::spawn(relay_browser(browser().await));
    let mut io = TcpStream::connect(client.address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&destination.port().to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    io.write_all(&payload).await.unwrap();
    io.flush().await.unwrap();
    ready.await.unwrap();
    let actual = capture(client);
    release.send(()).unwrap();
    let mut reverse = vec![0; payload.len()];
    io.read_exact(&mut reverse).await.unwrap();
    assert_eq!(reverse, payload);
    io.shutdown().await.unwrap();
    drop(io);
    target.await.unwrap();
    browser.await.unwrap();
    returned(&actual).await;
    actual
}
#[tokio::test]
async fn ordinary_browser_ws_registry_reuses_one_actual_source_after_first_business_and_closes_both_original_scopes(
) {
    let _serial = SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    tokio::time::timeout(Duration::from_secs(20),async {
        let remote=fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"ws":{"path":"/original"}}}],"route":{"final":{"type":"direct"}}}),"remote").await;
        let client=fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"ws":{"path":"/original?ed=2048","browser_dialer":{"listen":BROWSER,"idle_capacity":1}}}}],"route":{"final":{"type":"route","outbound":"node"}}}),"entry").await;
        let first=wire(&client,0x39).await;let second=wire(&client,0x71).await;
        assert!(first.owner.same_owner(&second.owner));assert!(!first.source.same_original(&second.source));assert_eq!(first.source.accepted().unwrap().instance(),second.source.accepted().unwrap().instance());
        assert_ne!(first.ordinal,second.ordinal);
        let roots=client.proxy.protocols.runtime_host().original_budgets();client.close().await;
        assert!(second.owner.closed().unwrap().matches(&second.owner));assert_eq!(roots.carrier.snapshot().charged,0);
        remote.close().await;
    }).await.unwrap();
}
