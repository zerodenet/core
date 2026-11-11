//! External browser protocol fixture for the actual installed XHTTP Factory.
//! The GET and POST sockets are external peers, not Kernel IO/Scope evidence.
use super::*;
use std::sync::{Arc, Mutex};
use zero_platform_tokio::contract::CancellationToken;

#[derive(Default)]
struct Seen {
    session: Option<String>,
    sequences: Vec<u64>,
    acknowledgements: usize,
    gets: usize,
}
fn record(task: &serde_json::Value, seen: &Mutex<Seen>) {
    let url = task["url"].as_str().unwrap();
    let (_, path) = url
        .strip_prefix("http://")
        .unwrap()
        .split_once('/')
        .unwrap();
    let mut parts = path.split('?').next().unwrap().split('/');
    assert_eq!(parts.next(), Some("original"));
    let session = parts.next().unwrap();
    assert_eq!(session.len(), 32);
    let mut seen = seen.lock().unwrap();
    if let Some(actual) = &seen.session {
        assert_eq!(session, actual);
    } else {
        seen.session = Some(session.into());
    }
    match task["method"].as_str().unwrap() {
        "GET" => {
            assert!(parts.next().is_none());
            seen.gets += 1;
            assert_eq!(seen.gets, 1);
        }
        "POST" => {
            let sequence: u64 = parts.next().unwrap().parse().unwrap();
            assert!(parts.next().is_none());
            assert_eq!(sequence, seen.sequences.len() as u64);
            seen.sequences.push(sequence);
        }
        other => panic!("unexpected actual browser task {other}"),
    }
}
async fn request(task: &serde_json::Value, body: &[u8]) -> TcpStream {
    let (authority, path) = task["url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://")
        .unwrap()
        .split_once('/')
        .unwrap();
    let mut io = TcpStream::connect(authority).await.unwrap();
    let mut header = format!(
        "{} /{} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
        task["method"].as_str().unwrap(),
        path,
        authority,
        body.len()
    );
    if let Some(headers) = task["extra"]["headers"].as_object() {
        for (name, value) in headers {
            assert!(!matches!(
                name.as_str(),
                "host" | "connection" | "content-length"
            ));
            header.push_str(name);
            header.push_str(": ");
            header.push_str(value.as_str().unwrap());
            header.push_str("\r\n");
        }
    }
    header.push_str("\r\n");
    io.write_all(header.as_bytes()).await.unwrap();
    io.write_all(body).await.unwrap();
    io.flush().await.unwrap();
    io
}
async fn line(io: &mut TcpStream) -> Option<String> {
    let mut bytes = Vec::new();
    loop {
        match io.read_u8().await {
            Ok(value) => bytes.push(value),
            Err(source)
                if bytes.is_empty() && source.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                return None
            }
            Err(source) => panic!("real HTTP response line failed: {source}"),
        }
        assert!(bytes.len() <= 8192);
        if bytes.ends_with(b"\r\n") {
            bytes.truncate(bytes.len() - 2);
            return Some(String::from_utf8(bytes).unwrap());
        }
    }
}
async fn response(io: &mut TcpStream) -> bool {
    let status = line(io).await.unwrap();
    assert!(status.starts_with("HTTP/1.1 200 "), "{status}");
    let mut chunked = false;
    loop {
        let header = line(io).await.unwrap();
        if header.is_empty() {
            return chunked;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("transfer-encoding") {
                assert_eq!(value.trim(), "chunked");
                chunked = true;
            }
        }
    }
}
async fn get(mut control: Agent, task: serde_json::Value, stop: CancellationToken) {
    // Matches the shipped page: assignment ACK precedes fetch/response headers.
    control.send(Message::Text("ok".into())).await.unwrap();
    let work = async {
        let mut io = request(&task, &[]).await;
        let chunked = response(&mut io).await;
        loop {
            let bytes = if chunked {
                let Some(size) = line(&mut io).await else {
                    break;
                };
                let size = usize::from_str_radix(size.split(';').next().unwrap(), 16).unwrap();
                if size == 0 {
                    break;
                }
                assert!(size <= 1024 * 1024);
                let mut bytes = vec![0; size];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(line(&mut io).await.as_deref(), Some(""));
                bytes
            } else {
                let mut bytes = vec![0; 16384];
                let n = io.read(&mut bytes).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes.truncate(n);
                bytes
            };
            control.send(Message::Binary(bytes)).await.unwrap();
        }
    };
    // Closing the controlled GET drops the external fetch TCP stream. This is
    // only fixture behavior; the installed Source still has its own real ACKs.
    tokio::select! {biased;_=stop.cancelled()=>{},_=work=>{}}
    let _ = control.close(None).await;
}
async fn post(mut control: Agent, task: serde_json::Value, seen: Arc<Mutex<Seen>>) {
    control.send(Message::Text("ok".into())).await.unwrap();
    let bytes = loop {
        match control.next().await.unwrap().unwrap() {
            Message::Binary(bytes) => break bytes,
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("expected actual POST payload, got {other:?}"),
        }
    };
    let mut io = request(&task, &bytes).await;
    response(&mut io).await;
    drop(io);
    control.send(Message::Text("ok".into())).await.unwrap();
    seen.lock().unwrap().acknowledgements += 1;
    let _ = control.close(None).await;
}
async fn browser_tasks(stop: CancellationToken, seen: Arc<Mutex<Seen>>) {
    let url = browser_url().await;
    let mut idle = register(&url).await;
    let mut work = tokio::task::JoinSet::new();
    loop {
        tokio::select! {biased;
            _=stop.cancelled()=>break,
            task=idle.next()=>{
                let task=task.unwrap().unwrap().into_text().unwrap();
                let task:serde_json::Value=serde_json::from_str(&task).unwrap();record(&task,&seen);
                let active=idle;
                if task["method"]=="GET" {work.spawn(get(active,task,stop.clone()));}
                else {work.spawn(post(active,task,seen.clone()));}
                // Like the actual page, replenish exactly its original one
                // idle browser after assignment, not after the GET completes.
                idle=register(&url).await;
            }
            result=work.join_next(),if !work.is_empty()=>{result.unwrap().unwrap();}
        }
    }
    let _ = idle.close(None).await;
    while let Some(result) = work.join_next().await {
        result.unwrap();
    }
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
    assert_eq!(node.recipe.spec().task_limit.unwrap().get(), 2);
    let owner = node.original_carrier_owner(0).unwrap();
    assert_eq!(owner.limit().get(), 1);
    let observed = owner.observe().unwrap();
    assert_eq!(observed.retained(), 1);
    let role = &observed.slots()[0];
    let setup = role.auxiliary().unwrap();
    let joined = setup.try_joined().unwrap();
    assert!(setup.matches_joined(&joined) && joined.registration_released());
    let driver = &role.driver().unwrap().1;
    assert!(!driver.is_joined());
    assert!(driver.scope_identity().same_scope(&setup.scope_identity()));
    assert!(role.native().is_none());
    let source = role.borrowed_origin().unwrap().clone();
    let accepted = source.accepted().unwrap();
    assert!(!accepted.is_settled());
    let provider = plan.component(accepted.instance().component).unwrap();
    assert!(node
        .dependency_instances()
        .contains(&provider.instance_id()));
    assert!(accepted.matches_receiver(&provider.original_accepted_receiver(0).unwrap()));
    assert!(!accepted
        .scope_identity()
        .same_scope(&driver.scope_identity()));
    Actual {
        owner,
        role: role.original().clone(),
        ordinal: role.ordinal(),
        source,
    }
}
#[tokio::test]
async fn ordinary_browser_xhttp_one_actual_get_orders_multiple_posts_before_parent_driver_exit_and_returns_every_source(
) {
    let _serial = SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    tokio::time::timeout(Duration::from_secs(20),async {
        let remote=fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"split_http":{"path":"/original/","mode":"auto"}}}],"route":{"final":{"type":"direct"}}}),"remote").await;
        let client=fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"split_http":{"path":"/original/","mode":"packet-up","sc_max_each_post_bytes":16384,"sc_min_posts_interval_ms":1,"browser_dialer":{"listen":BROWSER,"idle_capacity":1}}}}],"route":{"final":{"type":"route","outbound":"node"}}}),"entry").await;
        let stop=CancellationToken::new();let seen=Arc::new(Mutex::new(Seen::default()));let browser=tokio::spawn(browser_tasks(stop.clone(),seen.clone()));
        let target=TcpListener::bind("127.0.0.1:0").await.unwrap();let destination=target.local_addr().unwrap();let payload=vec![0x73;65537];let expected=payload.clone();
        let (uploaded,ready)=tokio::sync::oneshot::channel();let (release,wait)=tokio::sync::oneshot::channel();
        let target=tokio::spawn(async move{let(mut io,_)=target.accept().await.unwrap();let mut body=vec![0;expected.len()];io.read_exact(&mut body).await.unwrap();assert_eq!(body,expected);uploaded.send(()).unwrap();wait.await.unwrap();io.write_all(&body).await.unwrap();io.shutdown().await.unwrap();});
        let mut io=TcpStream::connect(client.address).await.unwrap();io.write_all(&[5,1,0]).await.unwrap();let mut auth=[0;2];io.read_exact(&mut auth).await.unwrap();assert_eq!(auth,[5,0]);
        let mut request=vec![5,1,0,1,127,0,0,1];request.extend_from_slice(&destination.port().to_be_bytes());io.write_all(&request).await.unwrap();let mut reply=[0;10];io.read_exact(&mut reply).await.unwrap();assert_eq!(reply[1],0);
        io.write_all(&payload).await.unwrap();io.flush().await.unwrap();ready.await.unwrap();
        loop {
            let complete = {let seen=seen.lock().unwrap();seen.sequences.len()>=2 && seen.acknowledgements==seen.sequences.len()};
            if complete {break;}tokio::task::yield_now().await;
        }
        let actual=capture(&client);
        {let seen=seen.lock().unwrap();assert_eq!(seen.gets,1);assert!(seen.sequences.len()>=2);assert_eq!(seen.acknowledgements,seen.sequences.len());}
        // The second actual ordered POST was impossible unless the first's
        // Source global reservation returned while this parent driver stayed
        // running. The GET Source remains independently charged and live.
        assert!(!actual.source.accepted().unwrap().is_settled());
        release.send(()).unwrap();let mut body=vec![0;payload.len()];io.read_exact(&mut body).await.unwrap();assert_eq!(body,payload);io.shutdown().await.unwrap();drop(io);target.await.unwrap();
        returned(&actual).await;stop.cancel();browser.await.unwrap();
        let roots=client.proxy.protocols.runtime_host().original_budgets();client.close().await;assert!(actual.owner.closed().unwrap().matches(&actual.owner));assert_eq!(roots.carrier.snapshot().charged,0);remote.close().await;
    }).await.unwrap();
}
