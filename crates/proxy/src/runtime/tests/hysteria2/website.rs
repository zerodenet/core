//! Appearance traffic uses original host file/origin providers, never proxy flows.
use super::inbound_tests::{connect, material, ready_proxy};
use crate::runtime::test_listener;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};
async fn request(address: std::net::SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut result = String::new();
    socket.read_to_string(&mut result).await.unwrap();
    result
}
fn reserve_sites(
    value: &mut serde_json::Value,
    hostname: bool,
) -> (std::net::TcpListener, std::net::TcpListener) {
    let http =
        std::net::TcpListener::bind((if hostname { "localhost" } else { "127.0.0.1" }, 0)).unwrap();
    let https = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let appearance = &mut value["inbounds"][0]["protocol"]["masquerade"];
    appearance["http"] = serde_json::json!({"address":if hostname {"localhost"}else{"127.0.0.1"},"port":http.local_addr().unwrap().port()});
    appearance["https"] =
        serde_json::json!({"address":"127.0.0.1","port":https.local_addr().unwrap().port()});
    (http, https)
}
#[tokio::test]
async fn standard_website_hostname_file_and_original_cleanup() {
    timeout(Duration::from_secs(15),async{
    let (dir,mut value)=material();std::fs::write(dir.path().join("index.html"),"owned file").unwrap();
    value["inbounds"][0]["protocol"]["masquerade"]=serde_json::json!({"type":"file","dir":dir.path(),"force_https":false,"http":{"address":"localhost","port":0}});
    let reserved=reserve_sites(&mut value,true);
    let proxy=ready_proxy(&value).await;
    let running=test_listener::start_with_handoff(&proxy,proxy.engine().config().inbounds[0].clone(),move||drop(reserved)).await;
    assert_eq!(running.auxiliary.len(),2);let response=request(running.auxiliary[0],"/").await;assert!(response.ends_with("owned file"),"{response}");
    // H3 appearance uses the same original FileTree provider before authentication.
    let (client,conn)=connect(running.address,b"h3").await;let (mut driver,mut sender)=h3::client::new(h3_quinn::Connection::new(conn.clone())).await.unwrap();let task=tokio::spawn(async move{std::future::poll_fn(|cx|driver.poll_close(cx)).await});
    let mut stream=sender.send_request(http_uri::Request::builder().uri("https://localhost/").body(()).unwrap()).await.unwrap();stream.finish().await.unwrap();assert_eq!(stream.recv_response().await.unwrap().status(),200);let data=stream.recv_data().await.unwrap().unwrap();use bytes::Buf;let mut data=data;assert_eq!(data.copy_to_bytes(data.remaining()).as_ref(),b"owned file");drop(stream);
    drop(sender);conn.close(0u32.into(),b"");client.close(0u32.into(),b"");task.abort();let _=task.await;client.wait_idle().await;
    assert_eq!(proxy.engine().stats_snapshot().total_started,0,"appearance requests are not proxy flows");
    test_listener::close(&proxy,"hy",running).await;
    proxy.protocols.shutdown_components(tokio::time::Instant::now()+Duration::from_secs(3)).await.unwrap();
}).await.unwrap();
}
#[tokio::test]
async fn standard_website_proxy_owns_pending_origin_until_confirmed_stop() {
    timeout(Duration::from_secs(15),async{
    let (_dir,mut value)=material();let origin=TcpListener::bind("127.0.0.1:0").await.unwrap();let port=origin.local_addr().unwrap().port();
    value["inbounds"][0]["protocol"]["masquerade"]=serde_json::json!({"type":"proxy","force_https":false,"url":format!("http://127.0.0.1:{port}/base?fixed=yes"),"rewrite_host":false,"x_forwarded":true,"http":{"address":"127.0.0.1","port":0}});
    let reserved=reserve_sites(&mut value,false);let proxy=ready_proxy(&value).await;let running=test_listener::start_with_handoff(&proxy,proxy.engine().config().inbounds[0].clone(),move||drop(reserved)).await;
    let mut client=TcpStream::connect(running.auxiliary[0]).await.unwrap();client.write_all(b"GET /next?q=one HTTP/1.1\r\nHost: website.example\r\nConnection: close\r\n\r\n").await.unwrap();let (mut peer,_)=origin.accept().await.unwrap();let mut bytes=vec![0;2048];let size=peer.read(&mut bytes).await.unwrap();let head=String::from_utf8(bytes[..size].to_vec()).unwrap();assert!(head.contains("/base/next?fixed=yes&q=one"),"{head}");assert!(head.contains("host: website.example"),"{head}");assert!(head.contains("x-forwarded-for: 127.0.0.1"),"{head}");
    // The awaited origin response is provably pending, not inferred from a delay.
    let mut response=[0;1];assert!(matches!(std::future::poll_fn(|cx|std::task::Poll::Ready(std::pin::Pin::new(&mut client).poll_read(cx,&mut tokio::io::ReadBuf::new(&mut response)))).await,std::task::Poll::Pending));
    assert_eq!(proxy.engine().stats_snapshot().total_started,0,"appearance requests are not proxy flows");
    test_listener::close(&proxy,"hy",running).await;
    proxy.protocols.shutdown_components(tokio::time::Instant::now()+Duration::from_secs(3)).await.unwrap();
    assert_eq!(peer.read(&mut response).await.unwrap(),0,"original scoped origin closed");
}).await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn standard_website_unix_proxy_uses_declared_original() {
    timeout(Duration::from_secs(15),async{
    let (dir,mut value)=material();let endpoint=dir.path().join("origin.sock");let origin=tokio::net::UnixListener::bind(&endpoint).unwrap();
    value["inbounds"][0]["protocol"]["masquerade"]=serde_json::json!({"type":"proxy","force_https":false,"url":format!("unix:{}",endpoint.display()),"http":{"address":"127.0.0.1","port":0}});
    let reserved=reserve_sites(&mut value,false);let proxy=ready_proxy(&value).await;let running=test_listener::start_with_handoff(&proxy,proxy.engine().config().inbounds[0].clone(),move||drop(reserved)).await;
    let response=tokio::spawn(request(running.auxiliary[0],"/owned"));let (mut peer,_)=origin.accept().await.unwrap();let mut bytes=[0;1024];let size=peer.read(&mut bytes).await.unwrap();assert!(String::from_utf8_lossy(&bytes[..size]).starts_with("GET /owned "));peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nunix").await.unwrap();assert!(response.await.unwrap().ends_with("unix"));
    assert_eq!(proxy.engine().stats_snapshot().total_started,0,"appearance requests are not proxy flows");
    test_listener::close(&proxy,"hy",running).await;
    proxy.protocols.shutdown_components(tokio::time::Instant::now()+Duration::from_secs(3)).await.unwrap();
}).await.unwrap();
}
