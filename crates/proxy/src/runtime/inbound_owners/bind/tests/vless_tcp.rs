//! Existing TCP carriers, actual production dispatch and serial original retirement.
use super::*;
use zero_platform_tokio::TcpRelayStream;
mod fixture;
mod packet;
mod prelude;
mod reverse;
const USER: &str = "11111111-2222-3333-4444-555555555555";
fn request(command: u8, port: u16) -> Vec<u8> {
    let mut b = vec![0];
    b.extend(vless::parse_uuid(USER).unwrap());
    b.extend([0, command]);
    b.extend(port.to_be_bytes());
    b.extend([1, 127, 0, 0, 1]);
    b
}
#[tokio::test]
async fn vless_tcp_xhttp_existing_modes_dispatch_and_close_originals_before_restart() {
    timeout(Duration::from_secs(40), async {
        for carrier in ["packet-up", "stream-up", "stream-one-h2", "stream-one-h1"] {
            exercise(carrier).await;
        }
    })
    .await
    .unwrap();
}
async fn open(address: std::net::SocketAddr, carrier: &str) -> TcpRelayStream {
    let socket = TcpStream::connect(address).await.unwrap();
    let mode = if carrier.starts_with("stream-one") {
        "stream-one"
    } else {
        carrier
    };
    let profile: zero_config::SplitHttpConfig =
        serde_json::from_value(serde_json::json!({"path":"/scope/","mode":mode})).unwrap();
    match carrier {
        "stream-one-h2" => TcpRelayStream::new(
            zero_transport::split_http::connect_xhttp_stream_one(socket, &profile)
                .await
                .unwrap(),
        ),
        "stream-one-h1" => TcpRelayStream::new(
            zero_transport::split_http::connect_xhttp_stream_one_http1(socket, &profile)
                .await
                .unwrap(),
        ),
        _ => TcpRelayStream::new(
            zero_transport::split_http::connect_split_http(
                socket,
                TcpStream::connect(address).await.unwrap(),
                &profile,
            )
            .await
            .unwrap(),
        ),
    }
}
async fn exercise(carrier: &str) {
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy=fixture::proxy(serde_json::json!({"type":"vless","users":[{"id":USER}],"split_http":{"path":"/scope/","mode":"auto"}})).await;
    for generation in 0..2 {
        let (owner, stop, address) = fixture::install(&proxy);
        let business = async {
            owner.wait_ready().await.unwrap();
            let mut stream = open(address, carrier).await;
            let mut header = request(1, echo.local_addr().unwrap().port());
            header.extend(b"real");
            stream.write_all(&header).await.unwrap();
            stream.flush().await.unwrap();
            let mut target = echo.accept().await.unwrap().0;
            let mut b = [0; 4];
            target.read_exact(&mut b).await.unwrap();
            assert_eq!(&b, b"real");
            target.write_all(b"back").await.unwrap();
            let mut reply = [0; 6];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"\0\0back");
            let mut datagram = open(address, carrier).await;
            let mut header = request(2, udp.local_addr().unwrap().port());
            header.extend(b"\0\x03udp");
            datagram.write_all(&header).await.unwrap();
            datagram.flush().await.unwrap();
            let mut b = [0; 64];
            let (n, sender) = udp.recv_from(&mut b).await.unwrap();
            assert_eq!(&b[..n], b"udp");
            udp.send_to(b"answer", sender).await.unwrap();
            let mut reply = [0; 10];
            datagram.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"\0\0\0\x06answer");
            let mut byte = [0];
            let mut waiting = Box::pin(target.read(&mut byte));
            std::future::poll_fn(|cx| {
                assert!(waiting.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            stop.send_replace(true);
            assert_eq!(waiting.await.unwrap(), 0, "original actual egress closes");
            drop(stream);
            drop(datagram);
        };
        let (observed, ()) = tokio::join!(owner.observe_driver(), business);
        observed.unwrap_or_else(|e| panic!("{carrier}/{generation}: {e:?}"));
        fixture::retire(&proxy, 5, 3).await;
        assert!(proxy.engine().active_sessions().is_empty());
        assert_eq!(
            proxy.engine().completed_sessions().len(),
            (generation + 1) * 2
        );
        let stats = proxy.engine().stats_snapshot();
        assert!(stats.bytes_up >= 7 && stats.bytes_down >= 10);
    }
}
