#![cfg(feature = "shadowsocks")]
mod support;
use shadowsocks::{udp::ShadowsocksDatagramCodec, CipherKind};
use support::{free_port, spawn_engine, wait_for_listener};
use tokio::{
    net::UdpSocket,
    time::{timeout, Duration},
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;
use zero_traits::DatagramCodec;
async fn receive(s: &UdpSocket) -> (Vec<u8>, std::net::SocketAddr) {
    let mut b = vec![0; 65535];
    let (n, a) = timeout(Duration::from_secs(3), s.recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    (b[..n].to_vec(), a)
}
#[tokio::test]
async fn legacy_udp_clients_and_users_have_isolated_flows() {
    for separate_users in [false, true] {
        let port = free_port();
        let password_b = if separate_users {
            "password-b"
        } else {
            "password-a"
        };
        let cfg = RuntimeConfig::parse(&format!(
            r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"aes-128-gcm","users":[{{"password":"password-a","principal_key":"a"}},{{"password":"{password_b}","principal_key":"b"}}]}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#
        ));
        // Same credential case needs just one configured user.
        let cfg = if separate_users {
            cfg.unwrap()
        } else {
            RuntimeConfig::parse(&format!(r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"aes-128-gcm","password":"password-a"}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#)).unwrap()
        };
        let running = spawn_engine(Proxy::new(cfg).unwrap());
        wait_for_listener(port).await;
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target_port = target.local_addr().unwrap().port();
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let ca = ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, "password-a");
        let cb = ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, password_b);
        a.send_to(
            &ca.encode(&Address::Ipv4([127, 0, 0, 1]), target_port, b"from-a")
                .unwrap(),
            ("127.0.0.1", port),
        )
        .await
        .unwrap();
        let (pa, up_a) = receive(&target).await;
        b.send_to(
            &cb.encode(&Address::Ipv4([127, 0, 0, 1]), target_port, b"from-b")
                .unwrap(),
            ("127.0.0.1", port),
        )
        .await
        .unwrap();
        let (_, up_b) = receive(&target).await;
        assert_ne!(up_a, up_b);
        target.send_to(&pa, up_a).await.unwrap();
        let (reply, _) = receive(&a).await;
        assert_eq!(ca.decode(&reply).unwrap().2, b"from-a");
        target.send_to(b"from-b", up_b).await.unwrap();
        let (reply, _) = receive(&b).await;
        assert_eq!(cb.decode(&reply).unwrap().2, b"from-b");

        running.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn principal_cancellation_preserves_other_users_on_shared_listener() {
    let port = free_port();
    let key_a = "MDEyMzQ1Njc4OWFiY2RlZg==";
    let key_b = "YWJjZGVmMDEyMzQ1Njc4OQ==";
    let cfg=RuntimeConfig::parse(&format!(r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"2022-blake3-aes-128-gcm","identity_password":"QUFBQUFBQUFBQUFBQUFBQQ==","users":[{{"password":"{key_a}","principal_key":"a"}},{{"password":"{key_b}","principal_key":"b"}}]}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#)).unwrap();
    let running = spawn_engine(Proxy::new(cfg).unwrap());
    wait_for_listener(port).await;
    let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target_port = target.local_addr().unwrap().port();
    let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let ca = ShadowsocksDatagramCodec::new(
        CipherKind::Blake3Aes128Gcm,
        format!("QUFBQUFBQUFBQUFBQUFBQQ==:{key_a}"),
    );
    let cb = ShadowsocksDatagramCodec::new(
        CipherKind::Blake3Aes128Gcm,
        format!("QUFBQUFBQUFBQUFBQUFBQQ==:{key_b}"),
    );
    for (s, c, p) in [(&a, &ca, b"a"), (&b, &cb, b"b")] {
        s.send_to(
            &c.encode(&Address::Ipv4([127, 0, 0, 1]), target_port, p)
                .unwrap(),
            ("127.0.0.1", port),
        )
        .await
        .unwrap();
        let (data, up) = receive(&target).await;
        target.send_to(&data, up).await.unwrap();
        let (reply, _) = receive(s).await;
        assert_eq!(c.decode(&reply).unwrap().2, p);
    }
    assert_eq!(
        running
            .close_principal_flows("a", "principal_disabled")
            .len(),
        1
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    b.send_to(
        &cb.encode(&Address::Ipv4([127, 0, 0, 1]), target_port, b"b-after")
            .unwrap(),
        ("127.0.0.1", port),
    )
    .await
    .unwrap();
    let (data, up) = receive(&target).await;
    assert_eq!(data, b"b-after");
    target.send_to(&data, up).await.unwrap();
    let (reply, _) = receive(&b).await;
    assert_eq!(cb.decode(&reply).unwrap().2, b"b-after");
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ss2022_association_reuses_socket_across_targets_and_migrates_all_replies() {
    let port = free_port();
    let password = "MDEyMzQ1Njc4OWFiY2RlZg==";
    let cfg=RuntimeConfig::parse(&format!(r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"2022-blake3-aes-128-gcm","password":"{password}"}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#)).unwrap();
    let running = spawn_engine(Proxy::new(cfg).unwrap());
    wait_for_listener(port).await;
    let t1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let t2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let codec = ShadowsocksDatagramCodec::new(CipherKind::Blake3Aes128Gcm, password);
    a.send_to(
        &codec
            .encode(
                &Address::Ipv4([127, 0, 0, 1]),
                t1.local_addr().unwrap().port(),
                b"first",
            )
            .unwrap(),
        ("127.0.0.1", port),
    )
    .await
    .unwrap();
    let (_, up1) = receive(&t1).await;
    a.send_to(
        &codec
            .encode(
                &Address::Ipv4([127, 0, 0, 1]),
                t2.local_addr().unwrap().port(),
                b"second",
            )
            .unwrap(),
        ("127.0.0.1", port),
    )
    .await
    .unwrap();
    let (_, up2) = receive(&t2).await;
    assert_eq!(
        up1, up2,
        "one client association uses one socket for both IPv4 destinations"
    );
    b.send_to(
        &codec
            .encode(
                &Address::Ipv4([127, 0, 0, 1]),
                t2.local_addr().unwrap().port(),
                b"migrate",
            )
            .unwrap(),
        ("127.0.0.1", port),
    )
    .await
    .unwrap();
    let (_, up3) = receive(&t2).await;
    assert_eq!(up2, up3);
    t1.send_to(b"old-target-after-migration", up1)
        .await
        .unwrap();
    let (reply, _) = receive(&b).await;
    assert_eq!(
        codec.decode(&reply).unwrap().2,
        b"old-target-after-migration"
    );
    running.shutdown().await.unwrap();
}
