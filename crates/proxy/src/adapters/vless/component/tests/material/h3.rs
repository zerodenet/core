//! Ordinary installed XMUX/H3 uses one actual native/borrowed original.
use super::fixture::{self, Running};
use super::quic::original::Original;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::timeout,
};
const ID: &str = "11111111-2222-3333-4444-555555555555";
#[path = "h3/wire.rs"]
mod wire;
#[tokio::test]
async fn ordinary_xmux_h3_keeps_one_original_across_two_real_users_and_replaces_after_network_change(
) {
    timeout(Duration::from_secs(60),async {
        for borrowed in [false,true] {for mode in ["stream-one","stream-up","packet-up"] {
            let files=tempfile::tempdir().unwrap();
            let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let ca=files.path().join("ca.pem");let key=files.path().join("key.pem");
            std::fs::write(&ca,certificate.cert.pem()).unwrap();std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
            let remote=fixture::start(serde_json::json!({
                "inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":ca,"key_path":key},"split_http":{"path":"/original/","mode":"auto"}}}],
                "route":{"final":{"type":"direct"}}
            }),"remote").await;
            let prefix=if borrowed {Some(fixture::start(serde_json::json!({"inbounds":[{"tag":"prefix","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}),"prefix").await)}else{None};
            let mut nodes=vec![serde_json::json!({"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"quic":{"server_name":"localhost","ca_cert_path":ca},"split_http":{"path":"/original/","mode":mode,"xmux":{"max_connections":1,"max_concurrency":8}}}})];
            if let Some(prefix)=&prefix {nodes.insert(0,serde_json::json!({"tag":"prefix","protocol":{"type":"socks5","server":"127.0.0.1","port":prefix.address.port()}}));}
            let mut config=serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":nodes,"route":{"final":{"type":"route","outbound":if borrowed {"chain"}else{"node"}}}});
            if borrowed {config["outbound_groups"]=serde_json::json!([{"tag":"chain","type":"relay","proxies":["prefix","node"]}]);}
            let client=fixture::start(config,"entry").await;
            let cancelled=wire::Exchange::discarded(&client).await;
            let first=wire::Exchange::open(&client).await;
            let original=Original::capture(&client,borrowed).await;
            if let Original::Native(_,original)=&original {
                let receiver=original.retained_driver().expect("actual H3 unique native claim and driver ledger");
                assert_ne!(receiver.packet_driver().task_id(),receiver.protocol_driver().task_id());
                assert!(!receiver.protocol_driver().is_joined());
            }
            // Cancel the actual first business after its complete upload
            // reached the target, while this second logical stream is live.
            cancelled.abandon().await;
            assert!(Original::capture(&client,borrowed).await.same(&original));
            let second=wire::Exchange::open(&client).await;
            let same=Original::capture(&client,borrowed).await;assert!(same.same(&original));
            first.finish().await;
            let still=Original::capture(&client,borrowed).await;assert!(still.same(&original));
            second.finish().await;
            // Logical EOFs return their own requests, not this cached native.
            let third=wire::Exchange::open(&client).await;
            assert!(Original::capture(&client,borrowed).await.same(&original));third.finish().await;
            wire::udp(&client,&original,borrowed).await;
            std::fs::remove_file(&ca).unwrap();
            client.proxy.egress_interface.invalidate_network();
            let fourth=wire::Exchange::open(&client).await;
            let next=Original::capture(&client,borrowed).await;assert!(!next.same(&original));
            fourth.finish().await;original.returned().await;
            client.close().await;next.returned().await;original.closed_owner();next.closed_owner();
            if let Some(prefix)=prefix {prefix.close().await;}remote.close().await;
        }}
    }).await.unwrap();
}
