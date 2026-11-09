//! Ordinary installed QUIC roles, using real listen0 Sources on both sides.
use super::fixture::{self, Running};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::timeout,
};
const ID: &str = "11111111-2222-3333-4444-555555555555";
#[path = "quic/original.rs"]
mod original;
#[path = "quic/wire.rs"]
mod wire;
#[tokio::test]
async fn ordinary_quic_native_and_borrowed_share_actual_n1_and_return_each_original() {
    timeout(Duration::from_secs(45),async {
        for borrowed in [false,true] {
            let directory=tempfile::tempdir().unwrap();
            let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert=directory.path().join("ca.pem");let key=directory.path().join("key.pem");
            std::fs::write(&cert,certificate.cert.pem()).unwrap();
            std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
            let remote=fixture::start(serde_json::json!({
                "inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":cert,"key_path":key}}}],
                "route":{"final":{"type":"direct"}}
            }),"remote").await;
            let prefix=if borrowed {Some(fixture::start(serde_json::json!({
                "inbounds":[{"tag":"prefix","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                "route":{"final":{"type":"direct"}}
            }),"prefix").await)}else{None};
            let mut nodes=vec![serde_json::json!({"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"quic":{"server_name":"localhost","ca_cert_path":cert}}})];
            if let Some(prefix)=&prefix {
                nodes.insert(0,serde_json::json!({"tag":"prefix","protocol":{"type":"socks5","server":"127.0.0.1","port":prefix.address.port()}}));
            }
            let mut document=serde_json::json!({
                "runtime":{"native_stream_max_originals":1},
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                "outbounds":nodes,"route":{"final":{"type":"route","outbound":if borrowed {"chain"}else{"node"}}}
            });
            if borrowed {document["outbound_groups"]=serde_json::json!([{"tag":"chain","type":"relay","proxies":["prefix","node"]}]);}
            let client=fixture::start(document,"entry").await;
            let plan=client.proxy.tcp_runtime_services().components.clone().unwrap();
            let slot=plan.configured("node").unwrap();
            assert!(plan.has_metadata_stream_provider(slot.role_id()));
            assert!(plan.stream_dependencies_supported(slot.role_id()));
            // Ordinary native and nonempty-prefix borrowed routes must select
            // the exact same resourceful owning recipe, never a raw executor.
            assert!(slot.original_metadata_owner(0).is_some());
            assert!(slot.original_carrier_owner(1).is_some());
            let first=wire::tcp(&client,borrowed).await;
            first.returned().await;
            std::fs::remove_file(&cert).unwrap();
            client.proxy.egress_interface.invalidate_network();
            let second=wire::tcp(&client,borrowed).await;
            assert!(!first.same(&second));second.returned().await;
            // Same accepted immutable CA survives this generation change; no
            // library reopen or fresh raw endpoint may supply the next stream.
            wire::udp(&client,borrowed).await;
            client.close().await;
            first.closed_owner();second.closed_owner();
            if let Some(prefix)=prefix {prefix.close().await;}
            remote.close().await;
        }
    }).await.unwrap();
}
