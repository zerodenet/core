//! Ordinary installed mixed/independent-download clients, with real wire IO.
#[path = "mixed/originals.rs"]
mod originals;
#[path = "mixed/peer.rs"]
mod peer;
use super::fixture::{self, Running};
use super::h3::wire::Exchange;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream, UdpSocket},
    time::timeout,
};
const ID: &str = "11111111-2222-3333-4444-555555555555";
// New fixture premise, not a raised threshold on an existing failure:
// two concurrent users * two independent H1 halves * (own Slot + SOCKS TCP).
// H2/H3 sharing uses fewer than this same actual worst-case eight originals.
const ORIGINALS: usize = 8;
#[tokio::test]
async fn ordinary_independent_download_preserves_actual_h1_h2_h3_half_owners_and_two_live_users() {
    timeout(Duration::from_secs(120),async {
        for borrowed in [false,true] {for (up,down) in [(1,1),(1,2),(2,1),(1,3),(3,1),(2,3),(3,2),(3,3)] {
            let directory=tempfile::tempdir().unwrap();
            let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let ca=directory.path().join("client.pem");std::fs::write(&ca,certificate.cert.pem()).unwrap();
            let peer=peer::Peer::new(&certificate).await;
            let prefix=if borrowed {Some(fixture::start(serde_json::json!({"inbounds":[{"tag":"prefix","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}),"prefix").await)}else{None};
            let mut upload=half(up,peer.address(up),&ca);
            upload["type"]=serde_json::json!("vless");upload["id"]=serde_json::json!(ID);
            upload["split_http"]["download_settings"]=half(down,peer.address(down),&ca);
            let mut nodes=vec![serde_json::json!({"tag":"node","protocol":upload})];
            if let Some(prefix)=&prefix {nodes.insert(0,serde_json::json!({"tag":"prefix","protocol":{"type":"socks5","server":"127.0.0.1","port":prefix.address.port()}}));}
            let mut config=serde_json::json!({"runtime":{"native_stream_max_originals":ORIGINALS},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":nodes,"route":{"final":{"type":"route","outbound":if borrowed {"chain"}else{"node"}}}});
            if borrowed {config["outbound_groups"]=serde_json::json!([{"tag":"chain","type":"relay","proxies":["prefix","node"]}]);}
            let client=fixture::start(config,"entry").await;
            let first=Exchange::discarded(&client).await;
            let second=Exchange::open(&client).await;
            let originals=originals::Originals::capture(&client,borrowed).await;
            // The identical 3/3 pool deliberately reuses its one actual
            // multiplexed opening; independent versions own two real halves.
            assert_eq!(originals.actual_halves, if up==3 && down==3 {1}else{2});
            first.abandon().await;
            // Its original first caller is gone; the second real logical's
            // half owners and actual drivers must continue on the same book.
            second.finish().await;
            let third=Exchange::open(&client).await;
            let latest=originals::Originals::capture(&client,borrowed).await;
            third.finish().await;
            originals::udp(&client).await;
            std::fs::remove_file(&ca).unwrap();
            client.proxy.egress_interface.invalidate_network();
            let fourth=Exchange::open(&client).await;
            let fresh=originals::Originals::capture(&client,borrowed).await;
            fourth.finish().await;
            assert!(!latest.same(&fresh),"actual network generation requires fresh half originals");
            let root=client.proxy.protocols.runtime_host().original_budgets().carrier.clone();
            assert!(root.snapshot().charged<=ORIGINALS);
            client.close().await;
            originals.closed().await;latest.closed().await;fresh.closed().await;
            assert_eq!(root.snapshot().charged,0);
            assert_eq!(root.buffer_budget().used_bytes(),0);
            if let Some(prefix)=prefix {prefix.close().await;}peer.close().await;
        }}
    }).await.unwrap();
}
fn half(version: usize, address: SocketAddr, ca: &std::path::Path) -> serde_json::Value {
    let mut half = serde_json::json!({"server":"127.0.0.1","port":address.port(),"split_http":{"path":"/original/","mode":"packet-up","xmux":{"max_connections":1,"max_concurrency":8}}});
    if version == 2 {
        half["tls"] = serde_json::json!({"server_name":"localhost","ca_cert_path":ca});
    }
    if version == 3 {
        half["quic"] = serde_json::json!({"server_name":"localhost","ca_cert_path":ca});
    }
    half
}
