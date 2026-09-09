use super::impairment::{Counters, Link};
use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Instant,
};

const RATE: u64 = 262_144;
const PAYLOAD: usize = 2 * 1024 * 1024;

#[derive(Debug)]
struct Measurement {
    seconds: f64,
    wire: Counters,
    official_brutal: bool,
}
impl Measurement {
    fn wire_rate(&self) -> f64 {
        self.wire.bytes as f64 / self.seconds
    }
    fn goodput(&self) -> f64 {
        PAYLOAD as f64 / self.seconds
    }
}

async fn upload(zero_sender: bool, loss: u64, disabled: bool) -> Measurement {
    let material = TempMaterial::new("hy2-bandwidth-loss");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let server_port = free_udp_port();
    let socks_port = free_port();
    let link = Link::start(server_port, loss).await;
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_port = target.local_addr().unwrap().port();
    let sink = tokio::spawn(async move {
        let (mut stream, _) = target.accept().await.unwrap();
        let mut bytes = vec![0; PAYLOAD];
        stream.read_exact(&mut bytes).await.unwrap();
        assert!(bytes.iter().all(|b| *b == 0x5a));
        stream.write_all(b"done").await.unwrap();
    });
    let client_protocol = serde_json::json!({
        "type":"hysteria2", "server":"127.0.0.1", "port":link.address.port(),
        "server_name":"localhost", "insecure":true, "password":"test",
        "up_bps":RATE, "down_bps":RATE,
        "transport":{"congestion":{"disable_loss_compensation":disabled}, "quic":{"disable_path_mtu_discovery":true}}
    });
    let server_protocol = serde_json::json!({
        "type":"hysteria2", "password":"test", "cert_path":cert_path, "key_path":key_path,
        "up_bps":RATE, "down_bps":RATE,
        "transport":{"quic":{"disable_path_mtu_discovery":true}}
    });
    let zero = if zero_sender {
        serde_json::json!({
            "inbounds":[{"tag":"socks", "listen":{"address":"127.0.0.1","port":socks_port}, "protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"hy", "protocol":client_protocol}],
            "route":{"rules":[],"final":{"type":"route","outbound":"hy"}}
        })
    } else {
        serde_json::json!({
            "inbounds":[{"tag":"hy", "listen":{"address":"127.0.0.1","port":server_port}, "protocol":server_protocol}],
            "route":{"rules":[],"final":{"type":"direct"}}
        })
    };
    let mut official_config = if zero_sender {
        serde_json::json!({"listen":format!("127.0.0.1:{server_port}"),
            "tls":{"cert":cert_path,"key":key_path}, "auth":{"type":"password","password":"test"}})
    } else {
        serde_json::json!({"server":link.address.to_string(), "auth":"test",
            "tls":{"insecure":true,"sni":"localhost"}, "socks5":{"listen":format!("127.0.0.1:{socks_port}")}})
    };
    official_config["bandwidth"] = serde_json::json!({"up":"2097152 bps", "down":"2097152 bps", "disableLossCompensation":disabled});
    official_config["quic"] = serde_json::json!({"disablePathMTUDiscovery":true});
    let config_path = material.path("official.json");
    std::fs::write(&config_path, official_config.to_string()).unwrap();
    let proxy = spawn_engine(Proxy::new(RuntimeConfig::parse(&zero.to_string()).unwrap()).unwrap());
    let mut official = ExternalProcess::start_with_env(
        std::env::var("HY2_BIN").expect("HY2_BIN must point to app/v2.12.2"),
        &[
            if zero_sender { "server" } else { "client" },
            "--config",
            config_path.to_str().unwrap(),
            "--disable-update-check",
        ],
        &[("HYSTERIA_BRUTAL_DEBUG", "true")],
        &material,
        "official",
    );
    wait_for_listener(socks_port).await;
    if zero_sender {
        sleep(Duration::from_millis(300)).await;
    }
    let measurement = timeout(Duration::from_secs(45), async {
        let mut stream = TcpStream::connect(("127.0.0.1", socks_port)).await.unwrap();
        stream.write_all(&[5, 1, 0]).await.unwrap();
        let mut auth = [0; 2];
        stream.read_exact(&mut auth).await.unwrap();
        assert_eq!(auth, [5, 0]);
        let [hi, lo] = target_port.to_be_bytes();
        stream
            .write_all(&[5, 1, 0, 1, 127, 0, 0, 1, hi, lo])
            .await
            .unwrap();
        // Both implementations reply with an IPv4 bind address.
        let mut reply = [0; 10];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply[..4], &[5, 0, 0, 1]);
        link.reset();
        let start = Instant::now();
        stream.write_all(&vec![0x5a; PAYLOAD]).await.unwrap();
        let mut reply = [0; 4];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"done");
        Measurement {
            seconds: start.elapsed().as_secs_f64(),
            wire: link.counters(),
            official_brutal: official.logs().contains("BrutalSender"),
        }
    })
    .await
    .unwrap_or_else(|e| panic!("bandwidth comparison: {e}; {}", official.logs()));
    sink.await.unwrap();
    timeout(Duration::from_secs(5), proxy.shutdown())
        .await
        .unwrap()
        .unwrap();
    official.kill();
    measurement
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn official_and_zero_bandwidth_under_matched_loss_and_compensation_settings() {
    let mut results = Vec::new();
    for zero_sender in [false, true] {
        let mut sender_results = Vec::new();
        for (loss, disabled) in [(0, false), (4, false), (4, true)] {
            let measured = upload(zero_sender, loss, disabled).await;
            eprintln!("HY2 bandwidth sender={} loss_every={loss} compensation={} goodput_Bps={:.0} wire_Bps={:.0} {:?}",
                if zero_sender {"zero"} else {"official"}, !disabled, measured.goodput(), measured.wire_rate(), measured);
            assert!(
                measured.seconds > 3.0,
                "fixed-rate negotiation was not applied"
            );
            assert_eq!(measured.wire.dropped == 0, loss == 0);
            if !zero_sender && !disabled {
                assert!(
                    measured.official_brutal,
                    "official sender did not report Brutal state"
                );
            }
            sender_results.push(measured);
        }
        assert!(
            sender_results[1].wire_rate() > sender_results[2].wire_rate() * 1.05,
            "loss compensation did not increase packet send rate: {sender_results:?}"
        );
        results.push(sender_results);
    }
    for (official, zero) in results[0].iter().zip(&results[1]) {
        let ratio = zero.goodput() / official.goodput();
        assert!((0.7..=1.3).contains(&ratio), "matched-link throughput differs by more than 30%: official={official:?}, zero={zero:?}");
    }
}
