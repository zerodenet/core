use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn dns_modes_return_real_wire_answers_through_the_tun_packet_pipeline() {
    for mode in ["disabled", "real", "fake_ip"] {
        let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = upstream.local_addr().unwrap();
        let responder = if mode != "fake_ip" {
            Some(tokio::spawn(async move {
                let mut request = [0; 512];
                let (length, peer) = upstream.recv_from(&mut request).await.unwrap();
                let mut response = request[..length].to_vec();
                response[2] = 0x81;
                response[3] = 0x80;
                response[7] = 1;
                response.extend_from_slice(&[
                    0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 30, 0, 4, 203, 0, 113, 7,
                ]);
                upstream.send_to(&response, peer).await.unwrap();
            }))
        } else {
            None
        };
        let mut value = serde_json::json!({"route":{"rules":[],"final":{"type":"direct"}}});
        if mode != "disabled" {
            value["runtime"] = serde_json::json!({"dns":{
                "servers":{"local":{"type":"udp","host":"127.0.0.1","port":endpoint.port()}},
                "default_server":"local", "answer":{"type":mode}
            }});
            if mode == "fake_ip" {
                value["runtime"]["dns"]["answer"]["cidr"] = "198.18.0.0/15".into();
            }
        }
        let proxy = crate::Proxy::new(RuntimeConfig::parse(&value.to_string()).unwrap()).unwrap();
        let (outbound, mut packets) = mpsc::channel(8);
        let stack = UserNetworkStack::new(outbound, 1440);
        let (_, udp) = stack.into_parts();
        let counter = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(run(
            proxy,
            Arc::clone(&udp),
            "dns-matrix".into(),
            mode != "disabled",
            Arc::clone(&counter),
        ));
        let (ip, port) = if mode == "disabled" {
            (endpoint.ip(), endpoint.port())
        } else {
            ("203.0.113.53".parse().unwrap(), 53)
        };
        let request = packet::build_udp(
            "10.0.0.2".parse().unwrap(),
            ip,
            55000,
            port,
            &dns_query("matrix.example"),
        );
        udp.feed(&request).await;
        let response = tokio::time::timeout(std::time::Duration::from_secs(3), packets.recv())
            .await
            .unwrap()
            .unwrap();
        let response = packet::parse_udp(&response).unwrap();
        assert_eq!(response.src.ip, ip);
        assert_eq!(response.src.port, port);
        assert_eq!(response.dst.port, 55000);
        assert_eq!(&response.payload[..2], &[0x12, 0x34]);
        assert_eq!(
            &response.payload[response.payload.len() - 4..],
            if mode == "fake_ip" {
                &[198, 18, 0, 1]
            } else {
                &[203, 0, 113, 7]
            },
            "{mode}"
        );
        assert_eq!(
            counter.load(Ordering::Relaxed),
            u64::from(mode != "disabled")
        );
        task.abort();
        let _ = task.await;
        if let Some(responder) = responder {
            responder.await.unwrap();
        }
    }
}
