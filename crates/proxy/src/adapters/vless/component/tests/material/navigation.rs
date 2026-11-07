//! Actual ordinary REALITY authentication keeps its existing TCP/XUDP wire.
use super::fixture::*;
use std::time::Duration;
use tokio::{net::UdpSocket, time::timeout};
use zero_core::Address;
const ID: &str = "11111111-2222-3333-4444-555555555555";

#[tokio::test]
async fn ordinary_reality_raw_vision_and_mux_keep_actual_originals_and_existing_datagrams() {
    timeout(Duration::from_secs(30), async {
        for mode in ["raw", "vision", "mux"] {
            let (private, public) = ::vless::reality::generate_reality_key_pair();
            let mut user = serde_json::json!({"id":ID});
            if mode == "vision" {
                user["flow"] = "xtls-rprx-vision".into();
            }
            let remote = start(
                serde_json::json!({
                    "inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},
                        "protocol":{"type":"vless","users":[user],"reality":{
                            "private_key":private,"short_ids":["00"],
                            "server_names":["decoy.test"],"server_name":"decoy.test"
                        }}}],"route":{"final":{"type":"direct"}}
                }),
                "remote",
            )
            .await;
            let mut protocol = serde_json::json!({
                "type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,
                "reality":{"public_key":public,"short_id":"00","server_name":"decoy.test",
                    "hybrid_key_exchange":false,"client_fingerprint":"chrome","spider_x":"/"}
            });
            if mode == "vision" {
                protocol["flow"] = "xtls-rprx-vision".into();
            }
            if mode == "mux" {
                protocol["mux_concurrency"] = 8.into();
                protocol["xudp_concurrency"] = 8.into();
            }
            let client = start(
                serde_json::json!({
                    "runtime":{"native_stream_max_originals":1,"native_mux_max_carriers":1,
                        "navigation_max_jobs":1,"navigation_max_requests":1},
                    "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},
                        "protocol":{"type":"socks5"}}],
                    "outbounds":[{"tag":"node","protocol":protocol}],
                    "route":{"final":{"type":"route","outbound":"node"}}
                }),
                "entry",
            )
            .await;
            let (owner, original) = echo(&client).await;
            let driver = if mode == "mux" {
                let driver = cached_driver(&owner, &original);
                let (same, reused) = echo(&client).await;
                assert!(owner.same_owner(&same) && original.same_original(&reused));
                // Same raw REALITY/MUX original also carries the existing XUDP
                // association; an authenticated open never starts a decoy job.
                let (control, socket, relay) = super::udp::associate(client.address).await;
                let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let target = peer.local_addr().unwrap();
                let codec = socks5::udp::Socks5InboundUdpCodec;
                for payload in [&b"before"[..], &b""[..], &b"after"[..]] {
                    let wire = codec
                        .encode_response_to_client(
                            &Address::Ipv4([127, 0, 0, 1]),
                            target.port(),
                            payload,
                        )
                        .unwrap();
                    socket.send_to(&wire, relay).await.unwrap();
                    let mut bytes = [0; 65536];
                    let (count, sender) =
                        timeout(Duration::from_secs(4), peer.recv_from(&mut bytes))
                            .await
                            .unwrap()
                            .unwrap();
                    assert_eq!(&bytes[..count], payload);
                    peer.send_to(payload, sender).await.unwrap();
                    let (count, _) = timeout(Duration::from_secs(4), socket.recv_from(&mut bytes))
                        .await
                        .unwrap()
                        .unwrap();
                    let (address, port, received) =
                        codec.decode_response(&bytes[..count]).unwrap().into_parts();
                    assert_eq!(address, Address::Ipv4([127, 0, 0, 1]));
                    assert_eq!(port, target.port());
                    assert_eq!(received.as_slice(), payload);
                    let (same, reused) = capture(&client.proxy);
                    assert!(owner.same_owner(&same) && original.same_original(&reused));
                }
                drop(socket);
                drop(control);
                Some(driver)
            } else {
                wait_returned(&owner, &original).await;
                None
            };
            client.close().await;
            if let Some(driver) = driver {
                let joined = driver.try_joined().unwrap();
                assert!(driver.matches_joined(&joined) && joined.registration_released());
            }
            let closed = original.closed().unwrap();
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            remote.close().await;
        }
    })
    .await
    .unwrap();
}
