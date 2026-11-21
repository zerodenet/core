//! Exercise real endpoint preparation on the Windows main-thread stack budget.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use serde_json::json;
use tokio::time::{Duration, Instant};
use zero_api::{CommandRequest, CommandService, EndpointOperationCommand, EndpointRuntimeState};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[test]
fn literal_peer_preparation_restart_and_original_shutdown_fit_one_mib() {
    std::thread::Builder::new()
        .name("endpoint-one-mib".into())
        .stack_size(1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                // Retain the real peer reservation; no guessed/free port fixture.
                let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let config = RuntimeConfig::parse(
                    &json!({
                        "endpoints":[{"tag":"small-stack",
                            "directions":{"inbound":true,"outbound":true},
                            "listen":{"address":"127.0.0.1","port":0},
                            "protocol":{"type":"wireguard",
                                "private_key":STANDARD.encode([61;32]),
                                "addresses":["10.0.0.1/32"],
                                "peers":[{
                                    "public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([62;32])).as_bytes()),
                                    "endpoint":peer.local_addr().unwrap().to_string(),
                                    "allowed_ips":["10.0.0.0/24"]}]}}],
                        "route":{"rules":[],"final":{"type":"direct"}}
                    })
                    .to_string(),
                )
                .unwrap();
                let proxy = Proxy::new(config).unwrap();
                let handle = super::management_support::handle(&proxy);
                let running = proxy.spawn();
                running
                    .wait_ready(Instant::now() + Duration::from_secs(5))
                    .await
                    .unwrap();
                assert_eq!(
                    super::management_support::endpoint(&handle, "small-stack").state,
                    EndpointRuntimeState::Running
                );
                let transitions = async {
                    for _ in 0..5 {
                        let previous =
                            super::management_support::endpoint(&handle, "small-stack");
                        for enabled in [false, true] {
                            handle
                                .execute_acknowledged(super::management_support::set_state(
                                    "small-stack",
                                    enabled,
                                ))
                                .await?;
                            assert_eq!(
                                super::management_support::endpoint(&handle, "small-stack").state,
                                if enabled {
                                    EndpointRuntimeState::Running
                                } else {
                                    EndpointRuntimeState::Stopped
                                }
                            );
                        }
                        handle
                            .execute_acknowledged(CommandRequest::EndpointRestart(
                                EndpointOperationCommand {
                                    endpoint_id: "endpoint:small-stack".into(),
                                    expected_intent_revision: None,
                                    expected_core_instance_id: None,
                                },
                            ))
                            .await?;
                        let current =
                            super::management_support::endpoint(&handle, "small-stack");
                        assert_eq!(current.state, EndpointRuntimeState::Running);
                        assert!(current.generation > previous.generation);
                    }
                    Ok::<(), zero_api::ApiError>(())
                }
                .await;
                // Consume the actual host's full original-provider/Scope result.
                let shutdown = running.shutdown().await;
                transitions.unwrap();
                shutdown.unwrap();
                assert_eq!(
                    super::management_support::endpoint(&handle, "small-stack").state,
                    EndpointRuntimeState::Stopped
                );
            });
        })
        .unwrap()
        .join()
        .unwrap();
}
