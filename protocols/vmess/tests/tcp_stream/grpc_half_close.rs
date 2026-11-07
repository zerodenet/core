//! The real H2 peer waits for request END before returning its response tail.
//! Raw/TLS still uses the independent default covered by native_half_close.
use super::*;
use std::{num::NonZeroUsize, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        lifecycle::ChannelControl,
        scope::{TaskClass, TaskScope},
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    ControlledTcpListenerOwner, EgressInterfaceControl, PendingTcpConnect,
};
use zero_transport::profile::OwnedGrpcProfile;
const UUID: &str = "11111111-2222-3333-4444-555555555555";
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
fn payload(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}
#[tokio::test]
async fn actual_grpc_request_end_precedes_full_reverse_vmess_eof_on_single_and_multi() {
    tokio::time::timeout(Duration::from_secs(8), async {
        for multi_mode in [false, true] {
            for cipher in [
                crate::VmessCipher::Aes128Gcm,
                crate::VmessCipher::Chacha20Poly1305,
            ] {
                let end = tokio::time::Instant::now() + Duration::from_secs(8);
                let one = NonZeroUsize::new(1).unwrap();
                let two = NonZeroUsize::new(2).unwrap();
                let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(two));
                let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
                    &ResourceDeclaration::TcpListener {
                        destination: zero_core::request::Destination::new(
                            zero_core::Address::Ipv4([127, 0, 0, 1]),
                            0,
                        ),
                        accepted: None,
                        max_bind_attempts: one,
                    },
                    one,
                    CancellationToken::new(),
                    one,
                    two,
                    &scope.registrar(),
                )
                .unwrap();
                let mut listener = original
                    .bind_listener("127.0.0.1:0".parse().unwrap())
                    .unwrap();
                let address = listener.local_addr().unwrap();
                let receiver = original.accepted_task_receiver().unwrap();
                scope.enable_registration().unwrap();
                let selection = EgressInterfaceControl::default()
                    .select_for_peer_with_policy(address, &Default::default())
                    .unwrap();
                let (mut pending, mut client_original) = PendingTcpConnect::prepare(
                    address,
                    &Default::default(),
                    &selection,
                    CancellationToken::new(),
                    None,
                )
                .unwrap();
                let (wire, accepted) = tokio::join!(pending.connect(end), listener.accept_owned());
                let wire = wire.unwrap();
                assert!(wire.nodelay().unwrap());
                let (accepted, _) = accepted.unwrap();
                let claim = accepted.claim();
                let profile = OwnedGrpcProfile {
                    service_names: vec!["vmess-end/Tun".into()],
                    multi_mode,
                    ..Default::default()
                };
                let server_profile = profile.clone();
                let (_, caller) = accepted
                    .spawn_handler(
                        &scope.registrar(),
                        TaskClass::Auxiliary,
                        move |socket, tasks| async move {
                            assert!(socket.nodelay().unwrap());
                            let incoming = zero_transport::grpc::accept_grpc_connection_scoped(
                                socket,
                                &server_profile,
                                &tasks.caller()?,
                            )
                            .map_err(failure)?;
                            let carrier = incoming.accept().await.expect("real H2 stream");
                            let users = crate::inbound::VmessInboundProfile::from_config_users([(
                                UUID.to_owned(),
                                cipher.name().to_owned(),
                                None::<String>,
                                None::<u64>,
                                None::<u64>,
                                None::<u32>,
                                None::<u64>,
                                None::<u64>,
                            )])
                            .unwrap();
                            let (_, mut io) = users
                                .accept_request_stream(crate::inbound::VmessInbound, carrier)
                                .await
                                .map_err(failure)?;
                            let mut upload = Vec::new();
                            io.read_to_end(&mut upload).await.map_err(failure)?;
                            assert_eq!(upload, payload(262144));
                            // Reading VMess EOF is insufficient. This is the original
                            // H2 receiver observing the actual request END_STREAM.
                            let mut tail = [0; 1];
                            assert_eq!(io.inner.read(&mut tail).await.map_err(failure)?, 0);
                            let mut response = upload;
                            response.extend(payload(1025));
                            io.write_all(&response).await.map_err(failure)?;
                            io.shutdown().await.map_err(failure)?;
                            drop(io);
                            drop(incoming);
                            Ok(())
                        },
                    )
                    .unwrap();
                let prepared = zero_transport::grpc::PreparedSingleClient::prepare(
                    wire,
                    &profile,
                    "localhost",
                    None,
                )
                .await
                .unwrap();
                let (mut carrier, driver) = prepared.into_parts();
                // This explicit library future has its real owning JoinHandle;
                // the server separately uses its actual accepted Scope above.
                let driver = tokio::spawn(driver);
                let (header, state) = crate::shared::prepare_request(
                    &zero_core::Address::Ipv4([127, 0, 0, 1]),
                    443,
                    &crate::parse_uuid(UUID).unwrap(),
                    cipher,
                    crate::shared::CMD_TCP,
                )
                .unwrap();
                carrier.write_all(&header).await.unwrap();
                carrier.flush().await.unwrap();
                let mut client = VmessAeadStream::outbound(carrier, state)
                    .unwrap()
                    .with_grpc_request_end();
                let transfer = async {
                    // There is deliberately no concurrent application read.
                    client.write_all(&payload(262144)).await.unwrap();
                    client.shutdown().await.unwrap();
                    assert!(client.native_shutdown.result.as_ref().unwrap().is_ok());
                    assert!(!matches!(client.read_state, ReadState::Eof));
                    let mut response = Vec::new();
                    client.read_to_end(&mut response).await.unwrap();
                    let mut expected = payload(262144);
                    expected.extend(payload(1025));
                    assert_eq!(response, expected);
                    drop(client);
                };
                let collect = async {
                    for _ in 0..2 {
                        let actual = scope.next_exit().await.unwrap();
                        assert!(actual.result.is_ok());
                        let accepted = receiver.observe_joined(actual.id).unwrap();
                        assert_eq!(accepted.id(), actual.id);
                        assert!(accepted.result().is_ok() && accepted.registration_released());
                    }
                };
                tokio::join!(transfer, collect);
                let joined = caller.try_joined().unwrap();
                assert!(caller.matches_joined(&joined) && joined.registration_released());
                driver.await.unwrap().unwrap();
                let native = client_original.wait_closed(end).await.unwrap();
                assert!(native.is_complete());
                assert!(original
                    .close_accepted_graph(&claim, end)
                    .await
                    .unwrap()
                    .unwrap()
                    .matches(&claim));
                drop(listener);
                assert!(original.wait_closed(end).await.unwrap().is_complete());
                assert!(scope.shutdown(end).await.is_complete());
            }
        }
    })
    .await
    .expect("original 8s gRPC/VMess directional END and complete reverse tail");
}
