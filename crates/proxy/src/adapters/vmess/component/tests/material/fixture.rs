use crate::{Proxy, ProxyHandle};
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_platform_tokio::controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration};
const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) struct Running {
    pub(super) proxy: Proxy,
    pub(super) address: SocketAddr,
    running: crate::RunningProxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Running {
    pub(super) fn handle(&self) -> ProxyHandle {
        ProxyHandle::new(
            zero_engine::EngineHandle::new(self.proxy.engine().clone()),
            self.proxy.clone(),
        )
    }
    pub(super) async fn close(self) {
        self.running.shutdown().await.unwrap();
        let closed = self
            .original
            .terminal_for_test()
            .expect("actual original Source native/users/Scope complete");
        assert_eq!(closed.identity(), self.original.identity());
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert!(closed.native_bound_failures().is_empty());
    }
}
pub(super) async fn start(document: serde_json::Value, tag: &str) -> Running {
    let config = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let registry = proxy.system_tun_source_registry();
    let running = proxy.spawn();
    let original = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(original) = registry.owner(tag) {
                break original;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    original.wait_ready().await.unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let address = *original.original_listener_address.get().unwrap();
    Running {
        proxy,
        address,
        running,
        original,
    }
}
fn transport(node: &mut serde_json::Value, kind: &str) {
    match kind.strip_prefix("mux-").unwrap_or(kind) {
        "raw" => {}
        "ws" => node["protocol"]["ws"] = serde_json::json!({"path":"/original"}),
        "grpc-single" | "grpc-multi" => {
            node["protocol"]["grpc"] = serde_json::json!({"service_names":["original/Tun"],"multi_mode":kind=="grpc-multi"})
        }
        _ => unreachable!("actual fixture transport"),
    }
}
pub(super) async fn server(kind: &str, cert: &Path, key: &Path) -> Running {
    let mut input = serde_json::json!({"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vmess","users":[{"id":ID}],"tls":{"cert_path":cert,"key_path":key}}});
    transport(&mut input, kind);
    start(
        serde_json::json!({"inbounds":[input],"route":{"final":{"type":"direct"}}}),
        "remote",
    )
    .await
}
pub(super) async fn client(kind: &str, backend: &str, endpoint: SocketAddr, ca: &Path) -> Running {
    let mut node = serde_json::json!({"tag":"node","protocol":{"type":"vmess","server":"127.0.0.1","port":endpoint.port(),"id":ID,"tls":{"server_name":"localhost","ca_cert_path":ca,"client_fingerprint":null,"options":{"backend":backend}}}});
    transport(&mut node, kind);
    if kind.starts_with("mux-") {
        node["protocol"]["mux_concurrency"] = serde_json::json!(8);
    }
    start(serde_json::json!({"runtime":{"native_stream_max_originals":1,"native_mux_max_carriers":1,"vmess_grpc_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[node],"route":{"final":{"type":"route","outbound":"node"}}}),"entry").await
}
pub(super) fn capture(proxy: &Proxy) -> (CarrierOwnerRegistration, CarrierOriginObservation) {
    let named = proxy
        .tcp_runtime_services()
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap();
    let cached = named.recipe.spec().resources.is_empty();
    let slot = if cached {
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        named.dependency_instances().iter().filter_map(|instance| plan.component(instance.component))
            .find(|slot| slot.recipe.spec().resources.iter().any(|resource| matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { .. })))
            .expect("same original configured MUX pool dependency")
    } else {
        named
    };
    let grpc =
        slot.recipe.spec().resources.iter().any(|resource| {
            matches!(resource,
        zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeStream { .. })
        });
    for ordinal in 0..slot.recipe.spec().resources.len() {
        let Some(owner) = slot.original_carrier_owner(ordinal) else {
            continue;
        };
        let Some(observed) = owner.observe() else {
            continue;
        };
        if let Some(original) = observed
            .slots()
            .iter()
            .find(|slot| slot.native().is_some_and(|native| !native.is_closed()))
        {
            if grpc || cached {
                let (task, driver) = original
                    .driver()
                    .expect("same original registered transport driver");
                assert!(!driver.is_joined());
                assert_eq!(owner.instance(), slot.instance_id());
                let bound = owner
                    .bound()
                    .expect("same actual provider registration is bound");
                assert!(driver.scope_identity().same_scope(&bound.scope_identity()));
                assert_eq!(driver.task_id(), Some(*task));
            } else {
                assert!(original.driver().is_none());
            }
            {
                let auxiliary = original
                    .auxiliary()
                    .expect("same per-connection setup task");
                let joined = auxiliary
                    .try_joined()
                    .expect("actual setup joined before delivery");
                assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
                let bound = owner.bound().expect("actual native owner bound");
                assert!(auxiliary
                    .scope_identity()
                    .same_scope(&bound.scope_identity()));
            }
            assert_eq!(observed.retained(), 1);
            return (owner, original.original().clone());
        }
    }
    panic!("actual ordinary VMess wire must own its same native grant")
}
pub(super) async fn echo(client: &Running) -> (CarrierOwnerRegistration, CarrierOriginObservation) {
    echo_with(client, || capture(&client.proxy)).await
}
pub(super) async fn echo_with<T>(client: &Running, inspect: impl FnOnce() -> T) -> T {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let payload: Vec<u8> = (0..262144).map(|index| (index % 251) as u8).collect();
    let expected = payload.clone();
    let peer = tokio::spawn(async move {
        let (mut io, _) = target.accept().await.unwrap();
        let mut body = Vec::new();
        io.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, expected);
        io.write_all(&body).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let mut io = TcpStream::connect(client.address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&port.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    // No application read while outgoing SETTINGS/WINDOW_UPDATE and writes
    // progress; then preserve a real write half-close and full response body.
    io.write_all(&payload).await.unwrap();
    io.flush().await.unwrap();
    let original = inspect();
    io.shutdown().await.unwrap();
    let mut received = Vec::new();
    io.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, payload);
    drop(io);
    peer.await.unwrap();
    original
}
pub(super) async fn wait_returned(
    owner: &CarrierOwnerRegistration,
    original: &CarrierOriginObservation,
) {
    timeout(Duration::from_secs(3), async {
        loop {
            if original.closed().is_some()
                && owner
                    .observe()
                    .is_some_and(|observed| observed.retained() == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let closed = original.closed().unwrap();
    assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
}

pub(super) fn cached_driver(
    owner: &CarrierOwnerRegistration,
    original: &CarrierOriginObservation,
) -> zero_platform_tokio::contract::scope::TaskJoinWitness {
    owner
        .observe()
        .unwrap()
        .slots()
        .iter()
        .find(|slot| slot.original().same_original(original))
        .and_then(|slot| slot.driver().map(|(_, witness)| witness.clone()))
        .expect("same actual cached MUX driver witness")
}
pub(super) async fn udp_echo(client: &Running) {
    udp_echo_with(client, || ()).await
}
pub(super) async fn udp_echo_with<T>(client: &Running, inspect: impl FnOnce() -> T) -> T {
    let mut control = TcpStream::connect(client.address).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut bound = [0; 10];
    control.read_exact(&mut bound).await.unwrap();
    assert_eq!(&bound[..4], &[5, 0, 0, 1]);
    let relay = SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([bound[8], bound[9]])));
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let codec = socks5::udp::Socks5InboundUdpCodec;
    for payload in [&b"same actual VMess MUX"[..], &b""[..]] {
        let wire = codec
            .encode_response_to_client(&zero_core::Address::Ipv4([127, 0, 0, 1]), port, payload)
            .unwrap();
        socket.send_to(&wire, relay).await.unwrap();
        let mut bytes = [0; 65536];
        let (n, peer) = target.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..n], payload);
        target.send_to(&bytes[..n], peer).await.unwrap();
        let (n, _) = socket.recv_from(&mut bytes).await.unwrap();
        let (address, source, body) = codec.decode_response(&bytes[..n]).unwrap().into_parts();
        assert_eq!(address, zero_core::Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(source, port);
        assert_eq!(body.as_slice(), payload);
    }
    let original = inspect();
    drop(control);
    original
}
