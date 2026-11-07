//! Ordinary gRPC and MUX retain their two actual scopes and one native socket.
use super::fixture::{start, Running};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use zero_platform_tokio::{
    contract::{component::ResourceOwner, scope::TaskJoinWitness},
    controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration},
};
const ID: &str = "11111111-2222-3333-4444-555555555555";
struct Originals {
    mux: CarrierOwnerRegistration,
    grpc: CarrierOwnerRegistration,
    upper: CarrierOriginObservation,
    native: CarrierOriginObservation,
    upper_driver: TaskJoinWitness,
    native_driver: TaskJoinWitness,
}
fn capture(client: &Running) -> Originals {
    let plan = client
        .proxy
        .tcp_runtime_services()
        .components
        .clone()
        .unwrap();
    let node = plan.configured("node").unwrap();
    assert!(node.recipe.spec().resources.is_empty());
    assert!(node.recipe.spec().task_limit.is_none());
    let mux_slot = plan.component(node.recipe.spec().dependencies[0]).unwrap();
    assert!(
        matches!(&mux_slot.recipe.spec().owner, ResourceOwner::Endpoint(name) if name.as_ref() == "shared.stream_pool")
    );
    let lower = mux_slot.dependency_instances().iter().filter_map(|id| plan.component(id.component))
        .find(|slot| matches!(&slot.recipe.spec().owner, ResourceOwner::Endpoint(name) if name.as_ref() == "grpc.client_pool"))
        .expect("same actual cached gRPC owner, not another inline H2 client");
    let owner = |slot: &Arc<crate::component::instance::ComponentSlot>| {
        (0..slot.recipe.spec().resources.len())
            .find_map(|ordinal| slot.original_carrier_owner(ordinal))
            .unwrap()
    };
    let mux = owner(&mux_slot);
    let grpc = owner(&lower);
    assert_eq!(mux.limit().get(), 1);
    assert_eq!(grpc.limit().get(), 1);
    let a = mux.observe().unwrap();
    let b = grpc.observe().unwrap();
    assert_eq!(a.retained(), 1);
    assert_eq!(b.retained(), 1);
    let upper = &a.slots()[0];
    let native = &b.slots()[0];
    assert!(
        upper.native().is_none(),
        "the MUX original owns the actual lower loan"
    );
    assert!(native.native().is_some_and(|native| !native.is_closed()));
    let loan = upper.borrowed_origin().unwrap();
    assert_eq!(loan.instance(), lower.instance_id());
    assert!(loan.owner().same_owner(&grpc));
    assert!(
        loan.parent().is_none(),
        "this actual gRPC original is the physical root"
    );
    for (slot, registration) in [(upper, &mux), (native, &grpc)] {
        let auxiliary = slot.auxiliary().expect("real owning preparation");
        let joined = auxiliary.try_joined().unwrap();
        assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
        assert!(auxiliary
            .scope_identity()
            .same_scope(&registration.bound().unwrap().scope_identity()));
        assert!(slot.acquisition_unknown().is_none());
    }
    assert!(!mux
        .bound()
        .unwrap()
        .scope_identity()
        .same_scope(&grpc.bound().unwrap().scope_identity()));
    Originals {
        mux,
        grpc,
        upper: upper.original().clone(),
        native: native.original().clone(),
        upper_driver: upper.driver().unwrap().1.clone(),
        native_driver: native.driver().unwrap().1.clone(),
    }
}
impl Originals {
    fn same_live(&self, client: &Running) {
        let current = capture(client);
        assert!(self.mux.same_owner(&current.mux) && self.grpc.same_owner(&current.grpc));
        assert!(
            self.upper.same_original(&current.upper) && self.native.same_original(&current.native)
        );
        assert!(self.upper.closed().is_none() && self.native.closed().is_none());
        assert!(!self.upper_driver.is_joined() && !self.native_driver.is_joined());
    }
    fn closed(&self) {
        assert!(self.upper.matches_closed(&self.upper.closed().unwrap()));
        let native = self.native.closed().unwrap();
        assert!(self.native.matches_closed(&native) && native.had_native_acquisition());
        for owner in [&self.mux, &self.grpc] {
            assert!(owner.closed().unwrap().matches(owner));
            assert!(owner.users_closed().unwrap().matches(owner));
        }
        for driver in [&self.upper_driver, &self.native_driver] {
            let joined = driver.try_joined().unwrap();
            assert!(driver.matches_joined(&joined) && joined.registration_released());
        }
    }
}
async fn connect(client: &Running, target: std::net::SocketAddr) -> TcpStream {
    let mut io = TcpStream::connect(client.address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&target.port().to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    io
}
async fn exchange(client: &Running) {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = target.local_addr().unwrap();
    let data: Vec<_> = (0..262144).map(|i| (i % 251) as u8).collect();
    let expected = data.clone();
    let peer = tokio::spawn(async move {
        let (mut io, _) = target.accept().await.unwrap();
        let mut received = Vec::new();
        io.read_to_end(&mut received).await.unwrap();
        eprintln!(
            "actual gRPC MUX target phase=upload EOF bytes={}",
            received.len()
        );
        assert_eq!(received, expected);
        io.write_all(&received).await.unwrap();
        eprintln!(
            "actual gRPC MUX target phase=reverse write accepted bytes={}",
            received.len()
        );
        io.shutdown().await.unwrap();
    });
    eprintln!("actual gRPC MUX exchange phase=SOCKS connect");
    let mut io = connect(client, endpoint).await;
    eprintln!("actual gRPC MUX exchange phase=SOCKS ready");
    io.write_all(&data).await.unwrap();
    eprintln!(
        "actual gRPC MUX exchange phase=upload accepted bytes={}",
        data.len()
    );
    io.shutdown().await.unwrap();
    eprintln!("actual gRPC MUX exchange phase=upload halfclosed");
    let mut received = Vec::new();
    io.read_to_end(&mut received).await.unwrap();
    eprintln!(
        "actual gRPC MUX exchange phase=reverse EOF bytes={}",
        received.len()
    );
    assert_eq!(received, data);
    peer.await.unwrap();
}
#[tokio::test]
async fn ordinary_mux_borrows_the_same_cached_grpc_original_for_tcp_udp_and_returned_logical_users()
{
    tokio::time::timeout(Duration::from_secs(30), async {
        for multi in [false, true] {
            let material = tempfile::tempdir().unwrap();
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let certificate = material.path().join("server.pem"); let key = material.path().join("server.key");
            std::fs::write(&certificate, cert.cert.pem()).unwrap();
            std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
            let grpc = serde_json::json!({"service_names":["same.mux/Tun"],"multi_mode":multi});
            let remote = start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"grpc":grpc,"tls":{"cert_path":certificate,"key_path":key}}}],"route":{"final":{"type":"direct"}}}), "remote").await;
            let client = start(serde_json::json!({"runtime":{"native_mux_max_carriers":1,"grpc_pool_max_carriers":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"mux_concurrency":8,"xudp_concurrency":8,"grpc":grpc,"tls":{"server_name":"localhost","ca_cert_path":certificate,"client_fingerprint":null}}}],"route":{"final":{"type":"route","outbound":"node"}}}), "entry").await;
            eprintln!("actual gRPC MUX mode={multi} phase=first TCP");
            exchange(&client).await;
            let originals = capture(&client);
            exchange(&client).await; originals.same_live(&client);
            let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut control = TcpStream::connect(client.address).await.unwrap();
            control.write_all(&[5, 1, 0]).await.unwrap(); let mut auth=[0;2]; control.read_exact(&mut auth).await.unwrap(); assert_eq!(auth,[5,0]);
            control.write_all(&[5,3,0,1,0,0,0,0,0,0]).await.unwrap(); let mut bound=[0;10]; control.read_exact(&mut bound).await.unwrap(); assert_eq!(&bound[..4],&[5,0,0,1]);
            let relay=std::net::SocketAddr::from(([127,0,0,1],u16::from_be_bytes([bound[8],bound[9]])));
            let socket=UdpSocket::bind("127.0.0.1:0").await.unwrap(); let codec=socks5::udp::Socks5InboundUdpCodec;
            for payload in [&b""[..], &b"same cached native"[..], &b""[..]] {
                let wire=codec.encode_response_to_client(&zero_core::Address::Ipv4([127,0,0,1]),target.local_addr().unwrap().port(),payload).unwrap();
                eprintln!("actual gRPC MUX mode={multi} phase=UDP send bytes={}", payload.len());
                socket.send_to(&wire,relay).await.unwrap(); let mut data=[0;65536];
                let (n,peer)=target.recv_from(&mut data).await.unwrap(); assert_eq!(&data[..n],payload);
                target.send_to(&data[..n],peer).await.unwrap(); let(n,_)=socket.recv_from(&mut data).await.unwrap();
                let (address,port,body)=codec.decode_response(&data[..n]).unwrap().into_parts();
                assert_eq!(address,zero_core::Address::Ipv4([127,0,0,1])); assert_eq!(port,target.local_addr().unwrap().port()); assert_eq!(body.as_slice(),payload);
                originals.same_live(&client);
            }
            drop(control); drop(socket);
            eprintln!("actual gRPC MUX mode={multi} phase=host shutdown");
            client.close().await; originals.closed(); remote.close().await;
        }
    }).await.unwrap();
}
