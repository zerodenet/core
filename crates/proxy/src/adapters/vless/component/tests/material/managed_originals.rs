//! Actual MUX and cached gRPC drivers receive their same lower QUIC original.
use super::fixture::{self, Running};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::contract::scope::TaskJoinWitness;
const ID: &str = "11111111-2222-3333-4444-555555555555";
async fn echo(client: &Running) {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let payload: Vec<u8> = (0..262144).map(|index| (index % 251) as u8).collect();
        let expected = payload.clone();
        let peer = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut body = Vec::new();
            io.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, expected);
            io.write_all(&body).await.unwrap();
            io.write_all(&[71; 1025]).await.unwrap();
            io.shutdown().await.unwrap();
        });
        let mut io = TcpStream::connect(client.address).await.unwrap();
        io.write_all(&[5, 1, 0]).await.unwrap();
        let mut auth = [0; 2];
        io.read_exact(&mut auth).await.unwrap();
        assert_eq!(auth, [5, 0]);
        let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
        request.extend_from_slice(&endpoint.port().to_be_bytes());
        io.write_all(&request).await.unwrap();
        let mut reply = [0; 10];
        io.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[1], 0);
        io.write_all(&payload).await.unwrap();
        io.flush().await.unwrap();
        io.shutdown().await.unwrap();
        let mut reverse = Vec::new();
        io.read_to_end(&mut reverse).await.unwrap();
        assert_eq!(reverse.len(), 262144 + 1025);
        assert_eq!(&reverse[..262144], payload);
        assert_eq!(&reverse[262144..], &[71; 1025]);
        drop(io);
        peer.await.unwrap();
    })
    .await
    .unwrap();
}
fn receiver(client: &Running, grpc: bool) -> TaskJoinWitness {
    let plan = client
        .proxy
        .tcp_runtime_services()
        .components
        .clone()
        .unwrap();
    let node = plan.configured("node").unwrap();
    let mux = plan.component(node.recipe.spec().dependencies[0]).unwrap();
    let owner = if grpc {
        mux.dependency_instances().iter().filter_map(|id|plan.component(id.component))
            .find(|slot|matches!(&slot.recipe.spec().owner,zero_platform_tokio::contract::component::ResourceOwner::Endpoint(name) if name.as_ref()=="grpc.client_pool")).unwrap()
    } else {
        mux
    };
    let registration = (0..owner.recipe.spec().resources.len())
        .find_map(|ordinal| owner.original_carrier_owner(ordinal))
        .unwrap();
    assert_eq!(registration.limit().get(), 1);
    let observed = registration.observe().unwrap();
    assert_eq!(observed.retained(), 1);
    let row = &observed.slots()[0];
    assert!(row.native().is_none() && row.borrowed_origin().is_some());
    let initializer = row.auxiliary().unwrap();
    let joined = initializer.try_joined().unwrap();
    assert!(initializer.matches_joined(&joined) && joined.registration_released());
    let driver = row.driver().unwrap().1.clone();
    assert!(!driver.is_joined());
    driver
}
#[tokio::test]
async fn ordinary_mux_and_cached_grpc_receive_exact_quic_child_then_keep_it_across_business_return()
{
    tokio::time::timeout(Duration::from_secs(35),async {
        for grpc in [false,true] {
            let material=tempfile::tempdir().unwrap();
            let cert=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let ca=material.path().join("ca.pem");let key=material.path().join("key.pem");
            std::fs::write(&ca,cert.cert.pem()).unwrap();std::fs::write(&key,cert.signing_key.serialize_pem()).unwrap();
            let remote=fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":ca,"key_path":key}}}],"route":{"final":{"type":"direct"}}}),"remote").await;
            let mut upper_protocol=serde_json::json!({"type":"vless","users":[{"id":ID}]});
            if grpc {upper_protocol["grpc"]=serde_json::json!({"service_names":["actual.child/Tun"],"multi_mode":true});}
            let upper=fixture::start(serde_json::json!({"inbounds":[{"tag":"upper","listen":{"address":"127.0.0.1","port":0},"protocol":upper_protocol}],"route":{"final":{"type":"direct"}}}),"upper").await;
            let mut protocol=serde_json::json!({"type":"vless","server":"127.0.0.1","port":upper.address.port(),"id":ID,"mux_concurrency":8,"xudp_concurrency":8});
            if grpc {protocol["grpc"]=serde_json::json!({"service_names":["actual.child/Tun"],"multi_mode":true});}
            let client=fixture::start(serde_json::json!({"runtime":{"native_stream_max_originals":1,"native_mux_max_carriers":1,"grpc_pool_max_carriers":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"prefix","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"quic":{"server_name":"localhost","ca_cert_path":ca}}},{"tag":"node","protocol":protocol}],"outbound_groups":[{"tag":"chain","type":"relay","proxies":["prefix","node"]}],"route":{"final":{"type":"route","outbound":"chain"}}}),"entry").await;
            echo(&client).await;
            let plan=client.proxy.tcp_runtime_services().components.clone().unwrap();
            let slot=plan.configured("prefix").unwrap();let owner=slot.original_metadata_owner(0).unwrap();
            let original=owner.current_original().unwrap();let receipt=original.retained_protocol_receiver().expect("actual cached driver accepted this exact unique lower LoanOwner");
            let driver=receiver(&client,grpc);assert!(driver.same_registration(receipt.protocol_driver()));
            assert!(original.closed().is_none()&&original.registration_observation().released().is_none());
            echo(&client).await;
            assert!(owner.current_original().unwrap().same_original(&original));
            assert!(driver.same_registration(&receiver(&client,grpc))&&!driver.is_joined());
            assert!(original.closed().is_none());
            client.close().await;
            let closed=original.closed().unwrap();assert!(closed.matches(&original)&&closed.had_native_acquisition());
            let registration=original.registration_observation();assert!(registration.released().unwrap().matches(&registration));
            let joined=driver.try_joined().unwrap();assert!(joined.registration_released());joined.result().unwrap();
            assert!(owner.closed().unwrap().matches(&owner));
            upper.close().await;remote.close().await;
        }
    }).await.unwrap();
}
