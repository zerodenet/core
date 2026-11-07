//! Existing Direct-first Flow relays borrow one actual Host TCP original.
use super::fixture::{self, Running};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration};
const ID: &str = "11111111-2222-3333-4444-555555555555";
struct Original {
    owner: CarrierOwnerRegistration,
    original: CarrierOriginObservation,
    native: bool,
}
fn capture(client: &Running) -> Vec<Original> {
    let services = client.proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let mut actual: Vec<Original> = Vec::new();
    for (index, tag) in ["first", "node"].into_iter().enumerate() {
        let slot = plan.configured(tag).unwrap();
        let entries: Vec<_> = (0..slot.recipe.spec().resources.len())
            .filter_map(|ordinal| slot.original_carrier_owner(ordinal))
            .filter_map(|owner| {
                let observed = owner.observe()?;
                if observed.retained() == 0 {
                    return None;
                }
                assert_eq!(observed.retained(), 1);
                let record = &observed.slots()[0];
                assert!(record.physical_view_live() && record.acquisition_unknown().is_none());
                assert_eq!(record.native().is_some(), index == 0);
                assert!(
                    record.driver().is_none(),
                    "per-connection inline transport has no cached driver"
                );
                let auxiliary = record
                    .auxiliary()
                    .expect("the original Direct/node setup Auxiliary");
                let joined = auxiliary
                    .try_joined()
                    .expect("setup returned before application delivery");
                assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
                assert!(owner
                    .bound()
                    .unwrap()
                    .scope_identity()
                    .same_scope(&auxiliary.scope_identity()));
                if index == 0 {
                    assert!(record.borrowed_origin().is_none());
                } else {
                    assert!(record
                        .borrowed_origin()
                        .unwrap()
                        .owner()
                        .same_owner(&actual[0].owner));
                }
                Some(Original {
                    original: record.original().clone(),
                    owner,
                    native: index == 0,
                })
            })
            .collect();
        assert_eq!(entries.len(), 1, "one real owner per configured role");
        actual.extend(entries);
    }
    assert!(!actual[0].owner.same_owner(&actual[1].owner));
    actual
}
async fn closed(originals: &[Original]) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !originals.iter().all(|original| {
            original.original.closed().is_some()
                && original
                    .owner
                    .observe()
                    .is_some_and(|state| state.retained() == 0)
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for original in originals {
        let closed = original.original.closed().unwrap();
        assert!(original.original.matches_closed(&closed));
        assert_eq!(closed.had_native_acquisition(), original.native);
    }
}
#[tokio::test]
async fn ordinary_direct_first_relay_uses_same_native_owner_and_real_node_loan_for_complete_halfclose(
) {
    tokio::time::timeout(Duration::from_secs(25),async {
        for transport in ["h2","http_upgrade"] {
            let directory=tempfile::tempdir().unwrap();
            let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert=directory.path().join("server.pem");let key=directory.path().join("server.key");
            std::fs::write(&cert,certificate.cert.pem()).unwrap();std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
            let remote=fixture::server(transport,&cert,&key).await;
            let mut node=serde_json::json!({"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"tls":{"server_name":"localhost","ca_cert_path":cert,"client_fingerprint":null}}});
            node["protocol"][transport]=serde_json::json!({"host":"front.test","path":"/original"});
            let client=fixture::start(serde_json::json!({
                "runtime":{"native_stream_max_originals":1},
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                "outbounds":[{"tag":"first","protocol":{"type":"direct"}},node],
                "outbound_groups":[{"tag":"chain","type":"relay","proxies":["first","node"]}],
                "route":{"final":{"type":"route","outbound":"chain"}}
            }),"entry").await;
            let target=TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port=target.local_addr().unwrap().port();
            let payload:Vec<u8>=(0..262144).map(|n|(n%251) as u8).collect();let expected=payload.clone();
            let peer=tokio::spawn(async move {
                let(mut io,_)=target.accept().await.unwrap();let mut body=Vec::new();
                io.read_to_end(&mut body).await.unwrap();assert!(body==expected);
                io.write_all(&body).await.unwrap();io.shutdown().await.unwrap();
            });
            let mut io=TcpStream::connect(client.address).await.unwrap();
            io.write_all(&[5,1,0]).await.unwrap();let mut auth=[0;2];io.read_exact(&mut auth).await.unwrap();assert_eq!(auth,[5,0]);
            let mut request=vec![5,1,0,1,127,0,0,1];request.extend_from_slice(&port.to_be_bytes());io.write_all(&request).await.unwrap();
            let mut reply=[0;10];io.read_exact(&mut reply).await.unwrap();assert_eq!(reply[1],0,"{:?}",client.proxy.engine().completed_sessions().iter().filter_map(|flow|flow.failure.as_ref()).collect::<Vec<_>>());
            io.write_all(&payload).await.unwrap();let originals=capture(&client);
            io.shutdown().await.unwrap();let mut received=Vec::new();io.read_to_end(&mut received).await.unwrap();
            assert!(received==payload,"{} of {} complete reverse bytes",received.len(),payload.len());
            drop(io);peer.await.unwrap();closed(&originals).await;
            let flow=tokio::time::timeout(Duration::from_secs(3),async {
                loop {if let Some(flow)=client.proxy.engine().completed_sessions().into_iter().find(|flow|flow.port==port){break flow;}tokio::task::yield_now().await;}
            }).await.unwrap();
            assert!(flow.failure.is_none());
            assert_eq!((flow.bytes_up,flow.bytes_down),(payload.len() as u64,payload.len() as u64));
            client.close().await;
            for original in originals {assert!(original.owner.closed().unwrap().matches(&original.owner));assert!(original.owner.users_closed().unwrap().matches(&original.owner));}
            remote.close().await;
        }
    }).await.expect("original ordinary Direct/node half-close deadline");
}
