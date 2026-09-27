#![cfg(feature="shadowsocks")]
mod support;
use tokio::{net::UdpSocket,time::{timeout,Duration}};
use zero_core::Address;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
use zero_traits::DatagramCodec;
use shadowsocks::{CipherKind,udp::ShadowsocksDatagramCodec};
use support::{free_port,spawn_engine,wait_for_listener};
async fn receive(s:&UdpSocket)->(Vec<u8>,std::net::SocketAddr){let mut b=vec![0;65535];let (n,a)=timeout(Duration::from_secs(3),s.recv_from(&mut b)).await.unwrap().unwrap();(b[..n].to_vec(),a)}
#[tokio::test]
async fn legacy_udp_clients_share_flow_and_last_client_receives_other_response(){
 for separate_users in [false,true]{
 let port=free_port();
 let password_b=if separate_users {"password-b"}else{"password-a"};
 let cfg=RuntimeConfig::parse(&format!(r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"aes-128-gcm","users":[{{"password":"password-a","principal_key":"a"}},{{"password":"{password_b}","principal_key":"b"}}]}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#));
 // Same credential case needs just one configured user.
 let cfg=if separate_users {cfg.unwrap()} else {RuntimeConfig::parse(&format!(r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"aes-128-gcm","password":"password-a"}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#)).unwrap()};
 let running=spawn_engine(Proxy::new(cfg).unwrap());wait_for_listener(port).await;
 let target=UdpSocket::bind("127.0.0.1:0").await.unwrap();let target_port=target.local_addr().unwrap().port();
 let a=UdpSocket::bind("127.0.0.1:0").await.unwrap();let b=UdpSocket::bind("127.0.0.1:0").await.unwrap();
 let ca=ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm,"password-a");
 let cb=ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm,password_b);
 a.send_to(&ca.encode(&Address::Ipv4([127,0,0,1]),target_port,b"from-a").unwrap(),("127.0.0.1",port)).await.unwrap();let (pa,up_a)=receive(&target).await;
 b.send_to(&cb.encode(&Address::Ipv4([127,0,0,1]),target_port,b"from-b").unwrap(),("127.0.0.1",port)).await.unwrap();let (_,up_b)=receive(&target).await;
 assert_eq!(up_a,up_b,"expected current shared-flow defect");
 target.send_to(&pa,up_a).await.unwrap();let (reply,_)=receive(&b).await;
 assert_eq!(cb.decode(&reply).unwrap().2,b"from-a");
 println!("separate_users={separate_users}: B received A response; shared upstream socket={up_a}");
 running.shutdown().await.unwrap();
 }
}
#[tokio::test]
async fn principal_cancellation_stops_shared_ss_udp_listener(){
 let port=free_port();let key_a="MDEyMzQ1Njc4OWFiY2RlZg==";let key_b="YWJjZGVmMDEyMzQ1Njc4OQ==";
 let cfg=RuntimeConfig::parse(&format!(r#"{{"inbounds":[{{"tag":"ss","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"shadowsocks","cipher":"2022-blake3-aes-128-gcm","identity_password":"QUFBQUFBQUFBQUFBQUFBQQ==","users":[{{"password":"{key_a}","principal_key":"a"}},{{"password":"{key_b}","principal_key":"b"}}]}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#)).unwrap();
 let running=spawn_engine(Proxy::new(cfg).unwrap());wait_for_listener(port).await;
 let target=UdpSocket::bind("127.0.0.1:0").await.unwrap();let target_port=target.local_addr().unwrap().port();
 let a=UdpSocket::bind("127.0.0.1:0").await.unwrap();let b=UdpSocket::bind("127.0.0.1:0").await.unwrap();
 let ca=ShadowsocksDatagramCodec::new(CipherKind::Blake3Aes128Gcm,format!("QUFBQUFBQUFBQUFBQUFBQQ==:{key_a}"));let cb=ShadowsocksDatagramCodec::new(CipherKind::Blake3Aes128Gcm,format!("QUFBQUFBQUFBQUFBQUFBQQ==:{key_b}"));
 for (s,c,p) in [(&a,&ca,b"a"),(&b,&cb,b"b")] {s.send_to(&c.encode(&Address::Ipv4([127,0,0,1]),target_port,p).unwrap(),("127.0.0.1",port)).await.unwrap();let (data,up)=receive(&target).await;target.send_to(&data,up).await.unwrap();let (reply,_)=receive(s).await;assert_eq!(c.decode(&reply).unwrap().2,p);}
 assert_eq!(running.close_principal_flows("a","principal_disabled").len(),1);
 tokio::time::sleep(Duration::from_millis(100)).await;
 b.send_to(&cb.encode(&Address::Ipv4([127,0,0,1]),target_port,b"b-after").unwrap(),("127.0.0.1",port)).await.unwrap();
 let mut buf=[0;1024];assert!(timeout(Duration::from_millis(500),target.recv_from(&mut buf)).await.is_err());
 println!("A cancellation: previously working B no longer reaches target");
 running.shutdown().await.unwrap();
}
