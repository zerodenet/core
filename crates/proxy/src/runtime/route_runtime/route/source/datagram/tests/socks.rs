//! Actual accepted TCP negotiation/UDP channel, original driver and native provider.
use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
#[tokio::test]
async fn accepted_socks_datagram_uses_host_bound_originals_and_keeps_control_lifetime() {
    timeout(Duration::from_secs(15),async {
        let target=UdpSocket::bind("127.0.0.1:0").await.unwrap();let address=target.local_addr().unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"egress","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv4","source_ip":"127.0.0.1"}}],
            "route":{"final":{"type":"route","outbound":"egress"}}
        }).to_string()).unwrap();
        let proxy=crate::Proxy::new(config).unwrap();let registry=proxy.system_tun_source_registry();
        let totals=proxy.protocols.runtime_host().original_budgets().carrier.clone();let running=proxy.spawn();
        let owner=loop {if let Some(owner)=registry.owner("socks"){break owner;}tokio::task::yield_now().await;};
        owner.wait_ready().await.unwrap();let entry=*owner.original_listener_address.get().unwrap();
        let client=UdpSocket::bind("127.0.0.1:0").await.unwrap();let local=client.local_addr().unwrap();
        let mut control=TcpStream::connect(entry).await.unwrap();
        control.write_all(&[5,1,0]).await.unwrap();let mut auth=[0;2];control.read_exact(&mut auth).await.unwrap();assert_eq!(auth,[5,0]);
        let mut associate=vec![5,3,0,1,127,0,0,1];associate.extend(local.port().to_be_bytes());control.write_all(&associate).await.unwrap();
        let mut reply=[0;10];control.read_exact(&mut reply).await.unwrap();assert_eq!(&reply[..4],&[5,0,0,1]);
        let relay=std::net::SocketAddr::new(std::net::Ipv4Addr::new(reply[4],reply[5],reply[6],reply[7]).into(),u16::from_be_bytes([reply[8],reply[9]]));
        for payload in [b"".as_slice(),b"accepted".as_slice()] {
            let mut packet=vec![0,0,0,1,127,0,0,1];packet.extend(address.port().to_be_bytes());packet.extend(payload);client.send_to(&packet,relay).await.unwrap();
            let mut bytes=[0;64];let (size,source)=target.recv_from(&mut bytes).await.unwrap();assert_eq!(&bytes[..size],payload);assert!(source.ip().is_loopback());
            assert!(totals.snapshot().charged>0,"physical accepted driver uses the actual Host original provider");
            target.send_to(payload,source).await.unwrap();let (size,from)=client.recv_from(&mut bytes).await.unwrap();assert_eq!(from,relay);assert_eq!(&bytes[10..size],payload);
        }
        // The actual TCP operation is explicitly pending while the UDP original
        // remains live, rather than inferring ownership from a port or a delay.
        {let mut byte=[0];let read=control.read(&mut byte);tokio::pin!(read);
        std::future::poll_fn(|cx|{use std::future::Future;assert!(read.as_mut().poll(cx).is_pending());std::task::Poll::Ready(())}).await;
        }
        drop(control);
        running.shutdown().await.unwrap();let terminal=owner.terminal_for_test().unwrap();
        assert!(terminal.failure().is_none() && terminal.task_failures().is_empty() && terminal.native_bound_failures().is_empty());
        assert!(terminal.closed_io_receipts()>0 && terminal.joined_tasks()>0);assert_eq!(totals.snapshot().charged,0);
    }).await.unwrap();
}
