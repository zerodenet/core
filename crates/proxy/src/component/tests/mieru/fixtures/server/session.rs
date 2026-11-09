use super::*;
use tokio::net::TcpStream;
use zero_core::InboundStreamUdpRelay;
fn host(address: &Address) -> String {
    match address {
        Address::Domain(host) => host.clone(),
        Address::Ipv4(ip) => std::net::Ipv4Addr::from(*ip).to_string(),
        Address::Ipv6(ip) => std::net::Ipv6Addr::from(*ip).to_string(),
    }
}
pub(in crate::component::tests::mieru) async fn run(
    route: mieru::inbound::MieruInboundAcceptedSession<
        mieru::inbound::multiplex::MieruLogicalStream,
    >,
    forward: bool,
) {
    match route {
        mieru::inbound::MieruInboundAcceptedSession::Tcp {
            session,
            mut stream,
        } => {
            if forward {
                eprintln!(
                    "Mieru fixture connecting actual TCP target {}:{}",
                    host(&session.target),
                    session.port
                );
                let mut peer = TcpStream::connect((host(&session.target).as_str(), session.port))
                    .await
                    .unwrap();
                eprintln!("Mieru fixture connected actual TCP target; forwarding logical bytes");
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut peer).await;
            } else {
                assert_eq!(session.target, destination().address);
                assert_eq!(session.port, destination().port);
                let (mut read, mut write) = tokio::io::split(stream);
                let _ = tokio::io::copy(&mut read, &mut write).await;
            }
        }
        mieru::inbound::MieruInboundAcceptedSession::Udp { relay, .. } => {
            let (mut stream, mut responder, _) = relay.into_stream_udp_parts();
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut injected = false;
            let mut bytes = vec![0; 65536];
            // One actual logical association owns its real socket. The owning
            // protocol retains partial frame input across this select's polls.
            loop {
                tokio::select! {
                    packet=responder.read_inbound_dispatch_tokio(&mut stream)=>{
                        let Ok(Some(packet))=packet else {break;};
                    if forward {
                        if !injected {
                            let noise=UdpSocket::bind("127.0.0.1:0").await.unwrap();
                            noise.send_to(b"unrelated UDP source",socket.local_addr().unwrap()).await.unwrap();
                            injected=true;
                        }
                        socket.send_to(packet.payload(),(host(packet.target()).as_str(),packet.port())).await.unwrap();
                    }
                        else {responder.write_response_for_target_tokio(&mut stream,packet.target(),packet.port(),packet.payload()).await.unwrap();}
                    }
                    result=socket.recv_from(&mut bytes),if forward=>{
                        let (size,peer)=result.unwrap();let address=match peer.ip() {std::net::IpAddr::V4(ip)=>Address::Ipv4(ip.octets()),std::net::IpAddr::V6(ip)=>Address::Ipv6(ip.octets())};
                        if responder.write_response_for_target_tokio(&mut stream,&address,peer.port(),&bytes[..size]).await.is_err() {break;}
                    }
                }
            }
        }
    }
}
