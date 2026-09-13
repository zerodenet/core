use super::*;
use crate::runtime::udp_flow::packet_path::{DatagramCodec, UdpDatagramDescriptor};
use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Default)]
struct Carrier {
    sent: Mutex<Vec<(Address, u16, Vec<u8>)>>,
    received: Mutex<VecDeque<Vec<u8>>>,
}

#[async_trait::async_trait]
impl PacketPathCarrier for Carrier {
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.sent
            .lock()
            .unwrap()
            .push((target.clone(), port, payload.to_vec()));
        Ok(())
    }

    async fn recv_from(&self, output: &mut [u8]) -> Result<usize, EngineError> {
        let wire = self
            .received
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected receive");
        assert!(
            wire.len() <= output.len(),
            "framing was limited by the plaintext buffer"
        );
        output[..wire.len()].copy_from_slice(&wire);
        Ok(wire.len())
    }
}

struct Codec(u8);
impl DatagramCodec<Address> for Codec {
    type Error = zero_core::Error;
    fn encode(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let ip = match target {
            Address::Ipv4(ip) => *ip,
            Address::Domain(name) => name.parse::<std::net::Ipv4Addr>().unwrap().octets(),
            _ => panic!("unexpected target"),
        };
        let mut wire = vec![self.0];
        wire.extend_from_slice(&ip);
        wire.extend_from_slice(&port.to_be_bytes());
        wire.extend_from_slice(payload);
        Ok(wire)
    }
    fn decode(&self, wire: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        if wire.len() < 7 || wire[0] != self.0 {
            return None;
        }
        Some((
            Address::Ipv4(wire[1..5].try_into().ok()?),
            u16::from_be_bytes(wire[5..7].try_into().ok()?),
            wire[7..].to_vec(),
        ))
    }
}

fn source(marker: u8, port: u16) -> UdpDatagramSource {
    UdpDatagramSource {
        descriptor: UdpDatagramDescriptor {
            tag: format!("hop-{marker}"),
            server: "127.0.0.1".into(),
            port,
            cache_key: marker.to_string(),
        },
        codec: Arc::new(Codec(marker)),
    }
}

#[tokio::test]
async fn nested_codecs_preserve_hop_order_and_receive_small_plaintext() {
    let carrier = Arc::new(Carrier::default());
    let path = wrap(wrap(carrier.clone(), source(1, 1001)), source(2, 1002));
    let destination = Address::Ipv4([192, 0, 2, 3]);
    path.send_to(&destination, 53, b"x").await.unwrap();
    let sent = carrier.sent.lock().unwrap().pop().unwrap();
    assert_eq!(sent.0, Address::Domain("127.0.0.1".into()));
    assert_eq!(sent.1, 1001);
    // Endpoint access retains a domain-valued address, even for numeric strings.
    let first = Codec(1).decode(&sent.2).unwrap();
    assert_eq!(first.0, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(first.1, 1002);
    assert_eq!(
        Codec(2).decode(&first.2).unwrap(),
        (destination.clone(), 53, b"x".to_vec())
    );
    let inner = Codec(2).encode(&destination, 53, b"y").unwrap();
    let outer = Codec(1)
        .encode(&Address::Ipv4([127, 0, 0, 1]), 1002, &inner)
        .unwrap();
    carrier.received.lock().unwrap().extend([vec![0], outer]);
    let mut output = [0; 1];
    assert_eq!(path.recv_from(&mut output).await.unwrap(), 1);
    assert_eq!(&output, b"y");
    let oversize = Codec(1).encode(&destination, 53, b"large").unwrap();
    carrier.received.lock().unwrap().push_back(oversize);
    let single = wrap(carrier, source(1, 1001));
    assert!(
        matches!(single.recv_from(&mut output).await, Err(EngineError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData)
    );
}
