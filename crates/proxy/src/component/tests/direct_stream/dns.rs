//! A real UDP DNS server. The original business resolver sends the request;
//! no fabricated DNS service, callback or prefilled resolution succeeds for it.
use super::*;
pub(super) async fn proxy() -> (
    Proxy,
    tokio::task::JoinHandle<usize>,
    tokio::sync::oneshot::Sender<()>,
) {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut queries = 0;
        loop {
            let mut bytes = [0; 2048];
            let (size, peer) = tokio::select! { biased;
                _ = &mut stopped => break,
                received = socket.recv_from(&mut bytes) => received.unwrap(),
            };
            queries += 1;
            assert!(
                queries <= 2,
                "two actual requests cannot create unbounded fixture queries"
            );
            assert!(size >= 16);
            let mut end = 12;
            let mut labels = Vec::new();
            while bytes[end] != 0 {
                let length = usize::from(bytes[end]);
                assert!(length <= 63 && end + 1 + length < size);
                labels.push(
                    std::str::from_utf8(&bytes[end + 1..end + 1 + length])
                        .unwrap()
                        .to_owned(),
                );
                end += 1 + length;
            }
            assert_eq!(labels.join("."), "direct.fixture.invalid");
            end += 5;
            assert!(end <= size);
            // Do not echo EDNS additional records into the answer section.
            let mut response = bytes[..end].to_vec();
            let size = end;
            assert_eq!(
                u16::from_be_bytes([response[size - 4], response[size - 3]]),
                1,
                "configured IPv4-only business DNS sends an actual A query"
            );
            assert_eq!(&response[size - 2..], &[0, 1]);
            response[2] |= 0x80;
            response[3] |= 0x80;
            response[6..8].copy_from_slice(&1u16.to_be_bytes());
            response[8..12].fill(0);
            response.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 30, 0, 4, 127, 0, 0, 1]);
            socket.send_to(&response, peer).await.unwrap();
        }
        queries
    });
    let config = zero_config::RuntimeConfig::parse(&format!(r#"{{"runtime":{{"dns":{{"servers":{{"actual":{{"type":"udp","host":"127.0.0.1","port":{}}}}},"default_server":"actual","policy":{{"address_family":"ipv4_only"}}}}}},"route":{{"final":{{"type":"direct"}}}}}}"#, address.port())).unwrap();
    (Proxy::new(config).unwrap(), server, stop)
}
