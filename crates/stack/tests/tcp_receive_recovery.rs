use std::{
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};
use tokio::{io::AsyncReadExt, sync::mpsc};
use zero_stack::{packet, UserNetworkStack};
use zero_traits::TcpStack;

const CLIENT: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
const SERVER: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

fn segment(flags: u8, sequence: u32, acknowledgement: u32, payload: &[u8]) -> Vec<u8> {
    packet::build_tcp(
        CLIENT,
        SERVER,
        54321,
        443,
        sequence,
        acknowledgement,
        flags,
        payload,
    )
}

async fn connection() -> (
    std::sync::Arc<zero_stack::UserTcpStack>,
    mpsc::Receiver<Vec<u8>>,
    zero_stack::UserTcpStream,
    u32,
) {
    let (sender, mut receiver) = mpsc::channel(128);
    let (tcp, _) = UserNetworkStack::new(sender, 1440).into_parts();
    tcp.feed(&segment(packet::tcp_flags::SYN, 1000, 0, &[]))
        .await;
    let response = receiver.recv().await.unwrap();
    let acknowledgement = packet::parse_tcp(&response).unwrap().seq + 1;
    tcp.feed(&segment(packet::tcp_flags::ACK, 1001, acknowledgement, &[]))
        .await;
    let (stream, _, _) = tcp.accept().await.unwrap();
    (tcp, receiver, stream, acknowledgement)
}

#[tokio::test]
async fn draining_after_a_tiny_window_reopens_advertises_further_capacity() {
    let (tcp, mut receiver, mut stream, acknowledgement) = connection().await;
    let mut sequence = 1001;
    let mut remaining = u16::MAX as usize;
    while remaining > 0 {
        let count = remaining.min(1024);
        tcp.feed(&segment(
            packet::tcp_flags::ACK,
            sequence,
            acknowledgement,
            &vec![7; count],
        ))
        .await;
        sequence += count as u32;
        remaining -= count;
        let response = receiver.recv().await.unwrap();
        assert_eq!(packet::tcp_window(&response), Some(remaining as u16));
    }
    stream.read_exact(&mut [0; 1]).await.unwrap();
    let update = receiver.recv().await.unwrap();
    assert_eq!(packet::tcp_window(&update), Some(1));
    // No incoming packet follows: a peer can still believe only one byte fits.
    stream.read_exact(&mut [0; 4096]).await.unwrap();
    let update = tokio::time::timeout(Duration::from_millis(100), receiver.recv())
        .await
        .expect("capacity growth was not advertised")
        .unwrap();
    assert_eq!(packet::tcp_window(&update), Some(4097));
    assert_eq!(packet::parse_tcp(&update).unwrap().ack, sequence);
}

#[tokio::test]
async fn abandoned_stream_retires_connection_instead_of_buffering_retransmissions() {
    let (tcp, mut receiver, stream, acknowledgement) = connection().await;
    drop(stream);
    let reset = receiver.recv().await.unwrap();
    assert!(packet::parse_tcp(&reset).unwrap().rst);
    // Give the notified connection worker an opportunity to retire its entry.
    tokio::time::sleep(Duration::from_millis(10)).await;
    tcp.feed(&segment(
        packet::tcp_flags::ACK,
        1001,
        acknowledgement,
        b"late payload",
    ))
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.recv())
            .await
            .is_err(),
        "abandoned connection still ACKs and buffers data after its stream was dropped"
    );
}

#[tokio::test]
async fn blocked_payload_is_retransmitted_once_after_reading_frees_space() {
    let (tcp, mut receiver, mut stream, acknowledgement) = connection().await;
    let initial = vec![7; u16::MAX as usize];
    let mut sequence = 1001;
    let mut full = Vec::new();
    for chunk in initial.chunks(1024) {
        tcp.feed(&segment(
            packet::tcp_flags::ACK,
            sequence,
            acknowledgement,
            chunk,
        ))
        .await;
        sequence += chunk.len() as u32;
        full = receiver.recv().await.unwrap();
    }
    let next = 1001 + initial.len() as u32;
    assert_eq!(packet::tcp_window(&full), Some(0));
    let payload = b"retry after backpressure";
    let retry = segment(packet::tcp_flags::ACK, next, acknowledgement, payload);
    tcp.feed(&retry).await;
    let rejected = receiver.recv().await.unwrap();
    assert_eq!(packet::parse_tcp(&rejected).unwrap().ack, next);
    assert_eq!(packet::tcp_window(&rejected), Some(0));
    let mut read = vec![0; initial.len()];
    stream.read_exact(&mut read).await.unwrap();
    assert_eq!(read, initial);
    let update = receiver.recv().await.unwrap();
    assert_eq!(packet::tcp_window(&update), Some(u16::MAX));
    tcp.feed(&retry).await;
    receiver.recv().await.unwrap();
    tcp.feed(&retry).await;
    receiver.recv().await.unwrap();
    let mut actual = vec![0; payload.len()];
    stream.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, payload);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.read(&mut [0; 1]))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn client_side_tunnel_stream_recovers_window_and_retires_when_abandoned() {
    let (sender, mut receiver) = mpsc::channel(128);
    let tcp =
        std::sync::Arc::new(zero_stack::ClientTcpStack::new(vec![CLIENT], sender, 1440).unwrap());
    let connecting = {
        let tcp = tcp.clone();
        tokio::spawn(async move {
            tcp.connect(CLIENT, std::net::SocketAddr::new(SERVER, 443))
                .await
        })
    };
    let syn = receiver.recv().await.unwrap();
    let syn = packet::parse_tcp(&syn).unwrap();
    let port = syn.src.port;
    let acknowledgement = syn.seq.wrapping_add(1);
    let response = |flags, sequence, payload: &[u8]| {
        packet::build_tcp(
            SERVER,
            CLIENT,
            443,
            port,
            sequence,
            acknowledgement,
            flags,
            payload,
        )
    };
    tcp.feed(&response(
        packet::tcp_flags::SYN | packet::tcp_flags::ACK,
        5000,
        &[],
    ))
    .await;
    let mut stream = connecting.await.unwrap().unwrap();
    receiver.recv().await.unwrap();
    let mut sequence = 5001;
    let mut remaining = u16::MAX as usize;
    while remaining > 0 {
        let count = remaining.min(1024);
        tcp.feed(&response(packet::tcp_flags::ACK, sequence, &vec![7; count]))
            .await;
        sequence += count as u32;
        remaining -= count;
        receiver.recv().await.unwrap();
    }
    stream.read_exact(&mut [0; 1]).await.unwrap();
    let update = receiver.recv().await.unwrap();
    assert_eq!(packet::tcp_window(&update), Some(1));
    stream.read_exact(&mut [0; 4096]).await.unwrap();
    let update = tokio::time::timeout(Duration::from_millis(100), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(packet::tcp_window(&update), Some(4097));
    let late = response(packet::tcp_flags::ACK, sequence, b"late payload");
    assert!(tcp.has_connection(&late).await);
    drop(stream);
    let reset = receiver.recv().await.unwrap();
    assert!(packet::parse_tcp(&reset).unwrap().rst);
    tokio::time::timeout(Duration::from_millis(100), async {
        while tcp.has_connection(&late).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abandoned client-side connection was not retired");
}

#[tokio::test]
async fn old_worker_retirement_preserves_a_replacement_connection_with_the_same_tuple() {
    let (tcp, mut receiver, old_stream, acknowledgement) = connection().await;
    tcp.feed(&segment(packet::tcp_flags::RST, 1001, acknowledgement, &[]))
        .await;
    tcp.feed(&segment(packet::tcp_flags::SYN, 2000, 0, &[]))
        .await;
    let response = receiver.recv().await.unwrap();
    let acknowledgement = packet::parse_tcp(&response).unwrap().seq + 1;
    tcp.feed(&segment(packet::tcp_flags::ACK, 2001, acknowledgement, &[]))
        .await;
    let (mut stream, _, _) = tcp.accept().await.unwrap();
    drop(old_stream);
    tokio::task::yield_now().await;
    tcp.feed(&segment(
        packet::tcp_flags::ACK,
        2001,
        acknowledgement,
        b"replacement",
    ))
    .await;
    let mut payload = [0; 11];
    tokio::time::timeout(Duration::from_millis(100), stream.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"replacement");
}
