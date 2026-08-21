use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use zero_stack::{packet, UserNetworkStack};
use zero_traits::TcpStack;

const CLIENT_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
const SERVER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
const CLIENT_PORT: u16 = 54_321;
const SERVER_PORT: u16 = 443;

fn client_packet(flags: u8, sequence: u32, acknowledgement: u32) -> Vec<u8> {
    client_packet_with_payload(flags, sequence, acknowledgement, &[])
}

fn client_packet_with_payload(
    flags: u8,
    sequence: u32,
    acknowledgement: u32,
    payload: &[u8],
) -> Vec<u8> {
    if flags & packet::tcp_flags::SYN != 0 && payload.is_empty() {
        return packet::build_tcp_with_mss(
            CLIENT_IP,
            SERVER_IP,
            CLIENT_PORT,
            SERVER_PORT,
            sequence,
            acknowledgement,
            flags,
            1_460,
        );
    }
    packet::build_tcp(
        CLIENT_IP,
        SERVER_IP,
        CLIENT_PORT,
        SERVER_PORT,
        sequence,
        acknowledgement,
        flags,
        payload,
    )
}

#[tokio::test]
async fn accepted_stream_preserves_segment_tail_across_small_reads() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 3_000;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    let payload = b"one-complete-tls-record-in-one-segment";
    tcp.feed(&client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 1,
        0,
        payload,
    ))
    .await;

    let mut actual = vec![0_u8; payload.len()];
    for chunk in actual.chunks_mut(5) {
        stream.read_exact(chunk).await.expect("read stream chunk");
    }
    assert_eq!(actual, payload);
}

#[tokio::test]
async fn retransmitted_segment_is_not_forwarded_twice() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 4_000;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    let payload = b"tls-record";
    let segment = client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 1,
        0,
        payload,
    );
    tcp.feed(&segment).await;
    tcp.feed(&segment).await;

    let mut actual = vec![0_u8; payload.len()];
    stream.read_exact(&mut actual).await.expect("read payload");
    assert_eq!(actual, payload);

    let mut extra = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.read(&mut extra))
            .await
            .is_err(),
        "duplicate segment was forwarded to the stream"
    );
}

#[tokio::test]
async fn overlapping_retransmission_forwards_only_new_bytes() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 5_000;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    tcp.feed(&client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 1,
        0,
        b"abcdef",
    ))
    .await;
    tcp.feed(&client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 4,
        0,
        b"defghi",
    ))
    .await;

    let mut actual = [0_u8; 9];
    stream.read_exact(&mut actual).await.expect("read payload");
    assert_eq!(&actual, b"abcdefghi");
}

#[tokio::test]
async fn out_of_order_segment_waits_for_missing_bytes() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 6_000;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    let future = client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 4,
        0,
        b"def",
    );
    tcp.feed(&future).await;

    let mut actual = [0_u8; 6];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.read(&mut actual))
            .await
            .is_err(),
        "out-of-order segment was forwarded before the gap was filled"
    );

    tcp.feed(&client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 1,
        0,
        b"abc",
    ))
    .await;
    tcp.feed(&future).await;

    stream.read_exact(&mut actual).await.expect("read payload");
    assert_eq!(&actual, b"abcdef");
}

#[tokio::test]
async fn receive_sequence_wraparound_preserves_exactly_once_delivery() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = u32::MAX - 2;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    let segment = client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence.wrapping_add(1),
        0,
        b"wrap",
    );
    tcp.feed(&segment).await;
    let acknowledgement = outbound_rx.recv().await.expect("wrapped ACK");
    assert_eq!(
        packet::parse_tcp(&acknowledgement)
            .expect("parse wrapped ACK")
            .ack,
        2
    );
    tcp.feed(&segment).await;
    let _duplicate_ack = outbound_rx.recv().await.expect("duplicate ACK");

    let mut payload = [0_u8; 4];
    stream
        .read_exact(&mut payload)
        .await
        .expect("wrapped payload");
    assert_eq!(&payload, b"wrap");
    let mut extra = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.read(&mut extra))
            .await
            .is_err(),
        "wrapped retransmission duplicated stream bytes"
    );
}

#[tokio::test]
async fn duplicate_syn_retransmits_syn_ack() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let syn = client_packet(packet::tcp_flags::SYN, 7_000, 0);

    tcp.feed(&syn).await;
    let first = outbound_rx.recv().await.expect("first SYN-ACK");
    tcp.feed(&syn).await;
    let second = outbound_rx.recv().await.expect("retransmitted SYN-ACK");

    let first = packet::parse_tcp(&first).expect("parse first SYN-ACK");
    let second = packet::parse_tcp(&second).expect("parse second SYN-ACK");
    assert!(first.syn && first.ack_flag);
    assert_eq!(second.seq, first.seq);
    assert_eq!(second.ack, first.ack);
}

#[tokio::test]
async fn half_open_connection_state_is_bounded() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();

    for index in 0..1_024_u16 {
        let syn = packet::build_tcp_with_mss(
            CLIENT_IP,
            SERVER_IP,
            20_000 + index,
            SERVER_PORT,
            u32::from(index),
            0,
            packet::tcp_flags::SYN,
            1_460,
        );
        tcp.feed(&syn).await;
        let response = outbound_rx.recv().await.expect("SYN-ACK response");
        assert!(packet::parse_tcp(&response).is_some_and(|tcp| tcp.syn && tcp.ack_flag));
    }

    let rejected = packet::build_tcp_with_mss(
        CLIENT_IP,
        SERVER_IP,
        30_000,
        SERVER_PORT,
        30_000,
        0,
        packet::tcp_flags::SYN,
        1_460,
    );
    tcp.feed(&rejected).await;
    let response = outbound_rx.recv().await.expect("connection-limit response");
    assert!(packet::parse_tcp(&response).is_some_and(|tcp| tcp.rst && tcp.ack_flag));
}

#[tokio::test]
async fn lost_syn_ack_is_retransmitted_by_the_rto_timer() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();

    tcp.feed(&client_packet(packet::tcp_flags::SYN, 7_500, 0))
        .await;
    let first = outbound_rx.recv().await.expect("initial SYN-ACK");
    let retransmitted = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("SYN-ACK retransmission timed out")
        .expect("outbound channel closed");

    let first = packet::parse_tcp(&first).expect("parse initial SYN-ACK");
    let retransmitted = packet::parse_tcp(&retransmitted).expect("parse retransmitted SYN-ACK");
    assert!(retransmitted.syn && retransmitted.ack_flag);
    assert_eq!(retransmitted.seq, first.seq);
    assert_eq!(retransmitted.ack, first.ack);
}

#[tokio::test]
async fn dropping_stack_releases_retransmission_workers_and_packet_channel() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, udp) = stack.into_parts();

    tcp.feed(&client_packet(packet::tcp_flags::SYN, 7_750, 0))
        .await;
    outbound_rx.recv().await.expect("initial SYN-ACK");
    drop(tcp);
    drop(udp);

    assert!(
        tokio::time::timeout(Duration::from_millis(100), outbound_rx.recv())
            .await
            .expect("retransmission worker retained packet channel")
            .is_none(),
        "packet channel remained open after stack shutdown"
    );
}

#[tokio::test]
async fn handshake_rejects_wrong_acknowledgement() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 8_000;

    tcp.feed(&client_packet(packet::tcp_flags::SYN, client_sequence, 0))
        .await;
    let syn_ack = outbound_rx.recv().await.expect("SYN-ACK packet");
    let syn_ack = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    tcp.feed(&client_packet(
        packet::tcp_flags::ACK,
        client_sequence + 1,
        syn_ack.seq,
    ))
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), tcp.accept())
            .await
            .is_err(),
        "invalid handshake acknowledgement was accepted"
    );

    tcp.feed(&client_packet(
        packet::tcp_flags::ACK,
        client_sequence + 1,
        syn_ack.seq + 1,
    ))
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), tcp.accept())
            .await
            .expect("valid handshake was not accepted")
            .is_some()
    );
}

#[tokio::test]
async fn handshake_ack_payload_is_preserved() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 9_000;

    tcp.feed(&client_packet(packet::tcp_flags::SYN, client_sequence, 0))
        .await;
    let syn_ack = outbound_rx.recv().await.expect("SYN-ACK packet");
    let syn_ack = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    tcp.feed(&client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 1,
        syn_ack.seq + 1,
        b"client-hello",
    ))
    .await;

    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");
    let mut actual = [0_u8; 12];
    stream.read_exact(&mut actual).await.expect("read payload");
    assert_eq!(&actual, b"client-hello");
}

#[tokio::test]
async fn outbound_stream_respects_mss_and_peer_window() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(128);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_000;
    let peer_window = 4_096;
    let server_sequence =
        establish_with_window(&tcp, &mut outbound_rx, client_sequence, peer_window).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    let payload = vec![7_u8; peer_window as usize];
    stream.write_all(&payload).await.expect("fill peer window");
    let first = outbound_rx.recv().await.expect("first data segment");
    let first = packet::parse_tcp(&first).expect("parse data segment");
    assert_eq!(first.payload.len(), 1_440);

    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.write(&[8]))
            .await
            .is_err(),
        "writer exceeded the peer receive window"
    );

    tcp.feed(&client_packet(
        packet::tcp_flags::ACK,
        client_sequence + 1,
        server_sequence
            .wrapping_add(1)
            .wrapping_add(u32::from(peer_window)),
    ))
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), stream.write(&[8]))
            .await
            .expect("ACK did not wake blocked writer")
            .expect("write after ACK failed"),
        1
    );
}

#[tokio::test]
async fn unacknowledged_stream_data_is_retransmitted_and_ack_cancels_timer() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_500;
    let server_sequence = establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    stream.write_all(b"reliable").await.expect("write payload");
    let first = outbound_rx.recv().await.expect("initial data segment");
    let retransmitted = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("data retransmission timed out")
        .expect("outbound channel closed");
    let first = packet::parse_tcp(&first).expect("parse initial data segment");
    let retransmitted = packet::parse_tcp(&retransmitted).expect("parse retransmitted data");
    assert_eq!(retransmitted.seq, first.seq);
    assert_eq!(retransmitted.payload, b"reliable");

    tcp.feed(&client_packet(
        packet::tcp_flags::ACK,
        client_sequence + 1,
        server_sequence + 1 + b"reliable".len() as u32,
    ))
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(350), outbound_rx.recv())
            .await
            .is_err(),
        "acknowledged data was retransmitted again"
    );
}

#[tokio::test]
async fn outbound_writer_waits_for_tun_queue_capacity() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(1);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_625;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    tcp.feed(&client_packet_with_payload(
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        client_sequence + 1,
        0,
        b"fill-queue-with-ack",
    ))
    .await;
    let pending_write = tokio::time::timeout(Duration::from_millis(20), stream.write(&[7])).await;
    assert!(pending_write.is_err(), "writer ignored full TUN queue");

    let acknowledgement = outbound_rx.recv().await.expect("client payload ACK");
    assert!(packet::parse_tcp(&acknowledgement).is_some_and(|packet| packet.ack_flag));
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), stream.write(&[7]))
            .await
            .expect("queue capacity did not wake writer")
            .expect("write after queue drain failed"),
        1
    );
    let data = outbound_rx.recv().await.expect("outbound data packet");
    assert_eq!(
        packet::parse_tcp(&data)
            .expect("parse outbound data")
            .payload,
        &[7]
    );
}

#[tokio::test]
async fn receive_window_closes_at_byte_limit_and_reopens_after_read() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(128);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_700;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    let mut sequence = client_sequence + 1;
    let mut remaining = u16::MAX as usize;
    let mut last_window = u16::MAX;
    while remaining > 0 {
        let count = remaining.min(1_024);
        tcp.feed(&client_packet_with_payload(
            packet::tcp_flags::PSH | packet::tcp_flags::ACK,
            sequence,
            0,
            &vec![7; count],
        ))
        .await;
        sequence = sequence.wrapping_add(count as u32);
        remaining -= count;
        let acknowledgement = outbound_rx.recv().await.expect("payload ACK");
        last_window = packet::tcp_window(&acknowledgement).expect("TCP receive window");
    }
    assert_eq!(last_window, 0);

    let mut byte = [0_u8; 1];
    stream
        .read_exact(&mut byte)
        .await
        .expect("drain receive byte");
    let update = tokio::time::timeout(Duration::from_millis(100), outbound_rx.recv())
        .await
        .expect("receive window did not reopen")
        .expect("outbound channel closed");
    assert_eq!(packet::tcp_window(&update), Some(1));
}

#[tokio::test]
async fn zero_peer_window_is_probed_and_window_update_wakes_writer() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_725;
    let server_sequence = establish_with_window(&tcp, &mut outbound_rx, client_sequence, 0).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.write(&[9]))
            .await
            .is_err(),
        "writer ignored the zero peer window"
    );
    let probe = tokio::time::timeout(Duration::from_millis(1_200), outbound_rx.recv())
        .await
        .expect("zero-window probe timed out")
        .expect("outbound channel closed");
    let probe = packet::parse_tcp(&probe).expect("parse zero-window probe");
    assert!(probe.ack_flag && probe.payload.is_empty());

    tcp.feed(&packet::build_tcp_with_window(
        CLIENT_IP,
        SERVER_IP,
        CLIENT_PORT,
        SERVER_PORT,
        client_sequence + 1,
        server_sequence + 1,
        packet::tcp_flags::ACK,
        1_024,
        &[],
    ))
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), stream.write(&[9]))
            .await
            .expect("peer window update did not wake writer")
            .expect("write after peer window update failed"),
        1
    );
}

#[tokio::test]
async fn unacknowledged_fin_is_retransmitted() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_750;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    stream.shutdown().await.expect("shutdown stream");
    let first = outbound_rx.recv().await.expect("initial FIN");
    let retransmitted = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
        .await
        .expect("FIN retransmission timed out")
        .expect("outbound channel closed");
    let first = packet::parse_tcp(&first).expect("parse initial FIN");
    let retransmitted = packet::parse_tcp(&retransmitted).expect("parse retransmitted FIN");
    assert!(first.fin && retransmitted.fin);
    assert_eq!(retransmitted.seq, first.seq);
}

#[tokio::test]
async fn retransmission_exhaustion_wakes_blocked_reader_and_writer() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(128);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 10_875;

    tcp.feed(&client_packet(packet::tcp_flags::SYN, client_sequence, 0))
        .await;
    // The outbound receiver is deliberately retained but not drained after
    // the handshake. This models successful TUN injection without any ACKs.
    let syn_ack = outbound_rx.recv().await.expect("SYN-ACK packet");
    let syn_ack = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    tcp.feed(&client_packet(
        packet::tcp_flags::ACK,
        client_sequence + 1,
        syn_ack.seq + 1,
    ))
    .await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    stream
        .write_all(&vec![7_u8; 14_401])
        .await
        .expect("fill initial congestion window");
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut byte = [0_u8; 1];
    let (read_result, write_result) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(reader.read(&mut byte), writer.write(&[8]))
    })
    .await
    .expect("retransmission retry limit was not enforced");

    assert_eq!(
        read_result.expect_err("reader should fail").kind(),
        std::io::ErrorKind::TimedOut
    );
    assert_eq!(
        write_result.expect_err("writer should fail").kind(),
        std::io::ErrorKind::TimedOut
    );
}

#[tokio::test]
async fn active_close_releases_four_tuple_after_peer_fin() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 11_000;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    stream.shutdown().await.expect("active close");
    let fin = outbound_rx.recv().await.expect("server FIN");
    let fin = packet::parse_tcp(&fin).expect("parse server FIN");
    assert!(fin.fin);
    tcp.feed(&client_packet(
        packet::tcp_flags::FIN | packet::tcp_flags::ACK,
        client_sequence + 1,
        fin.seq + 1,
    ))
    .await;
    let _final_ack = outbound_rx.recv().await.expect("final ACK");

    let replacement_syn = client_packet(packet::tcp_flags::SYN, 12_000, 0);
    tcp.feed(&replacement_syn).await;
    let replacement = outbound_rx.recv().await.expect("replacement SYN-ACK");
    assert!(packet::parse_tcp(&replacement).is_some_and(|packet| packet.syn && packet.ack_flag));
}

#[tokio::test]
async fn dropping_unclosed_stream_resets_local_client() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    establish(&tcp, &mut outbound_rx, 13_000).await;
    let (stream, _, _) = tcp.accept().await.expect("accepted connection");

    drop(stream);

    let reset = outbound_rx.recv().await.expect("reset packet");
    let reset = packet::parse_tcp(&reset).expect("parse reset packet");
    assert!(reset.rst && reset.ack_flag);
}

#[tokio::test]
async fn peer_reset_wakes_stream_without_echoing_reset() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();
    let client_sequence = 14_000;
    establish(&tcp, &mut outbound_rx, client_sequence).await;
    let (mut stream, _, _) = tcp.accept().await.expect("accepted connection");

    tcp.feed(&client_packet(
        packet::tcp_flags::RST | packet::tcp_flags::ACK,
        client_sequence + 1,
        0,
    ))
    .await;

    let mut byte = [0_u8; 1];
    let error = stream.read(&mut byte).await.expect_err("peer reset error");
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    drop(stream);
    assert!(
        outbound_rx.try_recv().is_err(),
        "RST was echoed to the peer"
    );
}

async fn establish(
    stack: &zero_stack::UserTcpStack,
    outbound: &mut mpsc::Receiver<Vec<u8>>,
    client_sequence: u32,
) -> u32 {
    stack
        .feed(&client_packet(packet::tcp_flags::SYN, client_sequence, 0))
        .await;
    let syn_ack = outbound.recv().await.expect("SYN-ACK packet");
    let syn_ack = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    stack
        .feed(&client_packet(
            packet::tcp_flags::ACK,
            client_sequence + 1,
            syn_ack.seq + 1,
        ))
        .await;
    syn_ack.seq
}

async fn establish_with_window(
    stack: &zero_stack::UserTcpStack,
    outbound: &mut mpsc::Receiver<Vec<u8>>,
    client_sequence: u32,
    window: u16,
) -> u32 {
    stack
        .feed(&client_packet(packet::tcp_flags::SYN, client_sequence, 0))
        .await;
    let syn_ack = outbound.recv().await.expect("SYN-ACK packet");
    let syn_ack = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    stack
        .feed(&packet::build_tcp_with_window(
            CLIENT_IP,
            SERVER_IP,
            CLIENT_PORT,
            SERVER_PORT,
            client_sequence + 1,
            syn_ack.seq + 1,
            packet::tcp_flags::ACK,
            window,
            &[],
        ))
        .await;
    syn_ack.seq
}

#[tokio::test]
async fn stale_accept_entry_cannot_duplicate_a_reused_four_tuple() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();

    establish(&tcp, &mut outbound_rx, 1_000).await;
    tcp.feed(&client_packet(packet::tcp_flags::RST, 1_001, 0))
        .await;
    establish(&tcp, &mut outbound_rx, 2_000).await;

    let (_stream, source, destination) =
        tokio::time::timeout(Duration::from_millis(100), tcp.accept())
            .await
            .expect("current connection was not accepted")
            .expect("accept queue closed");
    assert_eq!(source.port, CLIENT_PORT);
    assert_eq!(destination.port, SERVER_PORT);

    let second = tokio::time::timeout(Duration::from_millis(20), tcp.accept()).await;
    assert!(
        second.is_err(),
        "stale and current generations were both accepted"
    );
}
