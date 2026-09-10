use mieru::{
    crypto::{derive_key, MieruCipher},
    inbound::{multiplex::MieruInboundMultiplexer, MieruInboundProfile},
    metadata::*,
    segment::{build_data_segment, build_session_segment, parse_segment, Segment},
    session::MieruSession,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use zero_platform_tokio::TcpRelayStream;

pub const REQUEST: &[u8] = &[5, 1, 0, 1, 127, 0, 0, 1, 0, 80];
pub struct Peer {
    pub socket: DuplexStream,
    send: MieruCipher,
    recv: MieruCipher,
    first_send: bool,
    first_recv: bool,
    buffer: Vec<u8>,
}
impl Peer {
    pub async fn control(&mut self, id: u32, kind: u8, payload: &[u8]) {
        let mut meta = SessionMetadata::new(kind);
        meta.session_id = id;
        meta.timestamp = MieruSession::timestamp_minutes();
        meta.payload_length = payload.len() as u16;
        meta.suffix_length = 17;
        let wire = build_session_segment(&meta, payload, &mut self.send, self.first_send).unwrap();
        self.first_send = false;
        self.socket.write_all(&wire).await.unwrap();
    }
    pub async fn data(&mut self, id: u32, payload: &[u8]) {
        let mut meta = DataMetadata::new(DATA_CLIENT_TO_SERVER);
        meta.session_id = id;
        meta.payload_length = payload.len() as u16;
        meta.prefix_length = 7;
        meta.suffix_length = 13;
        let wire = build_data_segment(&meta, payload, &mut self.send, false).unwrap();
        self.socket.write_all(&wire).await.unwrap();
    }
    pub async fn next(&mut self) -> Segment {
        loop {
            match parse_segment(&self.buffer, &mut self.recv, self.first_recv, false) {
                Ok((frame, n)) => {
                    self.first_recv = false;
                    self.buffer.drain(..n);
                    return frame;
                }
                Err(zero_core::Error::Protocol("mieru: need more data")) => {}
                Err(error) => panic!("peer decode: {error}"),
            }
            let mut scratch = [0; 4096];
            let n = self.socket.read(&mut scratch).await.unwrap();
            assert!(n > 0, "unexpected peer EOF");
            self.buffer.extend_from_slice(&scratch[..n]);
        }
    }
}
pub async fn connect(payload: &[u8]) -> (Peer, MieruInboundMultiplexer) {
    connect_with_capacity(payload, 1 << 20).await
}
pub async fn connect_with_capacity(
    payload: &[u8],
    capacity: usize,
) -> (Peer, MieruInboundMultiplexer) {
    connect_with_options(payload, capacity, Default::default()).await
}
pub async fn connect_with_options(
    payload: &[u8],
    capacity: usize,
    options: mieru::config::MieruTransportOptions,
) -> (Peer, MieruInboundMultiplexer) {
    let (client, server) = tokio::io::duplex(capacity);
    let key = derive_key(
        "mux-user",
        "mux-password",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    let mut peer = Peer {
        socket: client,
        send: MieruCipher::new(&key),
        recv: MieruCipher::new(&key),
        first_send: true,
        first_recv: true,
        buffer: vec![],
    };
    let task = tokio::spawn(async move {
        MieruInboundProfile::from_config_users([("mux-user", "mux-password", Some("principal-1"))])
            .with_options(options)
            .accept_multiplexer(TcpRelayStream::new(server))
            .await
            .unwrap()
    });
    peer.control(101, OPEN_SESSION_REQUEST, payload).await;
    // A padded OPEN response can exceed a small test carrier's capacity.
    // Drain the peer concurrently with server acceptance, as real clients do.
    let (connection, response) = tokio::join!(task, peer.next());
    let connection = connection.unwrap();
    assert_eq!(
        response.session_meta.unwrap().protocol_type,
        OPEN_SESSION_RESPONSE
    );
    (peer, connection)
}
