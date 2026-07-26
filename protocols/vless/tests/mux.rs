#![cfg(feature = "reality")]

use tokio::sync::mpsc;
use vless::inbound::{VlessInbound, VlessUser, VlessUserStore};
use vless::mux_pool::establish_outbound_mux_connection;
use zero_traits::AsyncSocket;

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";

fn pair() -> (MockSocket, MockSocket) {
    let (a_tx, a_rx) = mpsc::unbounded_channel::<u8>();
    let (b_tx, b_rx) = mpsc::unbounded_channel::<u8>();
    (
        MockSocket { rx: a_rx, tx: b_tx },
        MockSocket { rx: b_rx, tx: a_tx },
    )
}

struct MockSocket {
    rx: mpsc::UnboundedReceiver<u8>,
    tx: mpsc::UnboundedSender<u8>,
}

#[derive(Default)]
struct CaptureSocket {
    written: Vec<u8>,
}

impl AsyncSocket for CaptureSocket {
    type Error = ();

    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        Err(())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.written.extend_from_slice(buf);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl AsyncSocket for MockSocket {
    type Error = ();
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        // Read at least one byte, then as many more as available
        let first = self.rx.recv().await.ok_or(())?;
        buf[0] = first;
        let mut n = 1;
        while n < buf.len() {
            match self.rx.try_recv() {
                Ok(b) => {
                    buf[n] = b;
                    n += 1;
                }
                Err(_) => break,
            }
        }
        Ok(n)
    }
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        for &b in buf {
            let _ = self.tx.send(b);
        }
        Ok(())
    }
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct TestUsers([u8; 16]);
impl VlessUserStore for TestUsers {
    fn find_user(&self, id: &[u8; 16]) -> Option<VlessUser> {
        if id == &self.0 {
            Some(VlessUser::new())
        } else {
            None
        }
    }
}

fn uuid() -> [u8; 16] {
    vless::parse_uuid(USER_ID).unwrap()
}

#[tokio::test]
async fn mux_handshake_establishes() {
    let (client, server) = pair();
    let id = uuid();

    let server_h = tokio::spawn({
        let inbound = VlessInbound;
        let auth = TestUsers(id);
        let mut server = server;
        async move { inbound.handshake_mux_with_auth(&mut server, &auth).await }
    });

    let mut client = client;
    establish_outbound_mux_connection(&mut client, &id)
        .await
        .unwrap();

    let session = server_h.await.unwrap().unwrap();
    assert!(VlessInbound::is_mux_session(&session));
}

#[tokio::test]
async fn mux_request_omits_target_fields() {
    let id = uuid();
    let mut socket = CaptureSocket::default();
    establish_outbound_mux_connection(&mut socket, &id)
        .await
        .expect("write MUX request");

    let mut expected = Vec::with_capacity(19);
    expected.push(vless::VLESS_VERSION);
    expected.extend_from_slice(&id);
    expected.push(0);
    expected.push(3);
    assert_eq!(socket.written, expected);
}
