use super::*;
use crate::shared::*;
use zero_platform_tokio::TokioSocket;
#[derive(Clone, Copy)]
pub(super) struct Case {
    pub(super) cipher: CipherKind,
    pub(super) method: &'static str,
    pub(super) user: &'static str,
    pub(super) identity: Option<&'static str>,
}
impl Case {
    pub(super) fn password(self) -> String {
        self.identity.map_or_else(
            || self.user.into(),
            |identity| format!("{identity}:{}", self.user),
        )
    }
}
pub(super) fn cases() -> [Case; 3] {
    [
        Case {
            cipher: CipherKind::Aes128Gcm,
            method: "aes-128-gcm",
            user: "facts-secret",
            identity: None,
        },
        Case {
            cipher: CipherKind::Blake3Aes128Gcm,
            method: "2022-blake3-aes-128-gcm",
            user: "MDEyMzQ1Njc4OWFiY2RlZg==",
            identity: None,
        },
        Case {
            cipher: CipherKind::Blake3Aes128Gcm,
            method: "2022-blake3-aes-128-gcm",
            user: "MDEyMzQ1Njc4OWFiY2RlZg==",
            identity: Some("ZmVkY2JhOTg3NjU0MzIxMA=="),
        },
    ]
}
pub(super) fn request(case: Case, seed: u8) -> (Vec<u8>, crate::ShadowsocksOutboundSession) {
    let cipher = case.cipher;
    let salt = vec![seed; cipher.salt_len()];
    let address = zero_core::Address::Domain("facts.example".into());
    let key = derive_session_key(cipher, case.user.as_bytes(), &salt).unwrap();
    let mut nonce = 0;
    let mut bytes = salt.clone();
    if cipher.is_blake3() {
        let chain = parse_2022_key_chain(cipher, case.password().as_bytes()).unwrap();
        bytes.extend_from_slice(
            &encode_tcp_2022_identity_headers(cipher, &chain.identity_keys, &chain.user_key, &salt)
                .unwrap(),
        );
        let variable = build_2022_request_var_header(&address, 443, &[], b"initial-").unwrap();
        let fixed = build_2022_request_fixed_header(now_unix_seconds(), variable.len() as u16);
        bytes.extend_from_slice(
            &encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &fixed).unwrap(),
        );
        bytes.extend_from_slice(
            &encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &variable).unwrap(),
        );
    } else {
        let initial = build_target_data(&address, 443, b"initial-").unwrap();
        bytes.extend_from_slice(&encrypt_tcp_chunk(cipher, &key, &mut nonce, &initial).unwrap());
    }
    bytes.extend_from_slice(&encrypt_tcp_chunk(cipher, &key, &mut nonce, b"tail").unwrap());
    (
        bytes,
        crate::ShadowsocksOutboundSession {
            legacy: None,
            session_key: key,
            next_upload_nonce: nonce,
            cipher,
            request_salt: salt,
        },
    )
}
pub(super) async fn connect_wire(bytes: &[u8]) -> (TokioSocket, TokioSocket) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    // Queue the complete original header before the server's SIP022 single-read
    // operation. No fabricated readiness, duplex pipe or protocol mock is used.
    client.write_all(bytes).await.unwrap();
    client.shutdown().await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (TokioSocket::new(server), TokioSocket::new(client))
}
