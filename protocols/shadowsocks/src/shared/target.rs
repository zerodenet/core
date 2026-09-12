//! Address parsing across authenticated legacy AEAD chunks.
use crate::CipherKind;
use zero_core::Error;
use zero_traits::AsyncSocket;
pub(crate) async fn complete_tcp_target<S: AsyncSocket>(
    stream: &mut S,
    cipher: CipherKind,
    key: &[u8],
    nonce: &mut u128,
    mut plain: Vec<u8>,
) -> Result<Vec<u8>, Error> {
    loop {
        let needed = match plain.first() {
            None => None,
            Some(1) => Some(7),
            Some(4) => Some(19),
            Some(3) => plain.get(1).map(|size| *size as usize + 4),
            Some(_) => return Err(Error::Protocol("ss: unknown address type")),
        };
        if needed.is_some_and(|needed| plain.len() >= needed) {
            return Ok(plain);
        }
        plain.extend_from_slice(&super::read_tcp_chunk(stream, cipher, key, nonce).await?);
    }
}
