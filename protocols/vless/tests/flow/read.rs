//! Pure wire decoding: these buffers provide no native ownership evidence.
use super::{flow_build_request, flow_read_request};
use crate::flow::FLOW_ZERO_AEAD_V1;
use zero_core::Address;
use zero_traits::AsyncSocket;
struct Bytes {
    data: Vec<u8>,
    read: usize,
    chunk: usize,
}
impl AsyncSocket for Bytes {
    type Error = core::convert::Infallible;
    async fn read(&mut self, output: &mut [u8]) -> Result<usize, Self::Error> {
        let count = output
            .len()
            .min(self.chunk)
            .min(self.data.len() - self.read);
        output[..count].copy_from_slice(&self.data[self.read..self.read + count]);
        self.read += count;
        Ok(count)
    }
    async fn write_all(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        panic!("request parsing must not write");
    }
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        panic!("pure parsing does not own stream shutdown");
    }
}
#[tokio::test]
async fn encrypted_lengths_authenticate_and_preserve_coalesced_payload_across_fragment_sizes() {
    let uuid = [7; 16];
    let body = b"first application payload after the original encrypted header";
    for target in [
        Address::Ipv4([203, 0, 113, 7]),
        Address::Ipv6([8; 16]),
        Address::Domain("x".repeat(253)),
    ] {
        for chunk in [1, 7, usize::MAX] {
            let (marker, mut data) =
                flow_build_request(&uuid, Some(FLOW_ZERO_AEAD_V1), 1, 443, &target).unwrap();
            assert_eq!(marker, 1);
            let original_len = data.len();
            data.extend_from_slice(body);
            let mut stream = Bytes {
                data,
                read: 0,
                chunk,
            };
            let decoded = flow_read_request(&mut stream, Some(FLOW_ZERO_AEAD_V1), &uuid)
                .await
                .unwrap();
            assert_eq!(decoded, (1, 443, target.clone()));
            assert_eq!(
                stream.read, original_len,
                "read exactly the authenticated original frame"
            );
            assert_eq!(
                &stream.data[stream.read..],
                body,
                "never discard first coalesced business bytes"
            );
        }
    }
}
#[tokio::test]
async fn wrong_key_and_corrupt_tag_never_publish_unauthed_command_or_destination() {
    let uuid = [7; 16];
    let (_, wire) = flow_build_request(
        &uuid,
        Some(FLOW_ZERO_AEAD_V1),
        1,
        443,
        &Address::Ipv4([203, 0, 113, 7]),
    )
    .unwrap();
    for corrupt in [false, true] {
        let mut data = wire.clone();
        if corrupt {
            *data.last_mut().unwrap() ^= 1;
        }
        let key = if corrupt { uuid } else { [8; 16] };
        let mut stream = Bytes {
            data,
            read: 0,
            chunk: 1,
        };
        assert!(
            flow_read_request(&mut stream, Some(FLOW_ZERO_AEAD_V1), &key)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn invalid_ciphertext_keeps_existing_padding_and_command_bound_without_consuming_tail() {
    let mut stream = Bytes {
        data: vec![0; 1024],
        read: 0,
        chunk: usize::MAX,
    };
    assert!(
        flow_read_request(&mut stream, Some(FLOW_ZERO_AEAD_V1), &[7; 16])
            .await
            .is_err()
    );
    assert_eq!(
        stream.read,
        8 + 1 + 31 + 2 + 300 + 16,
        "bounded original salt and authenticated frame search"
    );
}
