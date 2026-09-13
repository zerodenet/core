use super::*;
fn compressed(algorithm: u16, certificate: &[u8]) -> Vec<u8> {
    let payload = match algorithm {
        1 => {
            use std::io::Write;
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(certificate).unwrap();
            encoder.finish().unwrap()
        }
        2 => {
            let mut payload = Vec::new();
            let parameters = brotli::enc::BrotliEncoderParams {
                quality: 4,
                ..Default::default()
            };
            brotli::BrotliCompress(&mut io::Cursor::new(certificate), &mut payload, &parameters)
                .unwrap();
            payload
        }
        3 => zstd::bulk::compress(certificate, 1).unwrap(),
        _ => unreachable!(),
    };
    let mut wire = vec![25];
    wire.extend_from_slice(&((payload.len() + 8) as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&algorithm.to_be_bytes());
    wire.extend_from_slice(&(certificate.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
    wire.extend_from_slice(&payload);
    wire
}
#[test]
fn advertised_algorithms_decode_with_exact_lengths_and_bounded_output() {
    let certificate = [0, 0, 0, 6, 0, 0, 1, 42, 0, 0];
    for algorithm in [1, 2, 3] {
        let wire = compressed(algorithm, &certificate);
        let decoded = decode(&wire).unwrap();
        assert_eq!(&decoded[4..], &certificate);
        for index in [3, 8, 11] {
            let mut bad = wire.clone();
            bad[index] += 1;
            assert!(decode(&bad).is_err());
        }
        let mut huge = wire.clone();
        huge[6..9].copy_from_slice(&[255, 255, 255]);
        assert!(decode(&huge).is_err());
        let mut unsupported = wire.clone();
        unsupported[5] = 4;
        assert!(decode(&unsupported).is_err());
        assert!(decode(&compressed(algorithm, &[0; 65536])).is_err());
    }
}
