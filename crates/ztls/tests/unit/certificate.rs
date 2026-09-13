use super::*;

fn message(entries: &[u8]) -> Vec<u8> {
    let mut data = vec![
        11,
        0,
        0,
        (entries.len() + 4) as u8,
        0,
        0,
        0,
        entries.len() as u8,
    ];
    data.extend_from_slice(entries);
    data
}

#[test]
fn server_certificate_validates_whole_chain_and_extension_boundaries() {
    let one = [0, 0, 3, 1, 2, 3, 0, 0];
    let data = message(&[one.as_slice(), one.as_slice()].concat());
    assert_eq!(server_chain(&data).unwrap(), vec![&[1, 2, 3][..]; 2]);
    for pos in [0, 3, 4, 7, 10, 14, 15, 18, 22, 23] {
        let mut invalid = data.clone();
        invalid[pos] = 255;
        assert!(server_chain(&invalid).is_err(), "offset {pos}");
    }
    for length in 0..data.len() {
        assert!(server_chain(&data[..length]).is_err());
    }
    assert!(server_chain(&message(&[])).is_err());
    assert!(server_chain(&message(&[0, 0, 0, 0, 0])).is_err());
    let extension = [0, 0, 1, 1, 0, 4, 0, 18, 0, 0];
    assert!(server_chain(&message(&extension)).is_ok());
    let duplicate = [0, 0, 1, 1, 0, 8, 0, 18, 0, 0, 0, 18, 0, 0];
    assert!(server_chain(&message(&duplicate)).is_err());
}
