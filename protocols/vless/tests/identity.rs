use vless::{format_uuid, parse_uuid};

#[test]
fn custom_ids_match_reference_uuid_v5_vectors() {
    for (input, expected) in [
        ("alice", "ec61d669-fc85-586b-a097-822a49b51e7e"),
        ("Alice", "7fd757a2-2173-5a60-8d25-615994740358"),
        (" 用户 ", "213da4ec-9526-5a4e-be8e-484edbaedda7"),
        (
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "d20a3bd4-9d58-52e0-8caa-820ca42d1ad0",
        ),
    ] {
        assert_eq!(format_uuid(&parse_uuid(input).unwrap()), expected);
    }
    assert_ne!(parse_uuid("alice"), parse_uuid("Alice"));
    assert_ne!(parse_uuid("alice"), parse_uuid(" alice"));
}

#[test]
fn custom_id_limit_counts_utf8_bytes() {
    assert!(parse_uuid(&"中".repeat(10)).is_ok());
    assert!(parse_uuid(&"中".repeat(11)).is_err());
    assert!(parse_uuid("").is_err());
    assert!(parse_uuid(&"a".repeat(31)).is_err());
    assert!(parse_uuid(&"a".repeat(37)).is_err());
}

#[test]
fn uuid_group_separators_match_reference() {
    let expected = parse_uuid("11111111-2222-3333-4444-555555555555").unwrap();
    for input in [
        "11111111222233334444555555555555",
        "11111111-22223333-4444555555555555",
    ] {
        assert_eq!(parse_uuid(input).unwrap(), expected);
    }
}
