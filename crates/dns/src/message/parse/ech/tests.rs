use super::parse_ech_config_response;

#[test]
fn follows_a_trusted_cname_chain_to_the_https_ech_record() {
    let query = crate::message::build_query("alias.example", crate::message::TYPE_HTTPS).unwrap();
    let question_end = crate::message::parse_question(&query).unwrap().question_end;
    let mut response = query[..question_end].to_vec();
    response[2] = 0x81;
    response[3] = 0x80;
    response[6..8].copy_from_slice(&2_u16.to_be_bytes());
    response[8..12].fill(0);

    append_record(
        &mut response,
        &[0xc0, 0x0c],
        5,
        30,
        &encoded_name("canonical.example"),
    );
    let material = [0, 4, 0xfe, 0x0d, 0, 0];
    let mut https = vec![0, 1, 0];
    https.extend_from_slice(&5_u16.to_be_bytes());
    https.extend_from_slice(&(material.len() as u16).to_be_bytes());
    https.extend_from_slice(&material);
    append_record(
        &mut response,
        &encoded_name("canonical.example"),
        crate::message::TYPE_HTTPS,
        120,
        &https,
    );

    let parsed = parse_ech_config_response(&query, &response).unwrap();
    assert_eq!(parsed.config_list.as_deref(), Some(material.as_slice()));
    assert_eq!(parsed.ttl_seconds, 30);
}

fn append_record(message: &mut Vec<u8>, owner: &[u8], record_type: u16, ttl: u32, rdata: &[u8]) {
    message.extend_from_slice(owner);
    message.extend_from_slice(&record_type.to_be_bytes());
    message.extend_from_slice(&1_u16.to_be_bytes());
    message.extend_from_slice(&ttl.to_be_bytes());
    message.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    message.extend_from_slice(rdata);
}

fn encoded_name(name: &str) -> Vec<u8> {
    let mut encoded = Vec::new();
    for label in name.split('.') {
        encoded.push(label.len() as u8);
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    encoded
}
