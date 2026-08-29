fn response_header_with_counts(
    query: &[u8],
    answer_count: u16,
    authority_count: u16,
    additional_count: u16,
    rcode: u8,
    truncated: bool,
) -> Vec<u8> {
    zero_dns::udp::parse_dns_question(query).expect("parse question");
    let question_end = question_end(query);
    let mut response = Vec::new();
    response.extend_from_slice(&query[..2]);
    response.push(0x81 | if truncated { 0x02 } else { 0 });
    response.push(0x80 | rcode);
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&answer_count.to_be_bytes());
    response.extend_from_slice(&authority_count.to_be_bytes());
    response.extend_from_slice(&additional_count.to_be_bytes());
    response.extend_from_slice(&query[12..question_end]);
    response
}

fn append_test_record(response: &mut Vec<u8>, record_type: u16, ttl: u32, data: &[u8]) {
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&record_type.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&ttl.to_be_bytes());
    response.extend_from_slice(&(data.len() as u16).to_be_bytes());
    response.extend_from_slice(data);
}

pub(super) fn service_binding_response(query: &[u8], mandatory: &[u16]) -> Vec<u8> {
    let query_type = zero_dns::udp::parse_dns_question(query)
        .expect("parse service-binding question")
        .query_type;
    let mut response = response_header_with_counts(query, 1, 0, 2, 0, false);
    append_test_record(
        &mut response,
        query_type,
        120,
        &service_binding_data(mandatory),
    );
    append_test_record(&mut response, 1, 120, &[192, 0, 2, 45]);
    append_test_record(
        &mut response,
        28,
        120,
        &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 45],
    );
    response
}

fn service_binding_data(mandatory: &[u16]) -> Vec<u8> {
    let mut data = vec![0, 1, 0];
    if !mandatory.is_empty() {
        data.extend_from_slice(&0_u16.to_be_bytes());
        data.extend_from_slice(&((mandatory.len() * 2) as u16).to_be_bytes());
        for key in mandatory {
            data.extend_from_slice(&key.to_be_bytes());
        }
    }
    data.extend_from_slice(&1_u16.to_be_bytes());
    data.extend_from_slice(&3_u16.to_be_bytes());
    data.extend_from_slice(&[2, b'h', b'2']);
    data.extend_from_slice(&4_u16.to_be_bytes());
    data.extend_from_slice(&4_u16.to_be_bytes());
    data.extend_from_slice(&[192, 0, 2, 44]);
    data.extend_from_slice(&6_u16.to_be_bytes());
    data.extend_from_slice(&16_u16.to_be_bytes());
    data.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 44]);
    data
}

pub(super) fn response_records(response: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let count = usize::from(u16::from_be_bytes([response[6], response[7]]))
        + usize::from(u16::from_be_bytes([response[8], response[9]]))
        + usize::from(u16::from_be_bytes([response[10], response[11]]));
    let mut offset = question_end(response);
    let mut records = Vec::new();
    for _ in 0..count {
        let name_end = skip_name(response, offset);
        let record_type = u16::from_be_bytes([response[name_end], response[name_end + 1]]);
        let length = usize::from(u16::from_be_bytes([
            response[name_end + 8],
            response[name_end + 9],
        ]));
        let data_start = name_end + 10;
        let data_end = data_start + length;
        records.push((record_type, response[data_start..data_end].to_vec()));
        offset = data_end;
    }
    assert_eq!(offset, response.len());
    records
}

pub(super) fn service_binding_params(data: &[u8]) -> Vec<(u16, Vec<u8>)> {
    assert!(data.len() >= 3);
    assert_eq!(data[2], 0, "test service binding uses the root target");
    let mut offset = 3;
    let mut params = Vec::new();
    while offset < data.len() {
        let key = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let length = usize::from(u16::from_be_bytes([data[offset + 2], data[offset + 3]]));
        let value_start = offset + 4;
        let value_end = value_start + length;
        params.push((key, data[value_start..value_end].to_vec()));
        offset = value_end;
    }
    params
}

fn question_end(message: &[u8]) -> usize {
    skip_name(message, 12) + 4
}

fn skip_name(message: &[u8], mut offset: usize) -> usize {
    loop {
        let length = message[offset];
        if length & 0xc0 == 0xc0 {
            return offset + 2;
        }
        offset += 1;
        if length == 0 {
            return offset;
        }
        offset += usize::from(length);
    }
}
