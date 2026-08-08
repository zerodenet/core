pub(super) fn starts_with_tls_application_data(content: &[u8]) -> bool {
    content.len() >= 5 && content[0] == 0x17 && content[1] == 0x03 && content[2] == 0x03
}

pub(super) fn contains_tls13_server_hello(input: &[u8]) -> bool {
    let mut offset = 0;
    while offset + 5 <= input.len() {
        let record_len = u16::from_be_bytes([input[offset + 3], input[offset + 4]]) as usize;
        let end = offset + 5 + record_len;
        if end > input.len() {
            return false;
        }
        let record = &input[offset + 5..end];
        if input[offset] == 0x16
            && record.len() >= 4
            && record[0] == 0x02
            && server_hello_selects_tls13(record)
        {
            return true;
        }
        offset = end;
    }
    false
}

fn server_hello_selects_tls13(handshake: &[u8]) -> bool {
    if handshake.len() < 4 + 2 + 32 + 1 {
        return false;
    }
    let body_len =
        ((handshake[1] as usize) << 16) | ((handshake[2] as usize) << 8) | handshake[3] as usize;
    if handshake.len() < 4 + body_len {
        return false;
    }
    let body = &handshake[4..4 + body_len];
    let session_id_len = body[34] as usize;
    let mut offset = 35 + session_id_len;
    if offset + 2 + 1 + 2 > body.len() {
        return false;
    }
    offset += 3; // cipher suite + compression method
    let extensions_len = u16::from_be_bytes([body[offset], body[offset + 1]]) as usize;
    offset += 2;
    let end = (offset + extensions_len).min(body.len());
    while offset + 4 <= end {
        let extension_type = u16::from_be_bytes([body[offset], body[offset + 1]]);
        let extension_len = u16::from_be_bytes([body[offset + 2], body[offset + 3]]) as usize;
        offset += 4;
        if offset + extension_len > end {
            return false;
        }
        if extension_type == 0x002b
            && extension_len == 2
            && body[offset..offset + 2] == [0x03, 0x04]
        {
            return true;
        }
        offset += extension_len;
    }
    false
}
