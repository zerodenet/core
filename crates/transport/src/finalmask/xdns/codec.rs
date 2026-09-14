//! Xray XDNS query framing, EDNS policy and response payloads.
use super::wire::*;
use rand::RngCore;
use std::io;
pub const MAX_RESPONSE: usize = 1232;
pub const MAX_PAYLOAD: usize = 934;
pub fn query(domain: &Name, client: &[u8; 8], packet: &[u8]) -> io::Result<Vec<u8>> {
    if packet.len() >= 224 {
        return Err(invalid());
    }
    let padding = if packet.is_empty() { 8 } else { 3 };
    let mut data = client.to_vec();
    data.push(224 + padding as u8);
    let start = data.len();
    data.resize(start + padding, 0);
    rand::rng().fill_bytes(&mut data[start..]);
    if !packet.is_empty() {
        data.push(packet.len() as u8);
        data.extend(packet);
    }
    let encoded = data_encoding::BASE32_NOPAD
        .encode(&data)
        .to_ascii_lowercase();
    let mut name: Name = encoded.as_bytes().chunks(63).map(<[u8]>::to_vec).collect();
    name.extend(domain.clone());
    validate_name(&name)?;
    let message = Message {
        id: rand::random(),
        flags: 0x0100,
        questions: vec![Question {
            name,
            kind: 16,
            class: 1,
        }],
        records: [Vec::new(), Vec::new(), vec![opt()]],
    };
    encode(&message)
}
fn opt() -> Record {
    Record {
        name: Vec::new(),
        kind: 41,
        class: 4096,
        ttl: 0,
        data: Vec::new(),
    }
}
pub type QueryResponse = (Message, Option<[u8; 8]>, Vec<Vec<u8>>);
pub fn response_for(query: Message, domain: &Name) -> Option<QueryResponse> {
    if query.flags & 0x8000 != 0 {
        return None;
    }
    let mut response = Message {
        id: query.id,
        flags: 0x8000,
        questions: query.questions,
        ..Default::default()
    };
    let decoded = (|| {
        let mut size = 0;
        for record in &query.records[2] {
            if record.kind != 41 {
                continue;
            }
            if !response.records[2].is_empty() {
                return Err(1);
            }
            response.records[2].push(opt());
            if (record.ttl >> 16) & 255 != 0 {
                response.records[2][0].ttl = 1 << 24;
                return Err(0);
            }
            size = usize::from(record.class);
        }
        if response.questions.len() != 1 {
            return Err(1);
        }
        let question = &response.questions[0];
        let prefix = prefix(&question.name, domain).ok_or(3u16)?;
        response.flags |= 0x0400;
        if (query.flags >> 11) & 15 != 0 {
            return Err(4);
        }
        if question.kind != 16 {
            return Err(3);
        }
        let encoded = prefix.concat().to_ascii_uppercase();
        let data = data_encoding::BASE32_NOPAD
            .decode(&encoded)
            .map_err(|_| 3u16)?;
        if size < MAX_RESPONSE {
            return Err(1);
        }
        if data.len() < 8 {
            return Err(3);
        }
        Ok(data)
    })();
    match decoded {
        Err(code) => {
            response.flags |= code;
            Some((response, None, Vec::new()))
        }
        Ok(data) => {
            let id = data[..8].try_into().unwrap();
            let mut rest = &data[8..];
            let mut packets = Vec::new();
            while let Some((&length, tail)) = rest.split_first() {
                let n = if length >= 224 {
                    usize::from(length - 224)
                } else {
                    usize::from(length)
                };
                let Some(part) = tail.get(..n) else {
                    break;
                };
                if length < 224 {
                    packets.push(part.to_vec());
                }
                rest = &tail[n..];
            }
            Some((response, Some(id), packets))
        }
    }
}
pub fn response(mut message: Message, packets: &[Vec<u8>]) -> io::Result<Vec<u8>> {
    if message.flags & 15 == 0 && message.questions.len() == 1 {
        let question = &message.questions[0];
        let mut payload = Vec::new();
        for packet in packets {
            payload.extend((packet.len() as u16).to_be_bytes());
            payload.extend(packet);
        }
        message.records[0] = vec![Record {
            name: question.name.clone(),
            kind: question.kind,
            class: question.class,
            ttl: 60,
            data: txt_encode(&payload),
        }];
    }
    let mut bytes = encode(&message)?;
    if bytes.len() > MAX_RESPONSE {
        bytes.truncate(MAX_RESPONSE);
        bytes[2] |= 2;
    }
    Ok(bytes)
}
pub fn response_packets(message: Message, domain: &Name) -> io::Result<Vec<Vec<u8>>> {
    if message.flags & 0x800f != 0x8000 || message.records[0].len() != 1 {
        return Err(invalid());
    }
    let answer = &message.records[0][0];
    if answer.kind != 16 || prefix(&answer.name, domain).is_none() {
        return Err(invalid());
    }
    let data = txt_decode(&answer.data)?;
    let mut rest = data.as_slice();
    let mut packets = Vec::new();
    while rest.len() >= 2 {
        let n = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        rest = &rest[2..];
        let Some(packet) = rest.get(..n) else {
            break;
        };
        packets.push(packet.to_vec());
        rest = &rest[n..];
    }
    Ok(packets)
}
pub fn client_address(id: [u8; 8]) -> std::net::SocketAddr {
    let mut bytes = [0; 16];
    bytes[0] = 0xfd;
    bytes[8..].copy_from_slice(&id);
    std::net::SocketAddr::new(std::net::Ipv6Addr::from(bytes).into(), 0)
}
