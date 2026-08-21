use std::io;

use zero_traits::IpAddress;

use super::name::{decode_name, normalize_domain, skip_name};
use super::{MAX_DNS_MESSAGE_SIZE, MAX_UDP_DNS_PAYLOAD, TYPE_A, TYPE_AAAA, TYPE_OPT};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsQuestion {
    pub domain: String,
    pub query_type: u16,
    pub(crate) query_class: u16,
    pub(crate) question_end: usize,
    pub(crate) udp_payload_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedDnsResponse {
    pub(crate) addresses: Vec<IpAddress>,
    pub(crate) min_ttl_seconds: Option<u32>,
    pub(crate) response_code: u8,
    pub(crate) truncated: bool,
}

pub(crate) fn parse_question(message: &[u8]) -> io::Result<DnsQuestion> {
    if message.len() < 12 || message.len() > MAX_DNS_MESSAGE_SIZE {
        return Err(invalid("DNS message length is invalid"));
    }
    if message[2] & 0x80 != 0 || message[2] & 0x78 != 0 {
        return Err(invalid("DNS request must be a standard query"));
    }
    if read_u16(message, 4)? != 1 {
        return Err(invalid("DNS request must contain exactly one question"));
    }
    let (domain, name_end) = decode_name(message, 12)?;
    let question_end = name_end
        .checked_add(4)
        .ok_or_else(|| invalid("DNS question length overflow"))?;
    if question_end > message.len() {
        return Err(invalid("incomplete DNS question"));
    }
    let domain = normalize_domain(&domain)?;
    let query_type = read_u16(message, name_end)?;
    let query_class = read_u16(message, name_end + 2)?;
    if query_class != 1 {
        return Err(invalid("only Internet-class DNS questions are supported"));
    }

    let answer_count = read_u16(message, 6)? as usize;
    let authority_count = read_u16(message, 8)? as usize;
    let additional_count = read_u16(message, 10)? as usize;
    let mut offset = question_end;
    for _ in 0..answer_count + authority_count {
        offset = skip_record(message, offset)?.0;
    }
    let mut udp_payload_size = 512_usize;
    for _ in 0..additional_count {
        let (next, record_type, class) = skip_record(message, offset)?;
        if record_type == TYPE_OPT {
            udp_payload_size = usize::from(class).clamp(512, MAX_UDP_DNS_PAYLOAD);
        }
        offset = next;
    }
    if offset != message.len() {
        return Err(invalid("DNS request has trailing bytes"));
    }
    Ok(DnsQuestion {
        domain,
        query_type,
        query_class,
        question_end,
        udp_payload_size,
    })
}

pub(crate) fn parse_response(query: &[u8], response: &[u8]) -> io::Result<ParsedDnsResponse> {
    let expected = parse_question(query)?;
    if response.len() < 12 || response.len() > MAX_DNS_MESSAGE_SIZE {
        return Err(invalid("DNS response length is invalid"));
    }
    if response[2] & 0x80 == 0 || response[2] & 0x78 != 0 {
        return Err(invalid("DNS response has invalid flags"));
    }
    if response[..2] != query[..2] {
        return Err(invalid("DNS response transaction ID does not match query"));
    }
    if read_u16(response, 4)? != 1 {
        return Err(invalid("DNS response must echo exactly one question"));
    }
    let (domain, name_end) = decode_name(response, 12)?;
    let response_question_end = name_end
        .checked_add(4)
        .ok_or_else(|| invalid("DNS response question length overflow"))?;
    if response_question_end > response.len()
        || normalize_domain(&domain)? != expected.domain
        || read_u16(response, name_end)? != expected.query_type
        || read_u16(response, name_end + 2)? != expected.query_class
    {
        return Err(invalid("DNS response question does not match query"));
    }

    let answer_count = read_u16(response, 6)? as usize;
    let authority_count = read_u16(response, 8)? as usize;
    let additional_count = read_u16(response, 10)? as usize;
    let mut offset = response_question_end;
    let mut addresses = Vec::new();
    let mut min_ttl = None;
    for _ in 0..answer_count {
        let (next, record_type, class, ttl, rdata) = record(response, offset)?;
        if class == 1 {
            min_ttl = Some(min_ttl.map_or(ttl, |current: u32| current.min(ttl)));
            match (record_type, expected.query_type, rdata.len()) {
                (TYPE_A, TYPE_A, 4) => {
                    addresses.push(IpAddress::V4([rdata[0], rdata[1], rdata[2], rdata[3]]))
                }
                (TYPE_AAAA, TYPE_AAAA, 16) => {
                    let mut octets = [0_u8; 16];
                    octets.copy_from_slice(rdata);
                    addresses.push(IpAddress::V6(octets));
                }
                _ => {}
            }
        }
        offset = next;
    }
    for _ in 0..authority_count {
        let (next, _, class, ttl, _) = record(response, offset)?;
        if class == 1 {
            min_ttl = Some(min_ttl.map_or(ttl, |current: u32| current.min(ttl)));
        }
        offset = next;
    }
    for _ in 0..additional_count {
        offset = skip_record(response, offset)?.0;
    }
    if offset != response.len() {
        return Err(invalid("DNS response has trailing bytes"));
    }
    Ok(ParsedDnsResponse {
        addresses,
        min_ttl_seconds: min_ttl,
        response_code: response[3] & 0x0f,
        truncated: response[2] & 0x02 != 0,
    })
}

fn record(data: &[u8], offset: usize) -> io::Result<(usize, u16, u16, u32, &[u8])> {
    let name_end = skip_name(data, offset)?;
    if name_end + 10 > data.len() {
        return Err(invalid("truncated DNS resource record"));
    }
    let record_type = read_u16(data, name_end)?;
    let class = read_u16(data, name_end + 2)?;
    let ttl = read_u32(data, name_end + 4)?;
    let length = read_u16(data, name_end + 8)? as usize;
    let data_start = name_end + 10;
    let next = data_start
        .checked_add(length)
        .ok_or_else(|| invalid("DNS resource data length overflow"))?;
    let rdata = data
        .get(data_start..next)
        .ok_or_else(|| invalid("truncated DNS resource data"))?;
    Ok((next, record_type, class, ttl, rdata))
}

fn skip_record(data: &[u8], offset: usize) -> io::Result<(usize, u16, u16)> {
    record(data, offset).map(|(next, record_type, class, _, _)| (next, record_type, class))
}

fn read_u16(data: &[u8], offset: usize) -> io::Result<u16> {
    let bytes = data
        .get(offset..offset + 2)
        .ok_or_else(|| invalid("truncated DNS integer"))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(data: &[u8], offset: usize) -> io::Result<u32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| invalid("truncated DNS integer"))?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
