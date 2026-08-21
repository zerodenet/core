use std::io;
use std::sync::atomic::{AtomicU16, Ordering};

use zero_traits::IpAddress;

use super::name::encode_name;
use super::{parse_question, MAX_UDP_DNS_PAYLOAD, RCODE_FORMERR, TYPE_A, TYPE_AAAA};

static DNS_ID: AtomicU16 = AtomicU16::new(1);

pub(crate) fn build_query(domain: &str, query_type: u16) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(128);
    output.extend_from_slice(&DNS_ID.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    output.extend_from_slice(&[0x01, 0x00]);
    output.extend_from_slice(&1_u16.to_be_bytes());
    output.extend_from_slice(&0_u16.to_be_bytes());
    output.extend_from_slice(&0_u16.to_be_bytes());
    output.extend_from_slice(&1_u16.to_be_bytes());
    encode_name(domain, &mut output)?;
    output.extend_from_slice(&query_type.to_be_bytes());
    output.extend_from_slice(&1_u16.to_be_bytes());
    output.push(0);
    output.extend_from_slice(&41_u16.to_be_bytes());
    output.extend_from_slice(&(MAX_UDP_DNS_PAYLOAD as u16).to_be_bytes());
    output.extend_from_slice(&0_u32.to_be_bytes());
    output.extend_from_slice(&0_u16.to_be_bytes());
    Ok(output)
}

pub(crate) fn build_address_response(
    query: &[u8],
    addresses: &[IpAddress],
    ttl_seconds: u32,
) -> Vec<u8> {
    let Ok(question) = parse_question(query) else {
        return build_error_response(query, RCODE_FORMERR, false);
    };
    let matching = addresses
        .iter()
        .filter(|address| {
            matches!(
                (question.query_type, address),
                (TYPE_A, IpAddress::V4(_)) | (TYPE_AAAA, IpAddress::V6(_))
            )
        })
        .collect::<Vec<_>>();
    let mut response = response_header_and_question(query, matching.len() as u16, 0, false);
    for address in matching {
        response.extend_from_slice(&[0xc0, 0x0c]);
        match address {
            IpAddress::V4(octets) => {
                response.extend_from_slice(&TYPE_A.to_be_bytes());
                response.extend_from_slice(&1_u16.to_be_bytes());
                response.extend_from_slice(&ttl_seconds.to_be_bytes());
                response.extend_from_slice(&4_u16.to_be_bytes());
                response.extend_from_slice(octets);
            }
            IpAddress::V6(octets) => {
                response.extend_from_slice(&TYPE_AAAA.to_be_bytes());
                response.extend_from_slice(&1_u16.to_be_bytes());
                response.extend_from_slice(&ttl_seconds.to_be_bytes());
                response.extend_from_slice(&16_u16.to_be_bytes());
                response.extend_from_slice(octets);
            }
        }
    }
    response
}

pub(crate) fn build_error_response(query: &[u8], response_code: u8, truncated: bool) -> Vec<u8> {
    response_header_and_question(query, 0, response_code, truncated)
}

pub(crate) fn fit_response_to_udp(query: &[u8], response: Vec<u8>) -> Vec<u8> {
    let limit = parse_question(query)
        .map(|question| question.udp_payload_size)
        .unwrap_or(512);
    if response.len() <= limit {
        return response;
    }
    let response_code = response.get(3).copied().unwrap_or(0) & 0x0f;
    build_error_response(query, response_code, true)
}

fn response_header_and_question(
    query: &[u8],
    answer_count: u16,
    response_code: u8,
    truncated: bool,
) -> Vec<u8> {
    let question = parse_question(query).ok();
    let id = query.get(..2).unwrap_or(&[0, 0]);
    let recursion_desired = query.get(2).copied().unwrap_or(0) & 0x01;
    let mut response = Vec::with_capacity(question.as_ref().map_or(12, |q| q.question_end + 32));
    response.extend_from_slice(id);
    response.push(0x80 | recursion_desired | if truncated { 0x02 } else { 0 });
    response.push(0x80 | (response_code & 0x0f));
    response.extend_from_slice(&(question.is_some() as u16).to_be_bytes());
    response.extend_from_slice(&answer_count.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    if let Some(question) = question {
        response.extend_from_slice(&query[12..question.question_end]);
    }
    response
}
