use std::io;
mod service;
use service::rewrite_service_binding;

use super::name::skip_name;
use super::{parse_response, TYPE_A, TYPE_AAAA, TYPE_HTTPS, TYPE_SVCB};

const SVC_PARAM_MANDATORY: u16 = 0;
const SVC_PARAM_IPV4_HINT: u16 = 4;
const SVC_PARAM_IPV6_HINT: u16 = 6;
const PRIVATE_USE_START: u16 = 65_280;
const PRIVATE_USE_END: u16 = 65_534;
const FILTERED_RECORD_TYPE: u16 = PRIVATE_USE_START;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResponseAddressPolicy {
    pub(crate) allow_ipv4: bool,
    pub(crate) allow_ipv6: bool,
    pub(crate) suppress_real_addresses: bool,
}

impl ResponseAddressPolicy {
    fn strips_ipv4(self) -> bool {
        self.suppress_real_addresses || !self.allow_ipv4
    }

    fn strips_ipv6(self) -> bool {
        self.suppress_real_addresses || !self.allow_ipv6
    }
}

/// Apply the intercepted-DNS address-family contract without changing the
/// message length. Keeping every record at its original offset preserves DNS
/// compression pointers in otherwise opaque record data.
pub(crate) fn apply_response_address_policy_in_place(
    query: &[u8],
    response: &mut [u8],
    policy: ResponseAddressPolicy,
    memory: Option<&crate::DnsWireMemory>,
) -> io::Result<()> {
    parse_response(query, response)?;
    if !policy.strips_ipv4() && !policy.strips_ipv6() {
        return Ok(());
    }

    let response_question_end = skip_name(&response, 12)?
        .checked_add(4)
        .ok_or_else(|| invalid("DNS response question length overflow"))?;
    if response_question_end > response.len() {
        return Err(invalid("incomplete DNS response question"));
    }
    let record_count = usize::from(read_u16(&response, 6)?)
        + usize::from(read_u16(&response, 8)?)
        + usize::from(read_u16(&response, 10)?);
    let mut offset = response_question_end;
    for _ in 0..record_count {
        let record = record_span(&response, offset)?;
        match record.record_type {
            TYPE_A if policy.strips_ipv4() => filter_address_record(response, record),
            TYPE_AAAA if policy.strips_ipv6() => filter_address_record(response, record),
            TYPE_SVCB | TYPE_HTTPS => rewrite_service_binding(response, record, policy, memory)?,
            _ => {}
        }
        offset = record.next;
    }
    if offset != response.len() {
        return Err(invalid("DNS response has trailing bytes"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RecordSpan {
    next: usize,
    record_type: u16,
    type_offset: usize,
    data_start: usize,
    data_end: usize,
}

fn record_span(message: &[u8], offset: usize) -> io::Result<RecordSpan> {
    let name_end = skip_name(message, offset)?;
    if name_end + 10 > message.len() {
        return Err(invalid("truncated DNS resource record"));
    }
    let length = usize::from(read_u16(message, name_end + 8)?);
    let data_start = name_end + 10;
    let data_end = data_start
        .checked_add(length)
        .ok_or_else(|| invalid("DNS resource data length overflow"))?;
    if data_end > message.len() {
        return Err(invalid("truncated DNS resource data"));
    }
    Ok(RecordSpan {
        next: data_end,
        record_type: read_u16(message, name_end)?,
        type_offset: name_end,
        data_start,
        data_end,
    })
}

fn filter_address_record(message: &mut [u8], record: RecordSpan) {
    message[record.type_offset..record.type_offset + 2]
        .copy_from_slice(&FILTERED_RECORD_TYPE.to_be_bytes());
    message[record.data_start..record.data_end].fill(0);
}

fn read_u16(data: &[u8], offset: usize) -> io::Result<u16> {
    let bytes = data
        .get(offset..offset + 2)
        .ok_or_else(|| invalid("truncated DNS integer"))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
