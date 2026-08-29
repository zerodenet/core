use std::collections::BTreeSet;
use std::io;

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
pub(crate) fn apply_response_address_policy(
    query: &[u8],
    mut response: Vec<u8>,
    policy: ResponseAddressPolicy,
) -> io::Result<Vec<u8>> {
    parse_response(query, &response)?;
    if !policy.strips_ipv4() && !policy.strips_ipv6() {
        return Ok(response);
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
            TYPE_A if policy.strips_ipv4() => filter_address_record(&mut response, record),
            TYPE_AAAA if policy.strips_ipv6() => filter_address_record(&mut response, record),
            TYPE_SVCB | TYPE_HTTPS => rewrite_service_binding(&mut response, record, policy)?,
            _ => {}
        }
        offset = record.next;
    }
    if offset != response.len() {
        return Err(invalid("DNS response has trailing bytes"));
    }
    Ok(response)
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

#[derive(Clone)]
struct SvcParam {
    key: u16,
    value: Vec<u8>,
}

fn rewrite_service_binding(
    message: &mut [u8],
    record: RecordSpan,
    policy: ResponseAddressPolicy,
) -> io::Result<()> {
    if record.data_start + 3 > record.data_end {
        return Err(invalid("truncated SVCB resource data"));
    }
    let target_end = skip_name(message, record.data_start + 2)?;
    if target_end > record.data_end {
        return Err(invalid("SVCB target name exceeds resource data"));
    }

    let prefix = message[record.data_start..target_end].to_vec();
    let mut params = parse_svc_params(message, target_end, record.data_end)?;
    let mandatory = mandatory_keys(&params)?;
    let available_keys = params
        .iter()
        .map(|param| param.key)
        .collect::<BTreeSet<_>>();
    if mandatory.iter().any(|key| !available_keys.contains(key)) {
        return Err(invalid("SVCB mandatory key has no matching parameter"));
    }
    let removes = |key| {
        (key == SVC_PARAM_IPV4_HINT && policy.strips_ipv4())
            || (key == SVC_PARAM_IPV6_HINT && policy.strips_ipv6())
    };
    let removed_keys = params
        .iter()
        .filter(|param| removes(param.key))
        .map(|param| param.key)
        .collect::<BTreeSet<_>>();
    if mandatory.iter().any(|key| removes(*key)) {
        message[record.type_offset..record.type_offset + 2]
            .copy_from_slice(&FILTERED_RECORD_TYPE.to_be_bytes());
        zero_removed_param_values(message, target_end, &params, &removed_keys)?;
        return Ok(());
    }
    if removed_keys.is_empty() {
        return Ok(());
    }

    let used = params
        .iter()
        .map(|param| param.key)
        .collect::<BTreeSet<_>>();
    let padding_keys = (PRIVATE_USE_START..=PRIVATE_USE_END)
        .rev()
        .filter(|key| !used.contains(key))
        .take(removed_keys.len())
        .collect::<Vec<_>>();
    if padding_keys.len() != removed_keys.len() {
        message[record.type_offset..record.type_offset + 2]
            .copy_from_slice(&FILTERED_RECORD_TYPE.to_be_bytes());
        zero_removed_param_values(message, target_end, &params, &removed_keys)?;
        return Ok(());
    }
    let mut padding_keys = padding_keys.into_iter();
    for param in &mut params {
        if !removed_keys.contains(&param.key) {
            continue;
        }
        let padding_key = padding_keys
            .next()
            .expect("padding key count matches removed SVCB parameters");
        param.key = padding_key;
        param.value.fill(0);
    }
    params.sort_by_key(|param| param.key);

    let mut rewritten = prefix;
    for param in params {
        rewritten.extend_from_slice(&param.key.to_be_bytes());
        rewritten.extend_from_slice(&(param.value.len() as u16).to_be_bytes());
        rewritten.extend_from_slice(&param.value);
    }
    if rewritten.len() != record.data_end - record.data_start {
        return Err(invalid("SVCB policy rewrite changed resource length"));
    }
    message[record.data_start..record.data_end].copy_from_slice(&rewritten);
    Ok(())
}

fn parse_svc_params(message: &[u8], start: usize, end: usize) -> io::Result<Vec<SvcParam>> {
    let mut params = Vec::new();
    let mut offset = start;
    let mut previous = None;
    while offset < end {
        if offset + 4 > end {
            return Err(invalid("truncated SVCB parameter"));
        }
        let key = read_u16(message, offset)?;
        let length = usize::from(read_u16(message, offset + 2)?);
        let value_start = offset + 4;
        let value_end = value_start
            .checked_add(length)
            .ok_or_else(|| invalid("SVCB parameter length overflow"))?;
        if value_end > end {
            return Err(invalid("truncated SVCB parameter value"));
        }
        if previous.is_some_and(|previous| previous >= key) {
            return Err(invalid("SVCB parameters are not strictly ordered"));
        }
        if (key == SVC_PARAM_IPV4_HINT && (length == 0 || length % 4 != 0))
            || (key == SVC_PARAM_IPV6_HINT && (length == 0 || length % 16 != 0))
        {
            return Err(invalid("SVCB address hint length is invalid"));
        }
        params.push(SvcParam {
            key,
            value: message[value_start..value_end].to_vec(),
        });
        previous = Some(key);
        offset = value_end;
    }
    Ok(params)
}

fn mandatory_keys(params: &[SvcParam]) -> io::Result<BTreeSet<u16>> {
    let Some(mandatory) = params.iter().find(|param| param.key == SVC_PARAM_MANDATORY) else {
        return Ok(BTreeSet::new());
    };
    if mandatory.value.is_empty() || mandatory.value.len() % 2 != 0 {
        return Err(invalid("SVCB mandatory parameter is malformed"));
    }
    let mut keys = BTreeSet::new();
    let mut previous = None;
    for bytes in mandatory.value.as_chunks::<2>().0 {
        let key = u16::from_be_bytes([bytes[0], bytes[1]]);
        if key == SVC_PARAM_MANDATORY || previous.is_some_and(|previous| previous >= key) {
            return Err(invalid("SVCB mandatory keys are not strictly ordered"));
        }
        keys.insert(key);
        previous = Some(key);
    }
    Ok(keys)
}

fn zero_param_value(
    message: &mut [u8],
    start: usize,
    params: &[SvcParam],
    target_key: u16,
) -> io::Result<()> {
    let mut offset = start;
    for param in params {
        let value_start = offset + 4;
        let value_end = value_start + param.value.len();
        if param.key == target_key {
            message[value_start..value_end].fill(0);
            return Ok(());
        }
        offset = value_end;
    }
    Err(invalid("SVCB parameter disappeared during policy rewrite"))
}

fn zero_removed_param_values(
    message: &mut [u8],
    start: usize,
    params: &[SvcParam],
    removed_keys: &BTreeSet<u16>,
) -> io::Result<()> {
    for key in removed_keys {
        zero_param_value(message, start, params, *key)?;
    }
    Ok(())
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
