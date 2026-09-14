use super::{
    decode_name, invalid, named_record, normalize_response_name, parse_question, parse_response,
    read_u16, skip_name, trusted_cname_terminal, ResourceRecord,
};
use std::io;

const SVC_PARAM_ECH: u16 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedEchConfig {
    pub(crate) config_list: Option<Vec<u8>>,
    pub(crate) ttl_seconds: u32,
}

pub(crate) fn parse_ech_config_response(
    query: &[u8],
    response: &[u8],
) -> io::Result<ParsedEchConfig> {
    let expected = parse_question(query)?;
    if expected.query_type != super::super::TYPE_HTTPS {
        return Err(invalid("ECH lookup must use an HTTPS DNS question"));
    }
    let parsed = parse_response(query, response)?;
    if parsed.response_code != super::super::RCODE_NOERROR {
        return Err(io::Error::other(format!(
            "ECH DNS server returned response code {}",
            parsed.response_code
        )));
    }
    let (_, question_name_end) = decode_name(response, 12)?;
    let answer_count = read_u16(response, 6)? as usize;
    let mut offset = question_name_end + 4;
    let mut answers = Vec::with_capacity(answer_count);
    for _ in 0..answer_count {
        let answer = named_record(response, offset)?;
        offset = answer.next;
        answers.push(answer);
    }
    let (terminal_name, cname_ttl) = trusted_cname_terminal(&expected.domain, response, &answers)?;
    for answer in &answers {
        if answer.class != 1
            || answer.record_type != super::super::TYPE_HTTPS
            || normalize_response_name(&answer.owner)? != terminal_name
        {
            continue;
        }
        let config_list = parse_https_ech_parameter(response, answer)?;
        if config_list.is_some() {
            return Ok(ParsedEchConfig {
                config_list,
                ttl_seconds: cname_ttl.map_or(answer.ttl, |ttl| ttl.min(answer.ttl)),
            });
        }
    }
    Ok(ParsedEchConfig {
        config_list: None,
        // The ECH reference caches a successful empty answer for five minutes.
        ttl_seconds: 300,
    })
}

#[cfg(test)]
mod tests;

fn parse_https_ech_parameter(
    message: &[u8],
    answer: &ResourceRecord<'_>,
) -> io::Result<Option<Vec<u8>>> {
    if answer.rdata.len() < 3 {
        return Err(invalid("HTTPS DNS record is truncated"));
    }
    let target_start = answer.rdata_start + 2;
    let target_end = skip_name(message, target_start)?;
    if target_end > answer.next {
        return Err(invalid("HTTPS DNS target name exceeds record data"));
    }
    let mut offset = target_end;
    let mut previous_key = None;
    let mut ech = None;
    while offset < answer.next {
        if offset + 4 > answer.next {
            return Err(invalid("HTTPS DNS service parameter is truncated"));
        }
        let key = read_u16(message, offset)?;
        let length = read_u16(message, offset + 2)? as usize;
        let start = offset + 4;
        let end = start
            .checked_add(length)
            .ok_or_else(|| invalid("HTTPS DNS service parameter length overflow"))?;
        if end > answer.next {
            return Err(invalid("HTTPS DNS service parameter exceeds record data"));
        }
        if previous_key.is_some_and(|previous| key <= previous) {
            return Err(invalid(
                "HTTPS DNS service parameters are duplicated or unsorted",
            ));
        }
        if key == SVC_PARAM_ECH {
            if length == 0 {
                return Err(invalid("HTTPS DNS ech parameter is empty"));
            }
            ech = Some(message[start..end].to_vec());
        }
        previous_key = Some(key);
        offset = end;
    }
    Ok(ech)
}
