//! Borrowed SVCB spans and one prepaid same-length rewrite workspace.
use super::*;
// Only the two address hints can be replaced. All other parameters are
// borrowed spans over this original response; no copied parameter tree/set.
#[derive(Clone, Copy)]
struct Param {
    key: u16,
    start: usize,
    value: usize,
    end: usize,
}
struct Params<'a> {
    message: &'a [u8],
    offset: usize,
    end: usize,
    previous: Option<u16>,
}
impl<'a> Params<'a> {
    fn new(message: &'a [u8], offset: usize, end: usize) -> Self {
        Self {
            message,
            offset,
            end,
            previous: None,
        }
    }
    fn next(&mut self) -> io::Result<Option<Param>> {
        if self.offset == self.end {
            return Ok(None);
        }
        if self.offset + 4 > self.end {
            return Err(invalid("truncated SVCB parameter"));
        }
        let key = read_u16(self.message, self.offset)?;
        let length = usize::from(read_u16(self.message, self.offset + 2)?);
        let value = self.offset + 4;
        let end = value
            .checked_add(length)
            .ok_or_else(|| invalid("SVCB parameter length overflow"))?;
        if end > self.end {
            return Err(invalid("truncated SVCB parameter value"));
        }
        if self.previous.is_some_and(|previous| previous >= key) {
            return Err(invalid("SVCB parameters are not strictly ordered"));
        }
        if (key == SVC_PARAM_IPV4_HINT && (length == 0 || length % 4 != 0))
            || (key == SVC_PARAM_IPV6_HINT && (length == 0 || length % 16 != 0))
        {
            return Err(invalid("SVCB address hint length is invalid"));
        }
        let param = Param {
            key,
            start: self.offset,
            value,
            end,
        };
        self.offset = end;
        self.previous = Some(key);
        Ok(Some(param))
    }
}
pub(super) fn rewrite_service_binding(
    message: &mut [u8],
    record: RecordSpan,
    policy: ResponseAddressPolicy,
    memory: Option<&crate::DnsWireMemory>,
) -> io::Result<()> {
    if record.data_start + 3 > record.data_end {
        return Err(invalid("truncated SVCB resource data"));
    }
    let target_end = skip_name(message, record.data_start + 2)?;
    if target_end > record.data_end {
        return Err(invalid("SVCB target name exceeds resource data"));
    }
    let removes = |key| {
        (key == SVC_PARAM_IPV4_HINT && policy.strips_ipv4())
            || (key == SVC_PARAM_IPV6_HINT && policy.strips_ipv6())
    };
    let mut removed = [None; 2];
    let mut count = 0;
    let mut mandatory = None;
    let mut used = [false; (PRIVATE_USE_END - PRIVATE_USE_START + 1) as usize];
    let mut params = Params::new(message, target_end, record.data_end);
    while let Some(param) = params.next()? {
        if param.key == SVC_PARAM_MANDATORY {
            mandatory = Some(param);
        }
        if removes(param.key) {
            removed[count] = Some(param);
            count += 1;
        }
        if (PRIVATE_USE_START..=PRIVATE_USE_END).contains(&param.key) {
            used[usize::from(param.key - PRIVATE_USE_START)] = true;
        }
    }
    let mut required_removed = false;
    if let Some(mandatory) = mandatory {
        let values = &message[mandatory.value..mandatory.end];
        if values.is_empty() || values.len() % 2 != 0 {
            return Err(invalid("SVCB mandatory parameter is malformed"));
        }
        let mut previous = None;
        let mut available = Params::new(message, target_end, record.data_end);
        for bytes in values.as_chunks::<2>().0 {
            let key = u16::from_be_bytes(*bytes);
            if key == SVC_PARAM_MANDATORY || previous.is_some_and(|previous| previous >= key) {
                return Err(invalid("SVCB mandatory keys are not strictly ordered"));
            }
            loop {
                match available.next()? {
                    Some(param) if param.key < key => continue,
                    Some(param) if param.key == key => break,
                    _ => return Err(invalid("SVCB mandatory key has no matching parameter")),
                }
            }
            required_removed |= removes(key);
            previous = Some(key);
        }
    }
    if count == 0 {
        return Ok(());
    }
    let mut replacements = [None; 2];
    let mut padding = (PRIVATE_USE_START..=PRIVATE_USE_END)
        .rev()
        .filter(|key| !used[usize::from(*key - PRIVATE_USE_START)]);
    for index in 0..count {
        let Some(key) = padding.next() else {
            filter_service_record(message, record, &removed);
            return Ok(());
        };
        replacements[index] = Some((key, removed[index].unwrap()));
    }
    if required_removed {
        filter_service_record(message, record, &removed);
        return Ok(());
    }
    if count == 2 && replacements[0].unwrap().0 > replacements[1].unwrap().0 {
        replacements.swap(0, 1);
    }
    // Same-length output keeps all DNS compression offsets unchanged. This
    // single exact scratch allocation is prepaid beside the original backing.
    let mut output =
        crate::wire_memory::WireBuffer::new(record.data_end - record.data_start, memory)?;
    output.extend(&message[record.data_start..target_end])?;
    let mut pending = 0;
    let mut params = Params::new(message, target_end, record.data_end);
    while let Some(param) = params.next()? {
        while pending < count && replacements[pending].unwrap().0 < param.key {
            append_padding(&mut output, replacements[pending].unwrap())?;
            pending += 1;
        }
        if !removes(param.key) {
            output.extend(&message[param.start..param.end])?;
        }
    }
    while pending < count {
        append_padding(&mut output, replacements[pending].unwrap())?;
        pending += 1;
    }
    if output.len() != record.data_end - record.data_start {
        return Err(invalid("SVCB policy rewrite changed resource length"));
    }
    message[record.data_start..record.data_end].copy_from_slice(&output);
    Ok(())
}
fn append_padding(
    output: &mut crate::wire_memory::WireBuffer,
    (key, param): (u16, Param),
) -> io::Result<()> {
    let length = param.end - param.value;
    output.extend(&key.to_be_bytes())?;
    output.extend(&(length as u16).to_be_bytes())?;
    output.extend_zeroed(length)
}
fn filter_service_record(message: &mut [u8], record: RecordSpan, removed: &[Option<Param>; 2]) {
    message[record.type_offset..record.type_offset + 2]
        .copy_from_slice(&FILTERED_RECORD_TYPE.to_be_bytes());
    for param in removed.iter().flatten() {
        message[param.value..param.end].fill(0);
    }
}
