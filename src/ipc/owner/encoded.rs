//! The actual single encoded envelope, prepaid before its buffer allocation.
use super::*;
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
pub struct EncodedResponse {
    bytes: Vec<u8>,
    _storage: ControlBudgetPermit,
}
impl EncodedResponse {
    pub(crate) fn new(bytes: Vec<u8>, storage: ControlBudgetPermit) -> Self {
        Self {
            bytes,
            _storage: storage,
        }
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
/// The caller declares `storage` before its borrowed Engine view. An encode
/// error/panic therefore releases the Engine guards before this permit can
/// refund/wake. Success moves bytes/credit into the actual response owner.
pub(crate) fn encode_response(
    value: &impl serde::Serialize,
    budget: &Arc<ControlBudget>,
    storage: &mut Option<ControlBudgetPermit>,
    pretty: bool,
    newline: bool,
) -> io::Result<Vec<u8>> {
    let mut count = Count {
        bytes: 0,
        limit: budget.limits().messages.response_bytes,
        exceeded: false,
    };
    let measured = write_json(&mut count, value, pretty).and_then(|()| {
        if newline {
            std::io::Write::write_all(&mut count, b"\n")
        } else {
            Ok(())
        }
    });
    if count.exceeded {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            EnvelopeLimit(measured.expect_err("counter rejected oversized envelope")),
        ));
    }
    measured?;
    if storage.is_some() {
        return Err(io::Error::other("response storage already reserved"));
    }
    *storage = Some(budget.reserve(ControlCharge::ResponseBytes(count.bytes))?);
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count.bytes)
        .map_err(io::Error::other)?;
    write_json(&mut bytes, value, pretty)?;
    if newline {
        bytes.push(b'\n');
    }
    Ok(bytes)
}
fn write_json(
    out: &mut impl std::io::Write,
    value: &impl serde::Serialize,
    pretty: bool,
) -> io::Result<()> {
    if pretty {
        serde_json::to_writer_pretty(out, value)
    } else {
        serde_json::to_writer(out, value)
    }
    .map_err(io::Error::other)
}
#[derive(Debug)]
struct EnvelopeLimit(io::Error);
impl std::fmt::Display for EnvelopeLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("control response envelope exceeds configured limit")
    }
}
impl std::error::Error for EnvelopeLimit {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
struct Count {
    bytes: usize,
    limit: usize,
    exceeded: bool,
}
impl std::io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let total = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("response length overflow"))?;
        if total > self.limit {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control response envelope exceeds configured limit",
            ));
        }
        self.bytes = total;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
