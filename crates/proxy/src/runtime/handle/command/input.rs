//! One exact canonical JSON buffer, paid before allocation from the same Host.
//! AST/prepared snapshot storage is a separate owner and is not proven by this
//! temporary byte receipt or by its caller's completion.
use std::{
    io::{self, Write},
    sync::Arc,
};
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
pub(super) struct CanonicalJson {
    raw: String,
    _credit: ControlBudgetPermit,
}
impl AsRef<str> for CanonicalJson {
    fn as_ref(&self) -> &str {
        &self.raw
    }
}
struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::WouldBlock, "canonical JSON length overflow")
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Exact<'a> {
    bytes: &'a mut Vec<u8>,
    end: usize,
}
impl Write for Exact<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(data.len())
            .is_none_or(|n| n > self.end)
        {
            return Err(io::Error::other(
                "immutable configuration JSON changed during serialization",
            ));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn api(error: io::Error) -> zero_api::ApiError {
    let code = if error.kind() == io::ErrorKind::WouldBlock {
        zero_api::ApiErrorCode::Conflict
    } else {
        zero_api::ApiErrorCode::Internal
    };
    zero_api::ApiError::new(code, error.to_string())
}
pub(super) fn serialize(
    value: &serde_json::Value,
    book: &Arc<ControlBudget>,
) -> zero_api::ApiResult<CanonicalJson> {
    // Value is immutable and concrete: counting invokes no external callback.
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_err(|e| api(io::Error::other(e)))?;
    let credit = book
        .reserve(ControlCharge::RequestBytes(count.0))
        .map_err(api)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count.0)
        .map_err(|e| api(io::Error::other(e)))?;
    serde_json::to_writer(
        Exact {
            bytes: &mut bytes,
            end: count.0,
        },
        value,
    )
    .map_err(|e| api(io::Error::other(e)))?;
    let raw = String::from_utf8(bytes).map_err(|e| api(io::Error::other(e)))?;
    Ok(CanonicalJson {
        raw,
        _credit: credit,
    })
}
#[cfg(test)]
#[path = "tests/input.rs"]
mod tests;
