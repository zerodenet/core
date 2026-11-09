//! Borrow the original Value-shaped envelope; only log/magic creates Value.
use super::super::super::control_budget::{control_json_typed_array_bound, initial_storage};
use super::*;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{value::RawValue, Value};
mod walk;
const MAGIC: &str = "$serde_json::private::RawValue";
fn invalid(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, source)
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "initial logging projection envelope exhausted",
    )
}
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(capacity)
}
fn twice(a: usize) -> io::Result<usize> {
    a.checked_mul(2).ok_or_else(capacity)
}

pub(super) fn decode(raw: &str, maximum: usize) -> io::Result<Option<zero_config::LogConfig>> {
    let workspace = walk::workspace(raw.as_bytes())?;
    let available = maximum.checked_sub(workspace).ok_or_else(capacity)?;
    let original: &RawValue = serde_json::from_str(raw).map_err(invalid)?;
    // The old whole Value visits every subtree, including ones not selected
    // for logging. Keep its actual magic/numeric/error behavior without a tree.
    walk::validate(original, available, 0)?;
    if magic(original)? {
        let (value, held) = materialize(original, available)?;
        return finish(take_log(value, true), held, available);
    }
    let Some(runtime) = field(original, "runtime")? else {
        return Ok(None);
    };
    if magic(runtime)? {
        let (value, held) = materialize(runtime, available)?;
        return finish(take_log(value, false), held, available);
    }
    let Some(log) = field(runtime, "log")? else {
        return Ok(None);
    };
    let (value, held) = materialize(log, available)?;
    finish(Some(value), held, available)
}
fn take_log(value: Value, root: bool) -> Option<Value> {
    let Value::Object(mut map) = value else {
        return None;
    };
    if root {
        let Value::Object(mut runtime) = map.remove("runtime")? else {
            return None;
        };
        runtime.remove("log")
    } else {
        map.remove("log")
    }
}
fn finish(
    value: Option<Value>,
    held: usize,
    maximum: usize,
) -> io::Result<Option<zero_config::LogConfig>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let rows = value
        .get("files")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    // serde's owned Value deserializer moves strings; its original array may
    // coexist with the typed files allocation. Pay that exact row envelope
    // before from_value; no whole-config clone or content-tree conversion.
    let typed =
        control_json_typed_array_bound(rows, std::mem::size_of::<zero_config::LogFileConfig>())?;
    if add(held, typed)? > maximum {
        return Err(capacity());
    }
    serde_json::from_value(value).map(Some).map_err(invalid)
}
fn materialize(raw: &RawValue, maximum: usize) -> io::Result<(Value, usize)> {
    let held = initial_storage(raw.get().as_bytes(), maximum)?;
    let value = serde_json::from_str(raw.get()).map_err(invalid)?;
    Ok((value, held))
}
struct Key<'a>(&'a str);
impl<'de> DeserializeSeed<'de> for Key<'_> {
    type Value = bool;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        struct Name<'a>(&'a str);
        impl<'de> Visitor<'de> for Name<'_> {
            type Value = bool;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("object key")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<bool, E> {
                Ok(s == self.0)
            }
        }
        d.deserialize_str(Name(self.0))
    }
}
fn magic(raw: &RawValue) -> io::Result<bool> {
    struct Entry;
    impl<'de> Visitor<'de> for Entry {
        type Value = bool;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<bool, A::Error> {
            let Some(first) = a.next_key_seed(Key(MAGIC))? else {
                return Ok(false);
            };
            let _ = a.next_value::<&RawValue>()?;
            while a.next_key::<serde::de::IgnoredAny>()?.is_some() {
                let _ = a.next_value::<&RawValue>()?;
            }
            Ok(first)
        }
    }
    if !raw.get().starts_with('{') {
        return Ok(false);
    }
    serde::Deserializer::deserialize_map(&mut serde_json::Deserializer::from_str(raw.get()), Entry)
        .map_err(invalid)
}
fn field<'de>(raw: &'de RawValue, name: &str) -> io::Result<Option<&'de RawValue>> {
    struct Field<'a>(&'a str);
    impl<'de> Visitor<'de> for Field<'_> {
        type Value = Option<&'de RawValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            let mut selected = None;
            while let Some(matches) = a.next_key_seed(Key(self.0))? {
                let child = a.next_value::<&RawValue>()?;
                if matches {
                    selected = Some(child);
                }
            }
            Ok(selected)
        }
    }
    if !raw.get().starts_with('{') {
        return Ok(None);
    }
    serde::Deserializer::deserialize_map(
        &mut serde_json::Deserializer::from_str(raw.get()),
        Field(name),
    )
    .map_err(invalid)
}
