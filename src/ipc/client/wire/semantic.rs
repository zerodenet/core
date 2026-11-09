//! Preserve serde_json::Value's actual raw_value magic interpretation.
use super::*;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use zero_runtime::configuration::{
    control_json_decoder_workspace, control_json_response_storage_bound,
    control_json_storage_bound, ControlJsonStorage,
};
#[derive(Clone, Copy)]
pub(super) enum Memory {
    Request,
    Response,
}
impl Memory {
    pub(super) fn reserve(
        self,
        book: &Arc<ControlBudget>,
        bytes: usize,
    ) -> io::Result<ControlBudgetPermit> {
        book.reserve(match self {
            Self::Request => ControlCharge::RequestBytes(bytes),
            Self::Response => ControlCharge::ResponseBytes(bytes),
        })
    }
    fn bound(self, raw: &RawValue, book: &Arc<ControlBudget>) -> io::Result<usize> {
        match self {
            Self::Request => {
                control_json_storage_bound(raw.get().as_bytes(), book, ControlJsonStorage::Value)
            }
            Self::Response => control_json_response_storage_bound(
                raw.get().as_bytes(),
                book,
                ControlJsonStorage::Value,
            ),
        }
    }
}
const MAGIC: &str = "$serde_json::private::RawValue";
struct Key;
impl<'de> DeserializeSeed<'de> for Key {
    type Value = bool;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        struct Name;
        impl<'de> Visitor<'de> for Name {
            type Value = bool;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON object key")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<bool, E> {
                Ok(s == MAGIC)
            }
        }
        d.deserialize_str(Name)
    }
}
pub(super) fn is_magic(raw: &RawValue) -> Result<bool, serde_json::Error> {
    struct First;
    impl<'de> Visitor<'de> for First {
        type Value = bool;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("JSON object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<bool, A::Error> {
            let Some(first) = a.next_key_seed(Key)? else {
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
    serde::Deserializer::deserialize_map(&mut serde_json::Deserializer::from_str(raw.get()), First)
}
pub(super) fn value(
    raw: &RawValue,
    book: &Arc<ControlBudget>,
) -> io::Result<Guard<serde_json::Value>> {
    value_for(raw, book, Memory::Response)
}
fn value_for(
    raw: &RawValue,
    book: &Arc<ControlBudget>,
    memory: Memory,
) -> io::Result<Guard<serde_json::Value>> {
    let credit = memory.reserve(book, working_value_bytes(raw, book, memory)?)?;
    let value = match serde_json::from_str(raw.get()) {
        Ok(value) => value,
        Err(error) => return Err(workspace_error(error, credit)),
    };
    Ok(Guard {
        value,
        _credit: credit,
    })
}

/// Check every actual Value subtree before a writer can emit a prefix. Only
/// the exact magic branch materializes the original serde Value, prepaid.
pub(super) fn validate(raw: &RawValue, book: &Arc<ControlBudget>) -> io::Result<()> {
    if is_magic(raw).map_err(io::Error::other)? {
        drop(value(raw, book)?);
        return Ok(());
    }
    let mut failure = None;
    let mut parser = serde_json::Deserializer::from_str(raw.get());
    let result = match raw.get().as_bytes().first() {
        Some(b'{') => serde::Deserializer::deserialize_map(
            &mut parser,
            Walk {
                book,
                failure: &mut failure,
            },
        ),
        Some(b'[') => serde::Deserializer::deserialize_seq(
            &mut parser,
            Walk {
                book,
                failure: &mut failure,
            },
        ),
        _ => return Ok(()),
    };
    if let Some(error) = failure {
        return Err(error);
    }
    result.map_err(io::Error::other)
}
struct Walk<'a> {
    book: &'a Arc<ControlBudget>,
    failure: &'a mut Option<io::Error>,
}
impl<'de> Visitor<'de> for Walk<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON container")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<(), A::Error> {
        while a.next_key::<serde::de::IgnoredAny>()?.is_some() {
            let raw = a.next_value::<&RawValue>()?;
            if let Err(error) = validate(raw, self.book) {
                *self.failure = Some(error);
                return Err(serde::de::Error::custom("actual Value subtree rejected"));
            }
        }
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<(), A::Error> {
        while let Some(raw) = a.next_element::<&RawValue>()? {
            if let Err(error) = validate(raw, self.book) {
                *self.failure = Some(error);
                return Err(serde::de::Error::custom("actual Value subtree rejected"));
            }
        }
        Ok(())
    }
}

/// Presence (not activation) selects the exact old typed Value/from_value path
/// for the two small CLI projections. Sorting may expose a later magic key on
/// that second original pass; the original API decoder decides, not this scan.
pub(super) fn has_marker(raw: &RawValue) -> Result<bool, serde_json::Error> {
    struct Search;
    impl<'de> Visitor<'de> for Search {
        type Value = bool;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("JSON container")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<bool, A::Error> {
            let mut found = false;
            while let Some(key) = a.next_key_seed(Key)? {
                let child = a.next_value::<&RawValue>()?;
                found = found || key || has_marker(child).map_err(serde::de::Error::custom)?;
            }
            Ok(found)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<bool, A::Error> {
            let mut found = false;
            while let Some(child) = a.next_element::<&RawValue>()? {
                found = found || has_marker(child).map_err(serde::de::Error::custom)?;
            }
            Ok(found)
        }
    }
    let mut parser = serde_json::Deserializer::from_str(raw.get());
    match raw.get().as_bytes().first() {
        Some(b'{') => serde::Deserializer::deserialize_map(&mut parser, Search),
        Some(b'[') => serde::Deserializer::deserialize_seq(&mut parser, Search),
        _ => Ok(false),
    }
}

fn working_value_bytes(
    raw: &RawValue,
    book: &Arc<ControlBudget>,
    memory: Memory,
) -> io::Result<usize> {
    memory
        .bound(raw, book)?
        .checked_add(control_json_decoder_workspace(raw.get().as_bytes())?)
        .and_then(|n| n.checked_add(raw.get().len()))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(capacity)
}
/// One concrete formatter working frame: validate before output and reserve the
/// largest actual magic subtree once. Plain tree nodes do not acquire Value.
pub(super) fn working_bytes(
    raw: &RawValue,
    book: &Arc<ControlBudget>,
    memory: Memory,
) -> io::Result<usize> {
    if is_magic(raw).map_err(io::Error::other)? {
        let bytes = working_value_bytes(raw, book, memory)?;
        drop(value_for(raw, book, memory)?);
        return Ok(bytes);
    }
    let mut failure = None;
    let mut parser = serde_json::Deserializer::from_str(raw.get());
    let result = match raw.get().as_bytes().first() {
        Some(b'{') => serde::Deserializer::deserialize_map(
            &mut parser,
            Working {
                book,
                memory,
                failure: &mut failure,
            },
        ),
        Some(b'[') => serde::Deserializer::deserialize_seq(
            &mut parser,
            Working {
                book,
                memory,
                failure: &mut failure,
            },
        ),
        _ => return Ok(0),
    };
    if let Some(error) = failure {
        return Err(error);
    }
    result.map_err(io::Error::other)
}
struct Working<'a> {
    book: &'a Arc<ControlBudget>,
    memory: Memory,
    failure: &'a mut Option<io::Error>,
}
impl Working<'_> {
    fn child<E: serde::de::Error>(&mut self, raw: &RawValue) -> Result<usize, E> {
        match working_bytes(raw, self.book, self.memory) {
            Ok(bytes) => Ok(bytes),
            Err(error) => {
                *self.failure = Some(error);
                Err(E::custom("actual Value working frame rejected"))
            }
        }
    }
}
impl<'de> Visitor<'de> for Working<'_> {
    type Value = usize;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON container")
    }
    fn visit_map<A: MapAccess<'de>>(mut self, mut a: A) -> Result<usize, A::Error> {
        let mut peak = 0;
        while a.next_key::<serde::de::IgnoredAny>()?.is_some() {
            let raw = a.next_value::<&RawValue>()?;
            peak = peak.max(self.child(raw)?);
        }
        Ok(peak)
    }
    fn visit_seq<A: SeqAccess<'de>>(mut self, mut a: A) -> Result<usize, A::Error> {
        let mut peak = 0;
        while let Some(raw) = a.next_element::<&RawValue>()? {
            peak = peak.max(self.child(raw)?);
        }
        Ok(peak)
    }
}
