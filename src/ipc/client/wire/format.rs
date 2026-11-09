//! Value-compatible sorted formatting; only one object's paid index at a time.
use super::*;
use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Serialize,
};
use std::borrow::Cow;
pub(super) struct Entry<'a> {
    key: Cow<'a, str>,
    value: &'a RawValue,
    ordinal: usize,
}
struct BorrowedKey<'a>(Cow<'a, str>);
impl<'de> Deserialize<'de> for BorrowedKey<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Key;
        impl<'de> Visitor<'de> for Key {
            type Value = BorrowedKey<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON object key")
            }
            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                s: &'de str,
            ) -> Result<Self::Value, E> {
                Ok(BorrowedKey(Cow::Borrowed(s)))
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                Ok(BorrowedKey(Cow::Owned(s.to_owned())))
            }
        }
        d.deserialize_str(Key)
    }
}
struct Index;
impl<'de> Visitor<'de> for Index {
    type Value = Vec<Entry<'de>>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
        let mut rows = Vec::new();
        while let Some(BorrowedKey(key)) = a.next_key()? {
            if rows.len() == rows.capacity() {
                let target = if rows.capacity() == 0 {
                    4
                } else {
                    rows.capacity()
                        .checked_mul(2)
                        .ok_or_else(|| serde::de::Error::custom("object capacity overflow"))?
                };
                rows.try_reserve_exact(target - rows.len())
                    .map_err(serde::de::Error::custom)?;
            }
            let value = a.next_value()?;
            rows.push(Entry {
                key,
                value,
                ordinal: rows.len(),
            });
        }
        rows.sort_unstable_by(|a, b| a.key.cmp(&b.key).then(a.ordinal.cmp(&b.ordinal)));
        rows.dedup_by(|later, earlier| {
            if later.key == earlier.key {
                earlier.value = later.value;
                earlier.ordinal = later.ordinal;
                true
            } else {
                false
            }
        });
        Ok(rows)
    }
}
pub(super) struct Wire<'a> {
    raw: &'a RawValue,
    _magic: ControlBudgetPermit,
}
impl<'a> Wire<'a> {
    pub(super) fn new(raw: &'a RawValue, book: &Arc<ControlBudget>) -> io::Result<Self> {
        Self::for_memory(raw, book, semantic::Memory::Response)
    }
    pub(super) fn new_request(raw: &'a RawValue, book: &Arc<ControlBudget>) -> io::Result<Self> {
        Self::for_memory(raw, book, semantic::Memory::Request)
    }
    fn for_memory(
        raw: &'a RawValue,
        book: &Arc<ControlBudget>,
        memory: semantic::Memory,
    ) -> io::Result<Self> {
        let bytes = semantic::working_bytes(raw, book, memory)?;
        Ok(Self {
            raw,
            _magic: memory.reserve(book, bytes)?,
        })
    }
}
impl Serialize for Wire<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        Node(self.raw).serialize(s)
    }
}
struct Node<'a>(&'a RawValue);
impl Serialize for Node<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        match self.0.get().as_bytes().first() {
            Some(b'{') => {
                if semantic::is_magic(self.0).map_err(S::Error::custom)? {
                    // Wire::new validated and paid this exact immutable
                    // subtree before any prefix was written. No second lease.
                    let original: serde_json::Value =
                        serde_json::from_str(self.0.get()).map_err(S::Error::custom)?;
                    return original.serialize(s);
                }
                let rows = serde::Deserializer::deserialize_map(
                    &mut serde_json::Deserializer::from_str(self.0.get()),
                    Index,
                )
                .map_err(S::Error::custom)?;
                let mut map = s.serialize_map(Some(rows.len()))?;
                for row in &rows {
                    map.serialize_entry(row.key.as_ref(), &Node(row.value))?;
                }
                map.end()
            }
            Some(b'[') => {
                struct Array<S>(S);
                impl<'de, S: serde::Serializer> Visitor<'de> for Array<S> {
                    type Value = Result<S::Ok, S::Error>;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str("JSON array")
                    }
                    fn visit_seq<A: SeqAccess<'de>>(
                        self,
                        mut a: A,
                    ) -> Result<Self::Value, A::Error> {
                        let mut sequence = match self.0.serialize_seq(None) {
                            Ok(sequence) => sequence,
                            Err(error) => {
                                while a.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                                return Ok(Err(error));
                            }
                        };
                        while let Some(raw) = a.next_element::<&RawValue>()? {
                            if let Err(error) = sequence.serialize_element(&Node(raw)) {
                                while a.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                                return Ok(Err(error));
                            }
                        }
                        Ok(sequence.end())
                    }
                }
                serde::Deserializer::deserialize_seq(
                    &mut serde_json::Deserializer::from_str(self.0.get()),
                    Array(s),
                )
                .map_err(S::Error::custom)?
            }
            _ => {
                // Exactly one scalar; no container DTO fallback.
                let scalar: serde_json::Value =
                    serde_json::from_str(self.0.get()).map_err(S::Error::custom)?;
                scalar.serialize(s)
            }
        }
    }
}
pub(super) fn write(writer: &mut impl io::Write, wire: &Wire<'_>, pretty: bool) -> io::Result<()> {
    if pretty {
        serde_json::to_writer_pretty(writer, wire)
    } else {
        serde_json::to_writer(writer, wire)
    }
    .map_err(io::Error::other)
}

pub(super) fn tun_value(raw: &RawValue) -> io::Result<Option<&RawValue>> {
    if !raw.get().starts_with('{') {
        return Ok(None);
    }
    let rows = serde::Deserializer::deserialize_map(
        &mut serde_json::Deserializer::from_str(raw.get()),
        Index,
    )
    .map_err(io::Error::other)?;
    Ok(rows
        .first()
        .and_then(|row| (row.key == "tun_status").then_some(row.value)))
}
