//! Conservative capacity envelopes, not process RSS or a new allocator.
//! Rust 1.99.0 alloc/btree/node.rs: B=6, eleven key/value positions,
//! twelve edges, non-root occupancy >=5. The fixed word envelope includes
//! header/alignment and edges; it deliberately exceeds a leaf allocation.
use super::*;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{io, sync::Arc};
const RAW: &str = "$serde_json::private::RawValue";
mod layout;
use layout::{add, array};
pub(super) use layout::{map, scratch, typed_array};
struct Measure<'a> {
    book: &'a Arc<ControlBudget>,
    bytes: usize,
    failure: Option<io::Error>,
    mode: ControlJsonStorage,
    response: bool,
}
impl Measure<'_> {
    fn grow<E: serde::de::Error>(&mut self, n: usize) -> Result<(), E> {
        match add(self.bytes, n) {
            Ok(n) => {
                self.bytes = n;
                Ok(())
            }
            Err(error) => {
                self.failure = Some(error);
                Err(E::custom("control decoded capacity overflow"))
            }
        }
    }
    fn cost<E: serde::de::Error>(&mut self, cost: io::Result<usize>) -> Result<usize, E> {
        match cost {
            Ok(n) => Ok(n),
            Err(error) => {
                self.failure = Some(error);
                Err(E::custom("control storage overflow"))
            }
        }
    }
    fn text(&mut self, bytes: &[u8]) {
        let work = scratch(bytes)
            .and_then(|n| add(n, bytes.len()))
            .and_then(|n| add(n, 4096))
            .and_then(|n| {
                self.book.reserve(if self.response {
                    ControlCharge::ResponseBytes(n)
                } else {
                    ControlCharge::RequestBytes(n)
                })
            });
        let _workspace = match work {
            Ok(value) => value,
            Err(error) => {
                self.failure = Some(error);
                return;
            }
        };
        let mut parser = serde_json::Deserializer::from_slice(bytes);
        // The real decoder still decides result/error. Keep a partial-prefix
        // storage envelope even for malformed embedded magic-token JSON.
        let _ = serde::Deserializer::deserialize_any(&mut parser, Node(self));
    }
}
struct Node<'a, 'b>(&'a mut Measure<'b>);
impl<'de> DeserializeSeed<'de> for Node<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Node<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON value")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<(), E> {
        if matches!(self.0.mode, ControlJsonStorage::TypedRows { .. }) {
            Ok(())
        } else {
            self.0.grow(s.len())
        }
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut count = 0;
        let mut previous = 0;
        while seq.next_element_seed(Node(self.0))?.is_some() {
            count = self.0.cost(add(count, 1))?;
            let cost = match self.0.mode {
                ControlJsonStorage::Value => array(count, std::mem::size_of::<serde_json::Value>()),
                ControlJsonStorage::Content => typed_array(count, content_cell()),
                ControlJsonStorage::TypedRows { element_size } => typed_array(count, element_size),
            };
            let next = self.0.cost(cost)?;
            self.0.grow(next - previous)?;
            previous = next;
        }
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map_access: A) -> Result<(), A::Error> {
        let mut count = 0;
        let mut previous = 0;
        while let Some(magic) = map_access.next_key_seed(Key(self.0))? {
            count = self.0.cost(add(count, 1))?;
            let cost = match self.0.mode {
                ControlJsonStorage::Value => map(count),
                ControlJsonStorage::Content => typed_array(count, content_cell() * 2),
                ControlJsonStorage::TypedRows { .. } => Ok(0),
            };
            let next = self.0.cost(cost)?;
            self.0.grow(next - previous)?;
            previous = next;
            if magic && !matches!(self.0.mode, ControlJsonStorage::Content) {
                map_access.next_value_seed(Magic(self.0))?;
            } else {
                map_access.next_value_seed(Node(self.0))?;
            }
        }
        Ok(())
    }
}
struct Key<'a, 'b>(&'a mut Measure<'b>);
impl<'de> DeserializeSeed<'de> for Key<'_, '_> {
    type Value = bool;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        d.deserialize_str(self)
    }
}
impl<'de> Visitor<'de> for Key<'_, '_> {
    type Value = bool;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("object key")
    }
    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<bool, E> {
        if matches!(self.0.mode, ControlJsonStorage::TypedRows { .. }) {
            Ok(())
        } else {
            self.0.grow(s.len())
        }?;
        Ok(s == RAW)
    }
}
struct Magic<'a, 'b>(&'a mut Measure<'b>);
impl<'de> DeserializeSeed<'de> for Magic<'_, '_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Magic<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("possible raw JSON value")
    }
    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<(), E> {
        // Include both the actual boxed raw text and its possible recursively
        // decoded tree. This is measurement, never changed decode semantics.
        if matches!(self.0.mode, ControlJsonStorage::TypedRows { .. }) {
            Ok(())
        } else {
            self.0.grow(s.len())
        }?;
        // A recursively decoded raw value has its own live parser scratch.
        if !matches!(self.0.mode, ControlJsonStorage::TypedRows { .. }) {
            let work = self.0.cost(scratch(s.as_bytes()))?;
            self.0.grow(work)?;
        }
        self.0.text(s.as_bytes());
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, a: A) -> Result<(), A::Error> {
        Node(self.0).visit_map(a)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, a: A) -> Result<(), A::Error> {
        Node(self.0).visit_seq(a)
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        Ok(())
    }
}
pub fn measure(
    raw: &[u8],
    book: &Arc<ControlBudget>,
    mode: ControlJsonStorage,
    response: bool,
) -> io::Result<usize> {
    let mut m = Measure {
        book,
        bytes: 0,
        failure: None,
        mode,
        response,
    };
    m.text(raw);
    if let Some(error) = m.failure {
        return Err(error);
    }
    Ok(m.bytes)
}
fn content_cell() -> usize {
    std::mem::size_of::<(Vec<()>, u64, u8)>()
}
