//! Validate/count without allocating container DTOs. Arrays are streamed.
use super::*;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
#[derive(Clone, Copy, Default)]
pub(in crate::ipc::client) struct Layout {
    pub(in crate::ipc::client) maps: usize,
    pub(in crate::ipc::client) depth: usize,
    pub(in crate::ipc::client) escaped: usize,
}
impl Layout {
    pub(in crate::ipc::client) fn scratch(self) -> io::Result<usize> {
        self.escaped
            .max(self.depth)
            .checked_mul(2)
            .map(|n| if n == 0 { 0 } else { n.max(8) })
            .ok_or_else(capacity)
    }
    pub(in crate::ipc::client) fn workspace(self, bytes: usize) -> io::Result<usize> {
        let rows = self
            .maps
            .checked_mul(std::mem::size_of::<super::format::Entry<'_>>())
            .ok_or_else(capacity)?;
        // Nested RawValue decoding retains an ignored-value stack per active
        // parser. All map-key Strings and one scalar String fit in wire bytes.
        rows.checked_add(bytes)
            .and_then(|n| n.checked_add(self.escaped.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.depth.checked_mul(self.depth)?.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.depth.checked_mul(8)?))
            .ok_or_else(capacity)
    }
}
pub(in crate::ipc::client) fn scan(bytes: &[u8]) -> io::Result<Layout> {
    let (
        mut quote,
        mut escape,
        mut depth,
        mut high,
        mut span,
        mut escaped,
        mut start,
        mut has_escape,
    ) = (false, false, 0usize, 0, 0, 0, 0, false);
    for (at, byte) in bytes.iter().enumerate() {
        if quote {
            // The original decoder can grow scratch before reporting EOF.
            // Keep the escaped prefix even when no closing quote arrives.
            if has_escape {
                span = span.max(at - start);
                escaped = escaped.max(span);
            }
            if escape {
                escape = false;
                continue;
            }
            match byte {
                b'\\' => {
                    escape = true;
                    has_escape = true;
                }
                b'"' => {
                    quote = false;
                    if has_escape {
                        span = span.max(at - start);
                    }
                }
                _ => {}
            }
        } else {
            match byte {
                b'"' => {
                    quote = true;
                    start = at;
                    has_escape = false;
                }
                b'{' | b'[' => {
                    depth = depth.checked_add(1).ok_or_else(capacity)?;
                    high = high.max(depth);
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        escaped = escaped.max(span);
    }
    Ok(Layout {
        maps: 0,
        depth: high,
        escaped,
    })
}
pub(in crate::ipc::client) fn layout(
    bytes: &[u8],
    outer: usize,
) -> Result<Layout, serde_json::Error> {
    let mut parser = serde_json::Deserializer::from_slice(bytes);
    let maps = Count(outer).deserialize(&mut parser)?;
    parser.end()?;
    let mut facts = scan(bytes).map_err(serde::de::Error::custom)?;
    facts.maps = maps;
    Ok(facts)
}
struct Count(usize);
impl<'de> DeserializeSeed<'de> for Count {
    type Value = usize;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<usize, D::Error> {
        if self.0 >= 128 {
            return Err(serde::de::Error::custom("recursion limit exceeded"));
        }
        d.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Count {
    type Value = usize;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON value")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<usize, E> {
        Ok(0)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<usize, A::Error> {
        let mut peak = 0;
        while let Some(child) = a.next_element_seed(Count(self.0 + 1))? {
            peak = peak.max(child);
        }
        Ok(peak)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<usize, A::Error> {
        let (mut count, mut peak) = (0usize, 0usize);
        while a.next_key::<IgnoredAny>()?.is_some() {
            count = count
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom("object count overflow"))?;
            peak = peak.max(a.next_value_seed(Count(self.0 + 1))?);
        }
        let rows = if count == 0 {
            0
        } else {
            count
                .checked_next_power_of_two()
                .ok_or_else(|| serde::de::Error::custom("object capacity overflow"))?
                .max(4)
        };
        rows.checked_add(peak)
            .ok_or_else(|| serde::de::Error::custom("object capacity overflow"))
    }
}
