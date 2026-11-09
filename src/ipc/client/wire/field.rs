//! Small CLI projections borrow exact fields and stream array lengths.
use super::*;
use serde::de::{MapAccess, SeqAccess, Visitor};
fn member<'a>(raw: &'a RawValue, key: &str) -> Result<Option<&'a RawValue>, serde_json::Error> {
    struct Field<'k>(&'k str);
    impl<'de> Visitor<'de> for Field<'_> {
        type Value = Option<&'de RawValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("JSON object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
            struct Key<'a>(&'a str);
            impl<'de> serde::de::DeserializeSeed<'de> for Key<'_> {
                type Value = bool;
                fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
                    impl<'de> Visitor<'de> for Key<'_> {
                        type Value = bool;
                        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                            f.write_str("key")
                        }
                        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<bool, E> {
                            Ok(s == self.0)
                        }
                    }
                    d.deserialize_str(self)
                }
            }
            let mut found = None;
            while let Some(matches) = a.next_key_seed(Key(self.0))? {
                let value = a.next_value::<&RawValue>()?;
                if matches {
                    found = Some(value);
                }
            }
            Ok(found)
        }
    }
    if !raw.get().starts_with('{') {
        return Ok(None);
    }
    serde::Deserializer::deserialize_map(
        &mut serde_json::Deserializer::from_str(raw.get()),
        Field(key),
    )
}
fn length(raw: &RawValue) -> Result<usize, serde_json::Error> {
    struct Length;
    impl<'de> Visitor<'de> for Length {
        type Value = usize;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("array")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<usize, A::Error> {
            let mut count = 0usize;
            while a.next_element::<serde::de::IgnoredAny>()?.is_some() {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| serde::de::Error::custom("array length overflow"))?;
            }
            Ok(count)
        }
    }
    if !raw.get().starts_with('[') {
        return Ok(0);
    }
    serde::Deserializer::deserialize_seq(&mut serde_json::Deserializer::from_str(raw.get()), Length)
}
impl Reply {
    pub(crate) fn print_status(&self, socket: &str, json: bool) -> io::Result<()> {
        let credit = self.book.reserve(ControlCharge::ResponseBytes(
            self.layout.workspace(self.frame.bytes.len())?,
        ))?;
        let header = self.header()?;
        if json {
            let raw = header.result;
            // None is encoded explicitly below without retaining a second DTO.
            let stdout = io::stdout();
            let mut output = stdout.lock();
            if let Some(raw) = raw {
                format::write(&mut output, raw, true)?;
            } else {
                io::Write::write_all(&mut output, b"null")?;
            }
            io::Write::write_all(&mut output, b"\n")?;
        } else {
            println!("Engine Status (via {socket})");
            if let Some(result) = header.result {
                if let Some(stats) = member(result, "stats").map_err(io::Error::other)? {
                    let scalar = |key| -> io::Result<u64> {
                        Ok(member(stats, key)
                            .map_err(io::Error::other)?
                            .and_then(|raw| serde_json::from_str::<u64>(raw.get()).ok())
                            .unwrap_or(0))
                    };
                    println!(
                        "  sessions: active={} total={} completed={} failed={}",
                        scalar("active_sessions")?,
                        scalar("total_started")?,
                        scalar("completed_sessions")?,
                        scalar("failed_sessions")?
                    );
                }
                let array = |key| -> io::Result<usize> {
                    match member(result, key).map_err(io::Error::other)? {
                        Some(raw) => length(raw).map_err(io::Error::other),
                        None => Ok(0),
                    }
                };
                println!("  active_flows: {}", array("active_sessions")?);
                println!(
                    "  recent_completed: {}",
                    array("recent_completed_sessions")?
                );
            } else {
                println!("  active_flows: 0");
                println!("  recent_completed: 0");
            }
        }
        drop(header);
        drop(credit);
        Ok(())
    }
}
pub(super) fn tun_array_rows(raw: &RawValue) -> Result<usize, serde_json::Error> {
    let status = raw;
    let mut total = 0usize;
    for key in ["addresses", "include_cidrs", "exclude_cidrs"] {
        if let Some(raw) = member(status, key)? {
            let count = length(raw)?;
            let capacity = if count == 0 {
                0
            } else {
                count
                    .checked_next_power_of_two()
                    .ok_or_else(|| {
                        <serde_json::Error as serde::de::Error>::custom("array capacity overflow")
                    })?
                    .max(4)
            };
            total = total.checked_add(capacity).ok_or_else(|| {
                <serde_json::Error as serde::de::Error>::custom("array capacity overflow")
            })?;
        }
    }
    Ok(total)
}
