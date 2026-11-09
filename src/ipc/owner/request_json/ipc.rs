//! Borrowed fixed envelope replaces the internally-tagged Content tree.
use super::*;
use crate::ipc::protocol::IpcRequest;
enum Kind {
    Ping,
    Query,
    Command,
    Subscribe,
}
impl<'de> Deserialize<'de> for Kind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Tag;
        impl<'de> serde::de::Visitor<'de> for Tag {
            type Value = Kind;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("IPC type identifier")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Kind, E> {
                match s {
                    "ping" => Ok(Kind::Ping),
                    "query" => Ok(Kind::Query),
                    "command" => Ok(Kind::Command),
                    "subscribe" => Ok(Kind::Subscribe),
                    _ => Err(E::custom("unknown IPC type")),
                }
            }
        }
        d.deserialize_identifier(Tag)
    }
}
#[derive(Default)]
struct Envelope<'a> {
    kind: Option<&'a RawValue>,
    id: Option<&'a RawValue>,
    request: Option<&'a RawValue>,
    method: Option<&'a RawValue>,
    params: Option<&'a RawValue>,
    events: Option<&'a RawValue>,
    seen: u8,
    duplicates: u8,
}
#[derive(Clone, Copy)]
enum Key {
    Type = 0,
    Id,
    Request,
    Method,
    Params,
    Events,
    Other,
}
impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Field;
        impl<'de> serde::de::Visitor<'de> for Field {
            type Value = Key;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("IPC envelope key")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Key, E> {
                Ok(match s {
                    "type" => Key::Type,
                    "id" => Key::Id,
                    "request" => Key::Request,
                    "method" => Key::Method,
                    "params" => Key::Params,
                    "events" => Key::Events,
                    _ => Key::Other,
                })
            }
        }
        d.deserialize_identifier(Field)
    }
}
impl<'de> Deserialize<'de> for Envelope<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> serde::de::Visitor<'de> for Fields {
            type Value = Envelope<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("IPC object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut result = Envelope::default();
                while let Some(key) = map.next_key::<Key>()? {
                    let raw = map.next_value::<&RawValue>()?;
                    if matches!(key, Key::Other) {
                        continue;
                    }
                    let bit = 1u8 << (key as u8);
                    if result.seen & bit != 0 {
                        result.duplicates |= bit;
                    }
                    result.seen |= bit;
                    match key {
                        Key::Type => result.kind = Some(raw),
                        Key::Id => result.id = Some(raw),
                        Key::Request => result.request = Some(raw),
                        Key::Method => result.method = Some(raw),
                        Key::Params => result.params = Some(raw),
                        Key::Events => result.events = Some(raw),
                        Key::Other => {}
                    }
                }
                Ok(result)
            }
        }
        d.deserialize_map(Fields)
    }
}
impl Envelope<'_> {
    fn kind(&self) -> Result<Kind, serde_json::Error> {
        let kind: Kind = serde_json::from_str(required(self.kind, "type")?.get())?;
        let relevant = (1 << Key::Type as u8)
            | (1 << Key::Id as u8)
            | match kind {
                Kind::Ping => 0,
                Kind::Query => 1 << Key::Request as u8,
                Kind::Command => (1 << Key::Method as u8) | (1 << Key::Params as u8),
                Kind::Subscribe => 1 << Key::Events as u8,
            };
        if self.duplicates & relevant != 0 {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "duplicate IPC field",
            ));
        }
        Ok(kind)
    }
}
fn required<'a>(
    v: Option<&'a RawValue>,
    name: &'static str,
) -> Result<&'a RawValue, serde_json::Error> {
    v.ok_or_else(|| <serde_json::Error as serde::de::Error>::missing_field(name))
}
pub(super) fn decode(
    raw: &str,
    book: &Arc<ControlBudget>,
) -> Result<Decoded<IpcRequest>, DecodeError> {
    let work = workspace(raw.as_bytes(), book).map_err(DecodeError::Capacity)?;
    let mut storage = book
        .reserve(ControlCharge::RequestBytes(0))
        .map_err(DecodeError::Capacity)?;
    let result = (|| -> Result<IpcRequest, Failure> {
        // Content used to inspect every numeric value, including unknown root
        // fields. Preserve that validation without allocating Content nodes.
        validate::literal(raw.as_bytes())?;
        let envelope: Envelope<'_> = serde_json::from_str(raw)?;
        let kind = envelope.kind()?;
        let id = match envelope.id {
            Some(raw) if raw.get() != "null" => Some(value(raw, book, &mut storage)?),
            _ => None,
        };
        Ok(match kind {
            Kind::Ping => IpcRequest::Ping { id },
            Kind::Query => {
                let raw = required(envelope.request, "request")?;
                storage.grow(body::bytes(raw.get().as_bytes(), book, true)?)?;
                IpcRequest::Query {
                    id,
                    request: serde_json::from_str(raw.get())?,
                }
            }
            Kind::Command => {
                let method = required(envelope.method, "method")?;
                storage.grow(method.get().len())?;
                let method: String = serde_json::from_str(method.get())?;
                let params = value(required(envelope.params, "params")?, book, &mut storage)?;
                IpcRequest::Command { id, method, params }
            }
            Kind::Subscribe => {
                let events = match envelope.events {
                    None => None,
                    Some(raw) if raw.get() == "null" => None,
                    Some(raw) => {
                        // String rows are smaller than Value rows; this same
                        // checked tree envelope also covers bad-prefix decode.
                        storage.grow(estimate::value(raw.get().as_bytes(), book)?)?;
                        Some(serde_json::from_str(raw.get())?)
                    }
                };
                IpcRequest::Subscribe { id, events }
            }
        })
    })();
    finish(result, storage, work)
}
