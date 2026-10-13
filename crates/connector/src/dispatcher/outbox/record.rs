use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::{JournalRecord, OutboxDelivery};

// Deriving Deserialize for an internally tagged enum first buffers its entire
// content, including an IgnoredAny payload. Decode fields directly instead so
// replay stays streaming regardless of field order or payload length.
impl<'de, P: Deserialize<'de>> Deserialize<'de> for JournalRecord<P> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RecordVisitor<P>(PhantomData<P>);
        impl<'de, P: Deserialize<'de>> Visitor<'de> for RecordVisitor<P> {
            type Value = JournalRecord<P>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an outbox PUT or ACK record")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut op: Option<String> = None;
                let mut delivery: Option<Box<OutboxDelivery<P>>> = None;
                let mut sink_tag: Option<String> = None;
                let mut event_id: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "op" => {
                            if op.is_some() {
                                return Err(de::Error::duplicate_field("op"));
                            }
                            op = Some(map.next_value()?);
                        }
                        "delivery" => {
                            if delivery.is_some() {
                                return Err(de::Error::duplicate_field("delivery"));
                            }
                            delivery = Some(map.next_value()?);
                        }
                        "sink_tag" => {
                            if sink_tag.is_some() {
                                return Err(de::Error::duplicate_field("sink_tag"));
                            }
                            sink_tag = Some(map.next_value()?);
                        }
                        "event_id" => {
                            if event_id.is_some() {
                                return Err(de::Error::duplicate_field("event_id"));
                            }
                            event_id = Some(map.next_value()?);
                        }
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                match op
                    .as_deref()
                    .ok_or_else(|| de::Error::missing_field("op"))?
                {
                    "put" => Ok(JournalRecord::Put {
                        delivery: delivery.ok_or_else(|| de::Error::missing_field("delivery"))?,
                    }),
                    "ack" => Ok(JournalRecord::Ack {
                        sink_tag: sink_tag.ok_or_else(|| de::Error::missing_field("sink_tag"))?,
                        event_id: event_id.ok_or_else(|| de::Error::missing_field("event_id"))?,
                    }),
                    other => Err(de::Error::unknown_variant(other, &["put", "ack"])),
                }
            }
        }
        deserializer.deserialize_map(RecordVisitor(PhantomData))
    }
}
