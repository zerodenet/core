//! Validate the same JSON value domain as delivery without retaining values.
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::io::{BufRead, BufReader, Read};

mod strings;
use strings::StringElider;

struct DiscardValue;

impl<'de> Deserialize<'de> for DiscardValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = DiscardValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON value supported by event delivery")
            }

            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Ok(DiscardValue)
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Ok(DiscardValue)
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Ok(DiscardValue)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Ok(DiscardValue)
            }
            fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(DiscardValue)
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(DiscardValue)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                while seq.next_element::<DiscardValue>()?.is_some() {}
                Ok(DiscardValue)
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                while map.next_key::<DiscardValue>()?.is_some() {
                    map.next_value::<DiscardValue>()?;
                }
                Ok(DiscardValue)
            }
        }
        // deserialize_any retains serde_json's numeric and recursion checks;
        // deserialize_ignored_any deliberately omits those checks.
        deserializer.deserialize_any(ValueVisitor)
    }
}

pub(super) fn validate_slice(bytes: &[u8]) -> Result<(), serde_json::Error> {
    serde_json::from_slice::<DiscardValue>(bytes).map(|_| ())
}

pub(super) fn validate_reader(reader: impl Read) -> Result<(), serde_json::Error> {
    let mut input = BufReader::with_capacity(super::BUFFER_BYTES, reader);
    validate_elided(&mut input).map_err(|error| {
        if error.io_error_kind() == Some(std::io::ErrorKind::InvalidData) {
            <serde_json::Error as de::Error>::custom(error.to_string())
        } else {
            error
        }
    })
}

fn validate_elided(reader: &mut impl BufRead) -> Result<(), serde_json::Error> {
    // Large strings are validated and replaced by empty strings. Numbers,
    // literals and collection structure still reach the standard parser.
    // The original JSONL frame has already passed raw UTF-8 validation.
    let mut decoder = serde_json::Deserializer::from_reader(StringElider::new(reader));
    DiscardValue::deserialize(&mut decoder)?;
    decoder.end()
}
