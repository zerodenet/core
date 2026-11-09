//! Fixed borrowed response header, including the existing error wire shape.
use super::*;
#[derive(Clone, Copy, serde::Deserialize)]
pub(super) struct Header<'a> {
    pub(super) ok: bool,
    #[serde(borrow, default)]
    pub(super) result: Option<&'a RawValue>,
    #[serde(borrow, default)]
    pub(super) error: Option<Error<'a>>,
    #[serde(borrow, default, deserialize_with = "optional_string")]
    #[serde(rename = "api_id")]
    _api_id: Option<&'a RawValue>,
    #[serde(borrow, default)]
    #[serde(rename = "id")]
    _id: Option<&'a RawValue>,
}
#[derive(Clone, Copy, serde::Deserialize)]
pub(super) struct Error<'a> {
    #[serde(borrow, deserialize_with = "string")]
    pub(super) message: &'a RawValue,
    #[serde(borrow, deserialize_with = "string")]
    #[serde(rename = "code")]
    _code: &'a RawValue,
    #[serde(borrow, default, deserialize_with = "optional_string")]
    #[serde(rename = "field_path")]
    _field_path: Option<&'a RawValue>,
    // Validation is streamed and retains no Vec of API details.
    #[serde(default, deserialize_with = "details")]
    #[serde(rename = "details")]
    _details: (),
}
fn check(raw: &RawValue) -> Result<(), &'static str> {
    if raw.get().starts_with('"') {
        Ok(())
    } else {
        Err("expected JSON string")
    }
}
fn string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<&'de RawValue, D::Error> {
    let raw = <&RawValue as serde::Deserialize>::deserialize(d)?;
    check(raw).map_err(serde::de::Error::custom)?;
    Ok(raw)
}
fn optional_string<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<&'de RawValue>, D::Error> {
    let raw = <Option<&RawValue> as serde::Deserialize>::deserialize(d)?;
    if let Some(raw) = raw {
        check(raw).map_err(serde::de::Error::custom)?;
    }
    Ok(raw)
}
fn details<'de, D: serde::Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    struct Details;
    impl<'de> serde::de::Visitor<'de> for Details {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("error details array")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut a: A) -> Result<(), A::Error> {
            #[derive(Clone, Copy, serde::Deserialize)]
            struct Detail<'a> {
                #[serde(borrow, deserialize_with = "string")]
                #[serde(rename = "message")]
                _message: &'a RawValue,
                #[serde(borrow, default, deserialize_with = "optional_string")]
                #[serde(rename = "field_path")]
                _field_path: Option<&'a RawValue>,
            }
            while a.next_element::<Detail<'de>>()?.is_some() {}
            Ok(())
        }
    }
    d.deserialize_seq(Details)
}
