use super::*;
pub(super) struct StringSeed<'a>(pub &'a mut Owner);
impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = String;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<String, D::Error> {
        struct Text<'a>(&'a mut Owner);
        impl<'de> Visitor<'de> for Text<'_> {
            type Value = String;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
                self.0.string(value).map_err(|error| self.0.error(error))
            }
        }
        decoder.deserialize_str(Text(self.0))
    }
}
pub(super) struct OptionalString<'a>(pub &'a mut Owner);
impl<'de> DeserializeSeed<'de> for OptionalString<'_> {
    type Value = Option<String>;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        impl<'de> Visitor<'de> for OptionalString<'_> {
            type Value = Option<String>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("option")
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_some<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
                StringSeed(self.0).deserialize(decoder).map(Some)
            }
        }
        decoder.deserialize_option(self)
    }
}
pub(super) struct Strings<'a>(pub &'a mut Owner);
impl<'de> DeserializeSeed<'de> for Strings<'_> {
    type Value = Vec<String>;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        impl<'de> Visitor<'de> for Strings<'_> {
            type Value = Vec<String>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a sequence")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut rows = Vec::new();
                while let Some(value) = seq.next_element_seed(StringSeed(self.0))? {
                    self.0.push(&mut rows, value).map_err(|(error, original)| {
                        drop(original);
                        self.0.error(error)
                    })?;
                }
                Ok(rows)
            }
        }
        decoder.deserialize_seq(self)
    }
}
pub(super) struct Addresses<'a>(pub &'a mut Owner);
impl<'de> DeserializeSeed<'de> for Addresses<'_> {
    type Value = Vec<IpAddr>;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        impl<'de> Visitor<'de> for Addresses<'_> {
            type Value = Vec<IpAddr>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a sequence")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut rows = Vec::new();
                while let Some(value) = seq.next_element()? {
                    self.0
                        .push(&mut rows, value)
                        .map_err(|(error, _)| self.0.error(error))?;
                }
                Ok(rows)
            }
        }
        decoder.deserialize_seq(self)
    }
}
pub(super) struct Egress<'a>(pub &'a mut Owner);
impl<'de> DeserializeSeed<'de> for Egress<'_> {
    type Value = RouteInterface;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Name,
            Index,
            #[serde(other)]
            Other,
        }
        impl<'de> Visitor<'de> for Egress<'_> {
            type Value = RouteInterface;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("struct RouteInterface")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let (mut name, mut index) = (None, None);
                while let Some(field) = map.next_key()? {
                    match field {
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value_seed(StringSeed(self.0))?);
                        }
                        Field::Index => {
                            if index.is_some() {
                                return Err(de::Error::duplicate_field("index"));
                            }
                            index = Some(map.next_value()?);
                        }
                        Field::Other => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(RouteInterface {
                    name: name.ok_or_else(|| de::Error::missing_field("name"))?,
                    index: index.ok_or_else(|| de::Error::missing_field("index"))?,
                })
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                Ok(RouteInterface {
                    name: seq
                        .next_element_seed(StringSeed(self.0))?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?,
                    index: seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                })
            }
        }
        decoder.deserialize_struct("RouteInterface", &["name", "index"], self)
    }
}
