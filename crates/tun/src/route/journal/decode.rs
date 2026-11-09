//! Decode this fixed journal schema with pre-admitted returned allocations.
use super::{metadata::Owner, RouteInterface, RouteJournal};
use crate::packet_memory::{PacketDriverMetadataView, PacketMemoryError};
use serde::{
    de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor},
    Deserialize,
};
use std::{fmt, net::IpAddr};
mod scratch;
mod seeds;
use seeds::{Addresses, Egress, OptionalString, StringSeed, Strings};
#[derive(Debug)]
pub(super) enum Failure {
    Json(serde_json::Error),
    Memory(PacketMemoryError),
}
const FIELDS: &[&str] = &[
    "tun_name",
    "ipv6",
    "tun_index",
    "egress",
    "gateway",
    "excluded",
    "installed",
    "scoped_bypass",
];
struct Parts {
    tun_name: String,
    ipv6: bool,
    tun_index: u32,
    egress: RouteInterface,
    gateway: Option<String>,
    excluded: Vec<IpAddr>,
    installed: Vec<String>,
    scoped_bypass: bool,
}
struct Seed<'a>(&'a mut Owner);
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Parts;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Parts, D::Error> {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            TunName,
            Ipv6,
            TunIndex,
            Egress,
            Gateway,
            Excluded,
            Installed,
            ScopedBypass,
            #[serde(other)]
            Other,
        }
        impl<'de> Visitor<'de> for Seed<'_> {
            type Value = Parts;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("struct RouteJournal")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Parts, A::Error> {
                let (mut tun_name, mut ipv6, mut tun_index, mut egress) = (None, None, None, None);
                let (mut gateway, mut excluded, mut installed, mut scoped_bypass) =
                    (None, None, None, None);
                while let Some(field) = map.next_key()? {
                    match field {
                        Field::TunName => {
                            if tun_name.is_some() {
                                return Err(de::Error::duplicate_field("tun_name"));
                            }
                            tun_name = Some(map.next_value_seed(StringSeed(self.0))?);
                        }
                        Field::Ipv6 => {
                            if ipv6.is_some() {
                                return Err(de::Error::duplicate_field("ipv6"));
                            }
                            ipv6 = Some(map.next_value()?);
                        }
                        Field::TunIndex => {
                            if tun_index.is_some() {
                                return Err(de::Error::duplicate_field("tun_index"));
                            }
                            tun_index = Some(map.next_value()?);
                        }
                        Field::Egress => {
                            if egress.is_some() {
                                return Err(de::Error::duplicate_field("egress"));
                            }
                            egress = Some(map.next_value_seed(Egress(self.0))?);
                        }
                        Field::Gateway => {
                            if gateway.is_some() {
                                return Err(de::Error::duplicate_field("gateway"));
                            }
                            gateway = Some(map.next_value_seed(OptionalString(self.0))?);
                        }
                        Field::Excluded => {
                            if excluded.is_some() {
                                return Err(de::Error::duplicate_field("excluded"));
                            }
                            excluded = Some(map.next_value_seed(Addresses(self.0))?);
                        }
                        Field::Installed => {
                            if installed.is_some() {
                                return Err(de::Error::duplicate_field("installed"));
                            }
                            installed = Some(map.next_value_seed(Strings(self.0))?);
                        }
                        Field::ScopedBypass => {
                            if scoped_bypass.is_some() {
                                return Err(de::Error::duplicate_field("scoped_bypass"));
                            }
                            scoped_bypass = Some(map.next_value()?);
                        }
                        Field::Other => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(Parts {
                    tun_name: tun_name.ok_or_else(|| de::Error::missing_field("tun_name"))?,
                    ipv6: ipv6.ok_or_else(|| de::Error::missing_field("ipv6"))?,
                    tun_index: tun_index.ok_or_else(|| de::Error::missing_field("tun_index"))?,
                    egress: egress.ok_or_else(|| de::Error::missing_field("egress"))?,
                    gateway: gateway.unwrap_or(None),
                    excluded: excluded.ok_or_else(|| de::Error::missing_field("excluded"))?,
                    installed: installed.ok_or_else(|| de::Error::missing_field("installed"))?,
                    scoped_bypass: scoped_bypass.unwrap_or(false),
                })
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Parts, A::Error> {
                Ok(Parts {
                    tun_name: seq
                        .next_element_seed(StringSeed(self.0))?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?,
                    ipv6: seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?,
                    tun_index: seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(2, &self))?,
                    egress: seq
                        .next_element_seed(Egress(self.0))?
                        .ok_or_else(|| de::Error::invalid_length(3, &self))?,
                    gateway: seq
                        .next_element_seed(OptionalString(self.0))?
                        .unwrap_or(None),
                    excluded: seq
                        .next_element_seed(Addresses(self.0))?
                        .ok_or_else(|| de::Error::invalid_length(5, &self))?,
                    installed: seq
                        .next_element_seed(Strings(self.0))?
                        .ok_or_else(|| de::Error::invalid_length(6, &self))?,
                    scoped_bypass: seq.next_element()?.unwrap_or(false),
                })
            }
        }
        decoder.deserialize_struct("RouteJournal", FIELDS, self)
    }
}
pub(super) fn decode(
    bytes: &[u8],
    memory: PacketDriverMetadataView,
) -> Result<RouteJournal, Failure> {
    // Every temporary returned field is destroyed before this trailing owner
    // on any parse/admission error, including malformed later fields.
    let scratch_layout = scratch::layout(bytes).map_err(Failure::Memory)?;
    let _scratch = scratch_layout
        .map(|layout| memory.reserve(layout))
        .transpose()
        .map_err(Failure::Memory)?;
    let mut owner = Owner::new(Some(memory));
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let parts = match Seed(&mut owner).deserialize(&mut decoder) {
        Ok(parts) => parts,
        Err(error) => {
            return Err(owner
                .failure
                .take()
                .map_or(Failure::Json(error), Failure::Memory))
        }
    };
    decoder.end().map_err(Failure::Json)?;
    Ok(RouteJournal {
        tun_name: parts.tun_name,
        ipv6: parts.ipv6,
        tun_index: parts.tun_index,
        egress: parts.egress,
        gateway: parts.gateway,
        excluded: parts.excluded,
        installed: parts.installed,
        scoped_bypass: parts.scoped_bypass,
        path: Default::default(),
        _lease: None,
        #[cfg(target_os = "macos")]
        source: None,
        _metadata: owner,
        _removed_routes: 0,
        _removed_exclusions: 0,
    })
}

#[cfg(test)]
mod tests;
