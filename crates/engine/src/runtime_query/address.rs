//! Stack-only textual address projection, with no owned String copy.
use serde::{Serialize, Serializer};
use zero_core::Address;
#[derive(Clone, Copy)]
pub(super) struct Host<'a>(pub &'a Address);
impl std::fmt::Display for Host<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Address::Domain(value) => f.write_str(value),
            Address::Ipv4(value) => std::fmt::Display::fmt(&std::net::Ipv4Addr::from(*value), f),
            Address::Ipv6(value) => std::fmt::Display::fmt(&std::net::Ipv6Addr::from(*value), f),
        }
    }
}
impl Serialize for Host<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}
#[derive(Serialize)]
pub(super) struct AddressView<'a> {
    family: &'static str,
    value: Host<'a>,
}
impl<'a> AddressView<'a> {
    pub fn new(value: &'a Address) -> Self {
        Self {
            family: match value {
                Address::Domain(_) => "domain",
                Address::Ipv4(_) => "ipv4",
                Address::Ipv6(_) => "ipv6",
            },
            value: Host(value),
        }
    }
}
