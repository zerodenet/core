//! DNS message validation, inspection, and synthetic response construction.

mod build;
mod name;
mod parse;
mod policy;

pub(crate) use build::{
    build_address_response, build_error_response, build_query, fit_response_to_udp,
};
pub(crate) use name::normalize_domain;
pub use parse::DnsQuestion;
pub(crate) use parse::{parse_question, parse_response, ParsedDnsResponse};
pub(crate) use policy::{apply_response_address_policy, ResponseAddressPolicy};

pub(crate) fn rewrite_response_ttls(
    response: &mut [u8],
    elapsed_seconds: u32,
    ttl_cap: Option<u32>,
) -> std::io::Result<()> {
    parse::rewrite_response_ttls(response, elapsed_seconds, ttl_cap)
}

pub(crate) const TYPE_A: u16 = 1;
pub(crate) const TYPE_SVCB: u16 = 64;
pub(crate) const TYPE_HTTPS: u16 = 65;
pub(crate) const TYPE_AAAA: u16 = 28;
pub(crate) const TYPE_OPT: u16 = 41;

pub(crate) const RCODE_NOERROR: u8 = 0;
pub(crate) const RCODE_FORMERR: u8 = 1;
pub(crate) const RCODE_SERVFAIL: u8 = 2;
pub(crate) const RCODE_NXDOMAIN: u8 = 3;
pub(crate) const RCODE_NOTIMP: u8 = 4;

pub(crate) const DEFAULT_SYNTHETIC_TTL_SECONDS: u32 = 60;
pub(crate) const DEFAULT_NEGATIVE_TTL_SECONDS: u32 = 60;
pub(crate) const MAX_DNS_MESSAGE_SIZE: usize = u16::MAX as usize;
pub(crate) const MAX_UDP_DNS_PAYLOAD: usize = 4096;
