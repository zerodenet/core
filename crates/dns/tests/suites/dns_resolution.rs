//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../cache_ttl.rs"]
mod cache_ttl;
#[path = "../cname_chain.rs"]
mod cname_chain;
#[path = "../dispatch.rs"]
mod dispatch;
#[path = "../ech.rs"]
mod ech;
#[path = "../ech_disabled.rs"]
mod ech_disabled;
#[path = "../resolution.rs"]
mod resolution;

#[path = "../family_policy.rs"]
mod family_policy;
