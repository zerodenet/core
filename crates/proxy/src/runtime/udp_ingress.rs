//! Narrow ingress from an accepted inbound UDP packet into [`UdpPipe`].

mod dispatch;
mod model;
mod route;
mod session;
#[cfg(test)]
mod tests;

pub(crate) use model::UdpIngressRuntime;

#[cfg(test)]
#[path = "udp_ingress/tests/source.rs"]
pub(crate) mod test_source;
