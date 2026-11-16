//! Existing fixed-peer Packet source grammar, separate from standard producer selection.
use zero_config::{OutboundConfig, RuntimeConfig};

pub(crate) fn packet_source_selected(config: &RuntimeConfig, input: &OutboundConfig) -> bool {
    graph::packet_source_selected(config, input)
}
mod graph;
#[cfg(test)]
#[path = "mux_prefix/tests/producer.rs"]
mod pool_tests;
#[cfg(test)]
#[path = "mux_prefix/tests.rs"]
mod tests;
