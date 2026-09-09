//! Transport-neutral BBR tuning. Protocol profiles map into these parameters.
use super::Bbr;
use quinn::congestion::{Controller, ControllerFactory};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct BbrParameters {
    pub startup_pacing_gain: f64,
    pub startup_window_gain: f64,
    pub window_gain: f64,
    pub startup_rounds: u64,
    pub drain_to_target: bool,
    pub detect_overshooting: bool,
    pub loss_multiplier: u64,
    pub startup_ack_aggregation: bool,
    pub expire_startup_ack_aggregation: bool,
    pub avoid_overestimate: bool,
    pub reduce_ack_height_on_bandwidth_growth: bool,
}
impl Default for BbrParameters {
    fn default() -> Self {
        Self {
            startup_pacing_gain: 2.885,
            startup_window_gain: 2.0,
            window_gain: 2.0,
            startup_rounds: 3,
            drain_to_target: false,
            detect_overshooting: false,
            loss_multiplier: 2,
            startup_ack_aggregation: false,
            expire_startup_ack_aggregation: false,
            avoid_overestimate: false,
            reduce_ack_height_on_bandwidth_growth: false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct BbrConfig {
    pub parameters: BbrParameters,
    pub initial_window: u64,
}
impl Default for BbrConfig {
    fn default() -> Self {
        Self {
            parameters: BbrParameters::default(),
            initial_window: 38_400,
        }
    }
}
impl ControllerFactory for BbrConfig {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        Box::new(Bbr::new(self.parameters, self.initial_window, now, mtu))
    }
}
