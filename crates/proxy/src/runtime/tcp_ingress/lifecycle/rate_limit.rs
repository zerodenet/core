use zero_config::RuntimeConfig;
use zero_core::Session;

#[cfg(test)]
#[path = "tests/rate_limit.rs"]
mod tests;

pub(crate) fn apply_kernel_rate_limits_from_config(
    config: &RuntimeConfig,
    session: &mut Session,
    inbound_tag: &str,
) {
    let Some(cfg) = config.inbounds.iter().find(|i| i.tag == inbound_tag) else {
        return;
    };
    let (up, down) = cfg.protocol.rate_limits();
    if session.up_bps.is_none() {
        session.up_bps = up;
    }
    if session.down_bps.is_none() {
        session.down_bps = down;
    }
}
