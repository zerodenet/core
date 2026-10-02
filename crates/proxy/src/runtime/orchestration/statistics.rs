//! Sampling belongs to runtime lifecycle, including embedded/FFI runtimes.
//! One worker and bounded pages; subscriber delivery is Engine's nonblocking log.
pub(super) struct Sampler(tokio::task::JoinHandle<()>);
impl Sampler {
    pub fn start(engine: zero_engine::Engine) -> Self {
        Self(tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut seconds = 0_u64;
            let mut endpoint_offset = 0;
            loop {
                tick.tick().await;
                let engine = engine.clone();
                let endpoint_due = seconds.is_multiple_of(10);
                seconds = seconds.wrapping_add(1);
                let result = tokio::task::spawn_blocking(move || {
                    #[cfg(feature = "host-network-stats")]
                    {
                        sample_host_interfaces(&engine);
                        engine.push_host_traffic_stats_sampled();
                    }
                    engine.push_stats_sampled();
                    engine.push_flow_updates();
                    if endpoint_due {
                        engine.push_endpoint_stats_sampled_page(endpoint_offset)
                    } else {
                        endpoint_offset
                    }
                })
                .await;
                match result {
                    Ok(offset) => endpoint_offset = offset,
                    Err(_) => return,
                }
            }
        }))
    }
}
impl Drop for Sampler {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(feature = "host-network-stats")]
fn sample_host_interfaces(engine: &zero_engine::Engine) {
    let Ok(interfaces) = zero_platform_tokio::network_statistics::read() else {
        engine.observe_host_interfaces(None);
        return;
    };
    let sampled = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64;
    let samples = interfaces
        .into_iter()
        .map(|s| zero_engine::HostInterfaceSample {
            name: s.name,
            index: s.index,
            accounting_basis: s.accounting_basis,
            sampled_at_unix_ms: sampled,
            counters: [
                None,
                None,
                Some(s.rx_bytes),
                Some(s.tx_bytes),
                Some(s.rx_packets),
                Some(s.tx_packets),
                None,
                None,
                s.rx_dropped_packets,
                s.tx_dropped_packets,
                s.rx_errors,
                s.tx_errors,
            ],
        })
        .collect::<Vec<_>>();
    engine.observe_host_interfaces(Some(&samples));
}
