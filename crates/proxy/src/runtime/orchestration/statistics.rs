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
