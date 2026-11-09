//! Kernel production subscriber over already-installed original controllers.
use super::*;
impl ScopedTracing {
    /// All cold output controllers and actual Scope already exist. File IO is
    /// performed only by the subsequently registered and strongly held workers.
    pub fn install(&self, config: &LogConfig) {
        let level = config.level.as_str();
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new(level))
            .expect("valid tracing log level");
        let limiter = config
            .rate_limit
            .map(|r| Arc::new(RateLimiter::new(r.max_per_second)));
        let rate = limiter.clone();
        let stderr = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_target(false)
            .compact()
            .with_filter(tracing_subscriber::filter::filter_fn(
                move |_meta: &tracing::Metadata<'_>| rate.as_ref().is_none_or(|rate| rate.allow()),
            ))
            .with_filter(filter);
        let mut layers: Vec<Box<dyn Layer<_> + Send + Sync>> =
            vec![Box::new(stderr), Box::new(WarningBridgeLayer)];
        for (index, output) in config.files.iter().enumerate() {
            let level = output.level.as_deref().unwrap_or(level);
            let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new(level));
            let rate = limiter.clone();
            let layer = tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(false)
                .with_writer(self.writer(index).expect("same declared logger output"))
                .json()
                .with_filter(tracing_subscriber::filter::filter_fn(
                    move |_meta: &tracing::Metadata<'_>| {
                        rate.as_ref().is_none_or(|rate| rate.allow())
                    },
                ))
                .with_filter(filter);
            layers.push(Box::new(layer));
        }
        let _ = tracing_subscriber::registry().with(layers).try_init();
    }
}
