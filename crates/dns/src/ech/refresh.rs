//! One real Node query job also owns its original cache-update completion.
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::{
    dns::DnsGeneration, scope::TaskJoinWitness, BoxFuture, CancellationToken,
};

#[derive(Clone, Debug)]
pub struct EchRefresh {
    pub task: TaskJoinWitness,
    pub cancel: CancellationToken,
    pub generation: DnsGeneration,
}
impl DnsSystem {
    /// Register before running any HTTPS RR attempt. Caller drop/entry eviction
    /// never discards the Node provider's retained native/task cleanup ledger.
    /// `finish` is metadata-only publication into the existing ECH cache entry.
    pub fn spawn_ech_refresh(
        self: &Arc<Self>,
        query_name: String,
        server: String,
        finish: impl FnOnce(io::Result<EchDnsRecord>) -> BoxFuture<'static, ()> + Send + 'static,
    ) -> io::Result<EchRefresh> {
        let execution = self.prepare_owned_ech(&server)?;
        let generation = execution.generation();
        let tasks = execution.tasks()?;
        let cancel = CancellationToken::new();
        let dns = self.clone();
        let task = tasks.spawn_ech_job(
            async move {
                // This same registered job directly owns the attempt. Calling the
                // coalescing public API here would spawn a second Query at N=1.
                let mut answer = execution.query(&query_name).await;
                use zero_platform_tokio::contract::dns::DnsService;
                if dns.generation() != generation {
                    answer = Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "ECH refresh generation retired before publication",
                    ));
                }
                finish(answer).await;
            },
            cancel.clone(),
            generation.network.0,
        )?;
        Ok(EchRefresh {
            task,
            cancel,
            generation,
        })
    }
}
