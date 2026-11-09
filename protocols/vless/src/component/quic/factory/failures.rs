//! Two actual owning books. Every Ready result is durable before its branch
//! returns; cancellation of another pending branch cannot discard that result.
use super::*;
use zero_platform_tokio::contract::RetainedError;
use zero_traits::packet_metadata::PacketMetadataCredit;
pub(super) struct Branch {
    source: Option<RetainedError>,
    credit: Option<PacketMetadataCredit>,
    report: CloseReport,
}
pub(super) struct OriginalSources([Branch; 2]);
impl OriginalSources {
    pub fn prepare(memory: &PacketDriverMetadataView) -> Result<Self, BuildError> {
        let layout = RetainedError::metadata_layout().map_err(capacity)?;
        let mut paid = memory
            .reserve(layout.checked_add(layout).map_err(capacity)?)
            .map_err(capacity)?;
        let first = paid.split(layout).map_err(capacity)?;
        Ok(Self([Branch::new(first), Branch::new(paid)]))
    }
    pub fn branches(&mut self) -> (&mut Branch, &mut Branch) {
        let [first, second] = &mut self.0;
        (first, second)
    }
    pub fn report(
        &mut self,
        generation: &entry::Generation,
        report: &mut CloseReport,
    ) -> Result<(), BuildError> {
        for branch in &mut self.0 {
            if let Some(source) = &branch.source {
                let failure = generation
                    .resources
                    .receive_protocol_failure(&generation.tasks.scope_identity(), source)?;
                if !branch
                    .report
                    .resource_failures
                    .iter()
                    .any(|same| Arc::ptr_eq(same, &failure))
                {
                    branch.report.resource_failures.push(failure);
                }
            }
        }
        for branch in &mut self.0 {
            report.append(std::mem::take(&mut branch.report));
        }
        Ok(())
    }
}
impl Branch {
    fn new(credit: PacketMetadataCredit) -> Self {
        Self {
            source: None,
            credit: Some(credit),
            report: CloseReport::default(),
        }
    }
    pub fn capture(&mut self, result: Result<CloseReport, BuildError>) {
        match result {
            Ok(received) => {
                // The original carrier book returns its confirmed delta once.
                // A new positive report supersedes only this branch's previous
                // unfinished observation, never its actual native/task facts.
                self.report.unfinished.clear();
                self.report.append(received);
            }
            Err(source) => {
                if self.source.is_none() {
                    self.source = Some(
                        RetainedError::from_prepaid(
                            source,
                            self.credit
                                .take()
                                .expect("one actual original source header"),
                        )
                        .unwrap_or_else(|_| panic!("original source header prepaid")),
                    );
                }
            }
        }
    }
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> BuildError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
