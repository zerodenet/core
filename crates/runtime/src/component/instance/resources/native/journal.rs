//! Concrete same-Host declared journal originals, not a generic file executor.
use super::*;
use zero_platform_tokio::{
    contract::lifecycle::{CloseReport, Unfinished},
    journal::{JournalGrant, JournalOwner},
};
pub(super) struct Journals(Vec<(usize, JournalOwner)>);
#[derive(Clone, Debug)]
pub(in crate::component) struct JournalEvidence {
    pub ordinal: usize,
    pub confirmed: bool,
}
impl Journals {
    pub(super) fn new(
        declarations: &[ResourceDeclaration],
        component: ComponentId,
        instance: Option<zero_core::component::ComponentInstanceId>,
        tasks: Option<&zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
        root: &Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
        cancel: &CancellationToken,
    ) -> Result<Self, ChannelError> {
        let mut entries = Vec::new();
        for (ordinal, declaration) in declarations.iter().enumerate() {
            let ResourceDeclaration::ControlledJournal { spec } = declaration else {
                continue;
            };
            if entries
                .iter()
                .any(|(_, owner): &(usize, JournalOwner)| owner.grant().path() == spec.path)
            {
                return Err(error(
                    component,
                    Reason::InvalidConfig,
                    "duplicate journal path",
                ));
            }
            let instance = instance.ok_or_else(|| {
                error(
                    component,
                    Reason::InvalidConfig,
                    "journal requires actual Host instance",
                )
            })?;
            let scope = tasks
                .and_then(|tasks| tasks.retirement_observation())
                .ok_or_else(|| {
                    error(
                        component,
                        Reason::InvalidConfig,
                        "journal requires actual Host Scope",
                    )
                })?;
            let owner =
                JournalOwner::new_hosted(spec.clone(), instance, scope, cancel.clone(), root)
                    .map_err(|source| {
                        io_error(
                            component,
                            zero_platform_tokio::contract::Stage::Prepare,
                            source,
                        )
                    })?;
            entries.push((ordinal, owner));
        }
        Ok(Self(entries))
    }
    pub(super) fn open(
        &self,
        component: ComponentId,
        ordinal: usize,
    ) -> Result<JournalGrant, ChannelError> {
        let (_, owner) = self
            .0
            .iter()
            .find(|(actual, _)| *actual == ordinal)
            .ok_or_else(|| {
                error(
                    component,
                    Reason::InvalidConfig,
                    "undeclared journal ordinal",
                )
            })?;
        owner.start().map_err(|source| {
            io_error(
                component,
                zero_platform_tokio::contract::Stage::Prepare,
                source,
            )
        })?;
        Ok(owner.grant())
    }
    pub(super) fn faults(&self) -> Vec<Arc<ChannelError>> {
        self.0
            .iter()
            .filter_map(|(_, owner)| owner.failure())
            .collect()
    }
    pub(super) fn evidence(&self) -> Vec<JournalEvidence> {
        self.0
            .iter()
            .map(|(ordinal, owner)| JournalEvidence {
                ordinal: *ordinal,
                confirmed: owner
                    .closed()
                    .is_some_and(|receipt| owner.matches_closed(&receipt)),
            })
            .collect()
    }
    pub(super) fn close(&self, component: ComponentId) -> Result<CloseReport, ChannelError> {
        let mut report = CloseReport::default();
        let mut first = None;
        for (ordinal, owner) in &self.0 {
            match owner.try_close() {
                Ok(Some(receipt)) if owner.matches_closed(&receipt) => {
                    report.closed_io.push(format!(
                        "component {} journal {} exact original graph closed",
                        component.0, ordinal
                    ));
                }
                Ok(_) => report.unfinished.push(Unfinished::Io(format!(
                    "journal {ordinal} has an actual IO call in progress"
                ))),
                Err(source) => {
                    if owner
                        .closed()
                        .is_some_and(|receipt| owner.matches_closed(&receipt))
                    {
                        report.closed_io.push(format!(
                            "component {} journal {} exact original graph closed",
                            component.0, ordinal
                        ));
                        continue;
                    }
                    first.get_or_insert_with(|| {
                        io_error(
                            component,
                            zero_platform_tokio::contract::Stage::Close,
                            source,
                        )
                    });
                }
            }
        }
        // fault observation remains separately available after positive closure.
        match first {
            Some(error) => Err(error),
            None => Ok(report),
        }
    }
}
fn io_error(
    component: ComponentId,
    stage: zero_platform_tokio::contract::Stage,
    source: std::io::Error,
) -> ChannelError {
    let mut error = ChannelError::new(
        stage,
        if matches!(
            source.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::FileTooLarge
        ) {
            Reason::Capacity
        } else {
            Reason::Io
        },
        zero_platform_tokio::contract::Impact::Component,
    )
    .with_source(source);
    error.component = Some(component);
    error
}
