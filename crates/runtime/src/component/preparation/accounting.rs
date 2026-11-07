//! One candidate's finite Host accounting projection, without configuration retention.
use super::*;

impl ResourcePreparation<'_> {
    pub(in crate::component) fn project_carrier_total(
        &self,
        recipe: ComponentRecipe,
    ) -> Result<ComponentRecipe, BuildError> {
        let limit = self.carrier_total_limit_for(recipe.spec())?;
        let process = self.process_limits_for(recipe.spec())?;
        let preparation = self.preparation_limits_for(recipe.spec())?;
        let buffers = self.buffer_limits_for(recipe.spec())?;
        let tasks = self.component_task_total_for(recipe.spec())?;
        Ok(recipe
            .with_carrier_total_limit(limit)
            .with_process_limits(process)
            .with_preparation_limits(preparation)
            .with_buffer_limits(buffers)
            .with_component_task_total(tasks))
    }
    pub(super) fn carrier_total_limit_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<usize>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        let limit = candidate.runtime.effective_carrier_max_total_originals();
        if !(1..=65536).contains(&limit) {
            return Err(invalid(
                Reason::InvalidConfig,
                "invalid actual carrier total capacity",
            ));
        }
        Ok(crate::component::recipe::accounting::owns_carrier_custody(spec).then_some(limit))
    }
    pub(super) fn component_task_total_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<usize>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        if spec.task_limit.is_none() {
            return Ok(None);
        }
        let limit = candidate.runtime.effective_component_total_max_tasks();
        if !(1..=65536).contains(&limit) {
            return Err(invalid(
                Reason::InvalidConfig,
                "invalid actual component task total",
            ));
        }
        Ok(Some(limit))
    }
    pub(super) fn buffer_limits_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<zero_platform_tokio::controlled_carrier::CarrierBufferLimits>, BuildError>
    {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        if !crate::component::recipe::accounting::owns_carrier_custody(spec) {
            return Ok(None);
        }
        let limits = zero_platform_tokio::controlled_carrier::CarrierBufferLimits {
            max_queue_packets: candidate.runtime.effective_quic_max_queue_packets(),
            total_max_bytes: candidate.runtime.effective_quic_total_max_buffered_bytes(),
        };
        limits.validate().map_err(|source| {
            invalid(
                Reason::InvalidConfig,
                "invalid actual carrier buffer capacities",
            )
            .with_source(source)
        })?;
        Ok(Some(limits))
    }
}

impl ResourcePreparation<'_> {
    pub(super) fn process_limits_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<crate::component::recipe::accounting::ProcessLimits>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        if !crate::component::recipe::accounting::owns_process_custody(spec) {
            return Ok(None);
        }
        Ok(Some(
            crate::component::recipe::accounting::ProcessLimits::from_runtime(&candidate.runtime)?,
        ))
    }
}

impl ResourcePreparation<'_> {
    pub(super) fn preparation_limits_for(
        &self,
        spec: &ComponentSpec,
    ) -> Result<Option<crate::component::recipe::accounting::PreparationLimits>, BuildError> {
        let Some(candidate) = self.candidate() else {
            return Ok(None);
        };
        if !crate::component::recipe::accounting::owns_carrier_custody(spec) {
            return Ok(None);
        }
        crate::component::recipe::accounting::PreparationLimits::from_runtime(&candidate.runtime)
            .map(Some)
    }
}
