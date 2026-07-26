use crate::plan::{EnginePlan, TargetId};

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanView<'a> {
    plan: &'a EnginePlan,
}

impl<'a> PlanView<'a> {
    pub(crate) fn new(plan: &'a EnginePlan) -> Self {
        Self { plan }
    }

    pub(crate) fn target_tag(self, target_id: TargetId) -> &'a str {
        self.plan
            .target(target_id)
            .expect("engine plan should resolve target id")
            .tag()
    }

    pub(crate) fn target_tag_owned(self, target_id: TargetId) -> String {
        self.target_tag(target_id).to_owned()
    }

    pub(crate) fn target_tags(self, target_ids: &[TargetId]) -> Vec<String> {
        target_ids
            .iter()
            .map(|target_id| self.target_tag_owned(*target_id))
            .collect()
    }

    pub(crate) fn render_target_chains(self, chains: &[Vec<TargetId>]) -> Vec<Vec<String>> {
        chains.iter().map(|chain| self.target_tags(chain)).collect()
    }
}
