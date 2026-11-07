//! The same actual component slot, separated without changing ownership.
use super::*;

impl ComponentSlot {
    pub fn new(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ValidatedRecipe,
        dial: zero_traits::DialPolicy,
    ) -> Self {
        Self::from_recipe(
            id,
            generation,
            recipe.component_recipe(),
            dial,
            recipe.legacy_connect_stage(),
        )
    }
    pub fn from_recipe(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ComponentRecipe,
        dial: zero_traits::DialPolicy,
        legacy_connect_stage: &'static str,
    ) -> Self {
        Self::from_bound_recipe(
            id,
            generation,
            recipe,
            dial,
            legacy_connect_stage,
            Arc::from([]),
        )
    }
    pub fn from_bound_recipe(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ComponentRecipe,
        dial: zero_traits::DialPolicy,
        legacy_connect_stage: &'static str,
        dependencies: Arc<[ComponentInstanceId]>,
    ) -> Self {
        Self {
            id,
            generation,
            recipe,
            dependencies,
            dependency_slots: OnceLock::new(),
            legacy_connect_stage,
            dial,
            cancel: CancellationToken::new(),
            users: Arc::new(users::Users::new(id)),
            user_roots: OnceLock::new(),
            admission: OnceLock::new(),
            admission_owner: Arc::new(()),
            live: OnceCell::new(),
            start_gate: Mutex::new(()),
            owned_start_gate: std::sync::Mutex::new(()),
            #[cfg(any(target_os = "linux", windows))]
            prepaid_process: OnceLock::new(),
            owned_start: OnceLock::new(),
            owned_start_host: OnceLock::new(),
            start_wake: Arc::default(),
            originals: OnceLock::new(),
            packet_source: Arc::new(OnceLock::new()),
        }
    }
}
