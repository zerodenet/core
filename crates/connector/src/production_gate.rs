use std::path::Path;

mod model;
mod validation;

pub use model::{
    ProductionGateCandidate, ProductionGateCheck, ProductionGateEvidence, ProductionGateReport,
};

pub fn verify_production_gate(
    candidate: ProductionGateCandidate,
    evidence_paths: [&Path; 5],
) -> Result<ProductionGateReport, String> {
    validation::verify(candidate, evidence_paths)
}
