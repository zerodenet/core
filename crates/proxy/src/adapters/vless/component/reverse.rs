//! Thin cold projection of actual Source/provider edges, with no IO or runtime.
use super::*;
use crate::component::{RegistrationError, ResourcePreparation};
use sha2::{Digest, Sha256};
pub(super) fn options(
    adapter: &super::super::VlessAdapter,
    input: &OutboundConfig,
    resources: &mut ResourcePreparation<'_>,
) -> Result<::vless::reverse::component::Options, RegistrationError> {
    let mut sources = Vec::new();
    if let Some(config) = resources.candidate() {
        for source in &config.inbounds {
            if matches!(&source.protocol,zero_config::InboundProtocolConfig::Vless{users,..} if users.iter().any(|user|user.reverse_tag.as_deref()==Some(input.tag())))
            {
                let encoded = serde_json::to_vec(source)
                    .map_err(|source| ConfigError::InvalidInbound(source.to_string()))?;
                let digest: [u8; 32] = Sha256::digest(encoded).into();
                sources.push((source.tag.clone(), digest));
            }
        }
    }
    sources.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut identity = Sha256::new();
    let component = resources.named_component(input.tag());
    for (tag, digest) in sources {
        identity.update((tag.len() as u64).to_be_bytes());
        identity.update(tag.as_bytes());
        identity.update(digest);
        resources.add_source_dependency(
            &tag,
            digest,
            component.ok_or_else(|| {
                ConfigError::InvalidOutbound("reverse provider has no actual host identity".into())
            })?,
        )?;
    }
    let limit = resources
        .candidate()
        .map(|config| config.runtime.effective_component_total_max_tasks())
        .unwrap_or(8192);
    Ok(adapter
        .runtime
        .reverse_component_options(input.tag())
        .with_source_identity(identity.finalize().into())
        .with_source_limit(
            std::num::NonZeroUsize::new(limit)
                .ok_or_else(|| ConfigError::InvalidRuntime("zero reverse Source limit".into()))?,
        ))
}
