//! Opaque cache identities never contain raw credentials or plugin options.
pub(crate) fn cache_identity<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> String {
    let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
    for part in parts {
        hash.update(&(part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    hash.finish()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
