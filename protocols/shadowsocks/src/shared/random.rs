use super::*;
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn random_u64() -> Result<u64, Error> {
    let mut bytes = [0u8; 8];
    fill_random(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(feature = "crypto")]
pub(crate) fn fill_random(bytes: &mut [u8]) -> Result<(), Error> {
    use ring::rand::SecureRandom;
    ring::rand::SystemRandom::new()
        .fill(bytes)
        .map_err(|_| Error::Protocol("ss: random failed"))
}

#[cfg(feature = "crypto")]
pub fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
