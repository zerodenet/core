use super::*;
type Failure = (&'static str, io::Error);

pub(super) fn bind(state: &mut State, address: SocketAddr) -> Result<(), Failure> {
    let original = super::create::socket(address).map_err(|source| ("create_socket", source))?;
    state.raw = Some(original.into());
    {
        let original = socket2::SockRef::from(state.raw.as_ref().expect("installed original"));
        #[cfg(unix)]
        original
            .set_cloexec(true)
            .map_err(|source| ("close_on_exec", source))?;
        #[cfg(target_vendor = "apple")]
        original
            .set_nosigpipe(true)
            .map_err(|source| ("no_sigpipe", source))?;
        // Preserve Tokio/Mio listener policy, including no REUSEADDR on Windows.
        #[cfg(not(windows))]
        original
            .set_reuse_address(true)
            .map_err(|source| ("reuse_address", source))?;
        original
            .set_nonblocking(true)
            .map_err(|source| ("nonblocking", source))?;
        original
            .bind(&address.into())
            .map_err(|source| ("bind", source))?;
        let backlog = if cfg!(target_os = "horizon") {
            20
        } else if cfg!(target_os = "haiku") {
            32
        } else {
            128
        };
        original
            .listen(backlog)
            .map_err(|source| ("listen", source))?;
    }
    // The runtime consumes this original even when registration returns Err.
    // Keep permanent Unknown before that call, including unwinding; never
    // reconstruct its numeric descriptor or infer closure from port availability.
    state.error = Some(uncertain(
        "Tokio listener registration did not return a native outcome",
    ));
    let original = state
        .raw
        .take()
        .expect("installed original before registration");
    let listener = TcpListener::from_std(original).map_err(|source| ("register", source))?;
    state.listener = Some(listener);
    state.error = None;
    Ok(())
}
