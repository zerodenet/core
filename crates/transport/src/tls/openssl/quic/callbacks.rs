use std::ffi::{c_int, c_uchar, c_uint, c_void};

use openssl_sys::SSL;

use super::ffi;
use super::keys::CipherSuite;

const LEVELS: usize = 4;
const MAX_CRYPTO_BUFFER: usize = 64 * 1024;
const DIR_READ: usize = 0;
const DIR_WRITE: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(super) enum Level {
    Initial = 0,
    Early = 1,
    Handshake = 2,
    Application = 3,
}

impl Level {
    fn from_raw(level: c_uint) -> Option<Self> {
        match level {
            0 => Some(Self::Initial),
            1 => Some(Self::Early),
            2 => Some(Self::Handshake),
            3 => Some(Self::Application),
            _ => None,
        }
    }
}

pub(super) struct CallbackState {
    incoming: [Vec<u8>; LEVELS],
    outgoing: [Vec<u8>; LEVELS],
    secrets: [[Option<Vec<u8>>; 2]; LEVELS],
    suites: [Option<CipherSuite>; LEVELS],
    published: [bool; LEVELS],
    read_level: Level,
    write_level: Level,
    active_read: Option<(Level, Box<[u8]>)>,
    peer_transport_params: Option<Vec<u8>>,
    alert: Option<u8>,
    failure: Option<String>,
}

impl Default for CallbackState {
    fn default() -> Self {
        Self {
            incoming: Default::default(),
            outgoing: Default::default(),
            secrets: Default::default(),
            suites: Default::default(),
            published: [false; LEVELS],
            read_level: Level::Initial,
            write_level: Level::Initial,
            active_read: None,
            peer_transport_params: None,
            alert: None,
            failure: None,
        }
    }
}

impl CallbackState {
    pub(super) fn dispatch() -> [ffi::OsslDispatch; 7] {
        [
            ffi::dispatch_entry(
                ffi::OSSL_FUNC_SSL_QUIC_TLS_CRYPTO_SEND,
                crypto_send as ffi::CryptoSend,
            ),
            ffi::dispatch_entry(
                ffi::OSSL_FUNC_SSL_QUIC_TLS_CRYPTO_RECV_RCD,
                crypto_recv as ffi::CryptoRecv,
            ),
            ffi::dispatch_entry(
                ffi::OSSL_FUNC_SSL_QUIC_TLS_CRYPTO_RELEASE_RCD,
                crypto_release as ffi::CryptoRelease,
            ),
            ffi::dispatch_entry(
                ffi::OSSL_FUNC_SSL_QUIC_TLS_YIELD_SECRET,
                yield_secret as ffi::YieldSecret,
            ),
            ffi::dispatch_entry(
                ffi::OSSL_FUNC_SSL_QUIC_TLS_GOT_TRANSPORT_PARAMS,
                got_transport_params as ffi::GotTransportParams,
            ),
            ffi::dispatch_entry(ffi::OSSL_FUNC_SSL_QUIC_TLS_ALERT, alert as ffi::Alert),
            ffi::OsslDispatch {
                function_id: 0,
                function: std::ptr::null(),
            },
        ]
    }

    pub(super) fn push_input(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        let retained = self.active_read.as_ref().map_or(0, |(level, record)| {
            if *level == self.read_level {
                record.len()
            } else {
                0
            }
        });
        let input = &mut self.incoming[self.read_level as usize];
        if input
            .len()
            .saturating_add(bytes.len())
            .saturating_add(retained)
            > MAX_CRYPTO_BUFFER
        {
            return Err("OpenSSL QUIC CRYPTO input exceeded 64 KiB");
        }
        input.extend_from_slice(bytes);
        Ok(())
    }

    pub(super) fn drain_output(&mut self, level: Level, output: &mut Vec<u8>) {
        output.append(&mut self.outgoing[level as usize]);
    }

    pub(super) fn take_secret_pair(
        &mut self,
        level: Level,
    ) -> Option<(CipherSuite, Vec<u8>, Vec<u8>)> {
        let idx = level as usize;
        if self.published[idx] {
            return None;
        }
        let [read, write] = &self.secrets[idx];
        if read.is_none() || write.is_none() {
            return None;
        }
        self.published[idx] = true;
        Some((
            self.suites[idx]?,
            write.clone().unwrap(),
            read.clone().unwrap(),
        ))
    }

    pub(super) fn peer_transport_params(&self) -> Option<&[u8]> {
        self.peer_transport_params.as_deref()
    }

    pub(super) fn alert(&self) -> Option<u8> {
        self.alert
    }

    pub(super) fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
}

unsafe extern "C" fn crypto_send(
    _ssl: *mut SSL,
    buf: *const c_uchar,
    len: usize,
    consumed: *mut usize,
    arg: *mut c_void,
) -> c_int {
    let Some(state) = (unsafe { state_mut(arg) }) else {
        return 0;
    };
    if buf.is_null() || consumed.is_null() {
        state.failure = Some("OpenSSL supplied an invalid QUIC send buffer".into());
        return 0;
    }
    let output = &mut state.outgoing[state.write_level as usize];
    if output.len().saturating_add(len) > MAX_CRYPTO_BUFFER {
        state.failure = Some("OpenSSL QUIC CRYPTO output exceeded 64 KiB".into());
        return 0;
    }
    output.extend_from_slice(unsafe { std::slice::from_raw_parts(buf, len) });
    unsafe { *consumed = len };
    1
}

unsafe extern "C" fn crypto_recv(
    _ssl: *mut SSL,
    buf: *mut *const c_uchar,
    bytes_read: *mut usize,
    arg: *mut c_void,
) -> c_int {
    let Some(state) = (unsafe { state_mut(arg) }) else {
        return 0;
    };
    if buf.is_null() || bytes_read.is_null() {
        state.failure = Some("OpenSSL supplied invalid QUIC receive outputs".into());
        return 0;
    }
    if state.active_read.is_some() {
        state.failure =
            Some("OpenSSL requested a QUIC record before releasing the previous one".into());
        return 0;
    }
    let level = state.read_level;
    let input = &mut state.incoming[level as usize];
    let available = complete_message_len(input).unwrap_or(0);
    if available != 0 {
        // OpenSSL may retain this pointer until its release callback, including
        // across handshake calls. Further CRYPTO input must not relocate it.
        state.active_read = Some((
            level,
            input
                .drain(..available)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ));
    }
    unsafe {
        *bytes_read = available;
        *buf = state
            .active_read
            .as_ref()
            .map_or(std::ptr::null(), |(_, record)| record.as_ptr());
    }
    1
}

unsafe extern "C" fn crypto_release(_ssl: *mut SSL, bytes_read: usize, arg: *mut c_void) -> c_int {
    let Some(state) = (unsafe { state_mut(arg) }) else {
        return 0;
    };
    let Some((_, record)) = state.active_read.as_ref() else {
        state.failure = Some("OpenSSL released an inactive QUIC record".into());
        return 0;
    };
    if record.len() != bytes_read {
        state.failure = Some("OpenSSL released a partial QUIC record".into());
        return 0;
    }
    state.active_read.take();
    1
}

unsafe extern "C" fn yield_secret(
    ssl: *mut SSL,
    raw_level: c_uint,
    direction: c_int,
    secret: *const c_uchar,
    secret_len: usize,
    arg: *mut c_void,
) -> c_int {
    let Some(state) = (unsafe { state_mut(arg) }) else {
        return 0;
    };
    let Some(level) = Level::from_raw(raw_level) else {
        state.failure = Some("OpenSSL yielded an unknown QUIC protection level".into());
        return 0;
    };
    let Some(suite) = (unsafe { ffi::cipher_name(ssl) }).and_then(CipherSuite::from_name) else {
        state.failure =
            Some("OpenSSL yielded a secret for an unsupported QUIC cipher suite".into());
        return 0;
    };
    if level == Level::Initial || secret.is_null() || secret_len != suite.secret_len() {
        state.failure = Some("OpenSSL yielded unsupported QUIC secret material".into());
        return 0;
    }
    let suite_slot = &mut state.suites[level as usize];
    if suite_slot.is_some_and(|old| old != suite) {
        state.failure =
            Some("OpenSSL changed QUIC cipher suite within one protection level".into());
        return 0;
    }
    *suite_slot = Some(suite);
    let direction = match direction {
        0 => DIR_READ,
        1 => DIR_WRITE,
        _ => {
            state.failure = Some("OpenSSL yielded an unknown QUIC secret direction".into());
            return 0;
        }
    };
    let secret = unsafe { std::slice::from_raw_parts(secret, secret_len) };
    let slot = &mut state.secrets[level as usize][direction];
    if slot.as_deref().is_some_and(|old| old != secret) {
        state.failure = Some("OpenSSL replaced a QUIC traffic secret".into());
        return 0;
    }
    *slot = Some(secret.to_vec());
    if direction == DIR_READ {
        state.read_level = level;
    } else {
        state.write_level = level;
    }
    1
}

unsafe extern "C" fn got_transport_params(
    _ssl: *mut SSL,
    params: *const c_uchar,
    len: usize,
    arg: *mut c_void,
) -> c_int {
    let Some(state) = (unsafe { state_mut(arg) }) else {
        return 0;
    };
    if params.is_null() || len == 0 || len > MAX_CRYPTO_BUFFER {
        state.failure = Some("OpenSSL supplied invalid QUIC transport parameters".into());
        return 0;
    }
    let params = unsafe { std::slice::from_raw_parts(params, len) };
    if let Some(previous) = &state.peer_transport_params {
        return i32::from(previous.as_slice() == params);
    }
    state.peer_transport_params = Some(params.to_vec());
    1
}

unsafe extern "C" fn alert(_ssl: *mut SSL, alert_code: c_uchar, arg: *mut c_void) -> c_int {
    let Some(state) = (unsafe { state_mut(arg) }) else {
        return 0;
    };
    state.alert = Some(alert_code);
    1
}

unsafe fn state_mut<'a>(arg: *mut c_void) -> Option<&'a mut CallbackState> {
    unsafe { arg.cast::<CallbackState>().as_mut() }
}

fn complete_message_len(input: &[u8]) -> Option<usize> {
    let header = input.get(..4)?;
    let len =
        (usize::from(header[1]) << 16) | (usize::from(header[2]) << 8) | usize::from(header[3]);
    let total = 4usize.checked_add(len)?;
    (input.len() >= total).then_some(total)
}

#[cfg(test)]
#[path = "../../../../tests/tls/openssl/quic/callbacks.rs"]
mod tests;
