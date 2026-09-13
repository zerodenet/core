use std::ffi::{c_int, c_uchar, c_uint, c_void};

use openssl_sys::SSL;

pub(super) const SSL_ERROR_SSL: c_int = 1;
pub(super) const SSL_ERROR_WANT_READ: c_int = 2;
pub(super) const SSL_ERROR_WANT_WRITE: c_int = 3;

pub(super) const OSSL_FUNC_SSL_QUIC_TLS_CRYPTO_SEND: c_int = 2001;
pub(super) const OSSL_FUNC_SSL_QUIC_TLS_CRYPTO_RECV_RCD: c_int = 2002;
pub(super) const OSSL_FUNC_SSL_QUIC_TLS_CRYPTO_RELEASE_RCD: c_int = 2003;
pub(super) const OSSL_FUNC_SSL_QUIC_TLS_YIELD_SECRET: c_int = 2004;
pub(super) const OSSL_FUNC_SSL_QUIC_TLS_GOT_TRANSPORT_PARAMS: c_int = 2005;
pub(super) const OSSL_FUNC_SSL_QUIC_TLS_ALERT: c_int = 2006;

pub(super) const TLSEXT_NAMETYPE_HOST_NAME: c_int = 0;

#[repr(C)]
pub(super) struct OsslDispatch {
    pub(super) function_id: c_int,
    pub(super) function: *const c_void,
}

unsafe extern "C" {
    pub(super) fn SSL_set_quic_tls_cbs(
        ssl: *mut SSL,
        dispatch: *const OsslDispatch,
        arg: *mut c_void,
    ) -> c_int;

    pub(super) fn SSL_set_quic_tls_transport_params(
        ssl: *mut SSL,
        params: *const u8,
        params_len: usize,
    ) -> c_int;

}

pub(super) unsafe fn selected_alpn(ssl: *const SSL) -> Vec<u8> {
    let mut data = std::ptr::null();
    let mut len: c_uint = 0;
    unsafe { openssl_sys::SSL_get0_alpn_selected(ssl, &mut data, &mut len) };
    if data.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len as usize) }.to_vec()
    }
}

pub(super) unsafe fn server_name(ssl: *const SSL) -> Option<String> {
    let name = unsafe { openssl_sys::SSL_get_servername(ssl, TLSEXT_NAMETYPE_HOST_NAME) };
    if name.is_null() {
        return None;
    }
    unsafe { std::ffi::CStr::from_ptr(name) }
        .to_str()
        .ok()
        .map(str::to_owned)
}

pub(super) unsafe fn cipher_name(ssl: *const SSL) -> Option<&'static str> {
    let cipher = unsafe { openssl_sys::SSL_get_current_cipher(ssl) };
    if cipher.is_null() {
        return None;
    }
    let name = unsafe { openssl_sys::SSL_CIPHER_get_name(cipher) };
    if name.is_null() {
        return None;
    }
    unsafe { std::ffi::CStr::from_ptr(name) }.to_str().ok()
}

pub(super) unsafe fn export_keying_material(
    ssl: *mut SSL,
    output: &mut [u8],
    label: &[u8],
    context: &[u8],
) -> bool {
    unsafe {
        openssl_sys::SSL_export_keying_material(
            ssl,
            output.as_mut_ptr(),
            output.len(),
            label.as_ptr().cast(),
            label.len(),
            context.as_ptr(),
            context.len(),
            1,
        ) == 1
    }
}

pub(super) type CryptoSend =
    unsafe extern "C" fn(*mut SSL, *const c_uchar, usize, *mut usize, *mut c_void) -> c_int;
pub(super) type CryptoRecv =
    unsafe extern "C" fn(*mut SSL, *mut *const c_uchar, *mut usize, *mut c_void) -> c_int;
pub(super) type CryptoRelease = unsafe extern "C" fn(*mut SSL, usize, *mut c_void) -> c_int;
pub(super) type YieldSecret =
    unsafe extern "C" fn(*mut SSL, c_uint, c_int, *const c_uchar, usize, *mut c_void) -> c_int;
pub(super) type GotTransportParams =
    unsafe extern "C" fn(*mut SSL, *const c_uchar, usize, *mut c_void) -> c_int;
pub(super) type Alert = unsafe extern "C" fn(*mut SSL, c_uchar, *mut c_void) -> c_int;

pub(super) fn dispatch_entry<T: Copy>(function_id: c_int, callback: T) -> OsslDispatch {
    assert_eq!(
        std::mem::size_of::<T>(),
        std::mem::size_of::<*const c_void>()
    );
    OsslDispatch {
        function_id,
        function: unsafe { std::mem::transmute_copy(&callback) },
    }
}
