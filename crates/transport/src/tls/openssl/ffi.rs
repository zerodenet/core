use core::ffi::{c_char, c_int, c_void};
use openssl_sys::{BIO, EVP_PKEY, SSL, SSL_CTX};

pub(super) type SecurityCallback = unsafe extern "C" fn(
    ssl: *const SSL,
    context: *const SSL_CTX,
    operation: c_int,
    bits: c_int,
    nid: c_int,
    other: *mut c_void,
    extra: *mut c_void,
) -> c_int;

#[repr(C)]
pub(super) struct EchStore {
    _private: [u8; 0],
}

unsafe extern "C" {
    pub(super) fn OSSL_ECHSTORE_new(libctx: *mut c_void, propq: *const c_char) -> *mut EchStore;
    pub(super) fn OSSL_ECHSTORE_free(store: *mut EchStore);
    pub(super) fn OSSL_ECHSTORE_set1_key_and_read_pem(
        store: *mut EchStore,
        private_key: *mut EVP_PKEY,
        input: *mut BIO,
        for_retry: c_int,
    ) -> c_int;
    pub(super) fn SSL_CTX_set1_echstore(context: *mut SSL_CTX, store: *mut EchStore) -> c_int;
    #[cfg(test)]
    pub(super) fn SSL_ech_get1_status(
        ssl: *mut SSL,
        inner_name: *mut *mut c_char,
        outer_name: *mut *mut c_char,
    ) -> c_int;
    pub(super) fn SSL_has_pending(ssl: *const SSL) -> c_int;
    pub(super) fn SSL_CTX_get_security_callback(
        context: *const SSL_CTX,
    ) -> Option<SecurityCallback>;
    pub(super) fn SSL_CTX_set_security_callback(
        context: *mut SSL_CTX,
        callback: Option<SecurityCallback>,
    );
    pub(super) fn SSL_CTX_get0_security_ex_data(context: *const SSL_CTX) -> *mut c_void;
    pub(super) fn SSL_CTX_set0_security_ex_data(context: *mut SSL_CTX, extra: *mut c_void);
}
