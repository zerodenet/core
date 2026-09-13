//! RFC 7919 finite-field Diffie-Hellman using AWS-LC's built-in safe primes.
use super::RecordError;
use aws_lc_sys as lc;
use std::ptr::{self, NonNull};
use zeroize::Zeroizing;

pub struct Key {
    context: Context,
    public: Vec<u8>,
}
struct Context(NonNull<lc::DH>);
// SAFETY: no operation on the C context is exposed through a shared reference.
// Key generation has completed before publishing; completion consumes Key.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}
impl Drop for Context {
    fn drop(&mut self) {
        unsafe { lc::DH_free(self.0.as_ptr()) }
    }
}
struct Bn(NonNull<lc::BIGNUM>);
impl Drop for Bn {
    fn drop(&mut self) {
        unsafe { lc::BN_free(self.0.as_ptr()) }
    }
}
impl Key {
    pub fn new(bits: usize) -> Result<Self, RecordError> {
        let nid = match bits {
            2048 => lc::NID_ffdhe2048,
            3072 => lc::NID_ffdhe3072,
            4096 => lc::NID_ffdhe4096,
            8192 => lc::NID_ffdhe8192,
            _ => return Err(RecordError),
        };
        // SAFETY: the constructor returns an owned pointer or null. RAII frees it
        // on every exit. Built-in groups include q for public subgroup checking.
        let context = Context(NonNull::new(unsafe { lc::DH_new_by_nid(nid) }).ok_or(RecordError)?);
        if unsafe { lc::DH_generate_key(context.0.as_ptr()) } != 1 {
            return Err(RecordError);
        }
        let mut public_bn = ptr::null();
        unsafe {
            lc::DH_get0_key(context.0.as_ptr(), &mut public_bn, ptr::null_mut());
        }
        if public_bn.is_null() {
            return Err(RecordError);
        }
        let mut public = vec![0; bits / 8];
        // SAFETY: output capacity is the exact group modulus size. Padding
        // permits TLS 1.3 use; TLS 1.2 also allows this public-key encoding.
        if unsafe { lc::BN_bn2bin_padded(public.as_mut_ptr(), public.len(), public_bn) } != 1 {
            return Err(RecordError);
        }
        Ok(Self { context, public })
    }
    pub fn public_key(&self) -> &[u8] {
        &self.public
    }
    pub fn complete(self, peer: &[u8]) -> Result<Zeroizing<Vec<u8>>, RecordError> {
        if peer.is_empty() || peer.len() > self.public.len() {
            return Err(RecordError);
        }
        // SAFETY: pointer/length describe peer; null creates a new owned BN.
        let peer =
            Bn(
                NonNull::new(unsafe { lc::BN_bin2bn(peer.as_ptr(), peer.len(), ptr::null_mut()) })
                    .ok_or(RecordError)?,
            );
        let mut flags = 0;
        if unsafe { lc::DH_check_pub_key(self.context.0.as_ptr(), peer.0.as_ptr(), &mut flags) }
            != 1
            || flags != 0
        {
            return Err(RecordError);
        }
        let mut secret = Zeroizing::new(vec![0; self.public.len()]);
        // SAFETY: output capacity equals DH_size; context and peer are valid,
        // exclusively owned objects. The context is consumed after this call.
        let n = unsafe {
            lc::DH_compute_key_padded(
                secret.as_mut_ptr(),
                peer.0.as_ptr(),
                self.context.0.as_ptr(),
            )
        };
        if n <= 0 || n as usize != secret.len() {
            return Err(RecordError);
        }
        Ok(secret)
    }
}
