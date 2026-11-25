//! TLS 1.2 MAC-then-encrypt records using AWS-LC's dedicated constant-time
//! TLS CBC implementation, with a bounded constant-work SHA256 fallback.
pub mod dh;
mod sha256;
use aws_lc_sys as lc;
use zeroize::Zeroizing;

#[derive(Clone, Copy, Debug)]
pub enum Algorithm {
    Aes128Sha1,
    Aes256Sha1,
    Aes128Sha256,
    Aes256Sha384,
    Aes256Sha256,
    TripleDesSha1,
}
impl Algorithm {
    pub fn key_len(self) -> usize {
        match self {
            Self::Aes128Sha1 | Self::Aes128Sha256 => 16,
            Self::TripleDesSha1 => 24,
            _ => 32,
        }
    }
    pub fn mac_len(self) -> usize {
        match self {
            Self::Aes128Sha256 | Self::Aes256Sha256 => 32,
            Self::Aes256Sha384 => 48,
            _ => 20,
        }
    }
    pub fn block_len(self) -> usize {
        if matches!(self, Self::TripleDesSha1) {
            8
        } else {
            16
        }
    }
    fn aead(self) -> *const lc::EVP_AEAD {
        // SAFETY: these return immutable static algorithm descriptors.
        unsafe {
            match self {
                Self::Aes256Sha256 => unreachable!("SHA256 fallback has no AEAD descriptor"),
                Self::Aes128Sha1 => lc::EVP_aead_aes_128_cbc_sha1_tls(),
                Self::Aes256Sha1 => lc::EVP_aead_aes_256_cbc_sha1_tls(),
                Self::Aes128Sha256 => lc::EVP_aead_aes_128_cbc_sha256_tls(),
                Self::Aes256Sha384 => lc::EVP_aead_aes_256_cbc_sha384_tls(),
                Self::TripleDesSha1 => lc::EVP_aead_des_ede3_cbc_sha1_tls(),
            }
        }
    }
}
#[derive(Debug)]
pub struct RecordError;

/// Key bytes are wiped on drop; no mutable FFI context is shared between records.
/// Each operation owns one context, making this type naturally Send + Sync.
pub struct RecordKey {
    algorithm: Algorithm,
    key: Zeroizing<Vec<u8>>,
}
impl RecordKey {
    pub fn new(
        algorithm: Algorithm,
        cipher_key: &[u8],
        mac_key: &[u8],
    ) -> Result<Self, RecordError> {
        if cipher_key.len() != algorithm.key_len() || mac_key.len() != algorithm.mac_len() {
            return Err(RecordError);
        }
        let mut key = Zeroizing::new(Vec::with_capacity(mac_key.len() + cipher_key.len()));
        key.extend_from_slice(mac_key);
        key.extend_from_slice(cipher_key);
        Ok(Self { algorithm, key })
    }
    pub fn encrypted_len(&self, plain_len: usize) -> usize {
        let block = self.algorithm.block_len();
        block + (plain_len + self.algorithm.mac_len() + block) / block * block
    }
    /// Produces an explicit unpredictable IV followed by the authenticated ciphertext.
    pub fn seal(&self, aad: &[u8; 11], plain: &[u8]) -> Result<Vec<u8>, RecordError> {
        if plain.len() > 16384 {
            return Err(RecordError);
        }
        if matches!(self.algorithm, Algorithm::Aes256Sha256) {
            return sha256::seal(&self.key, aad, plain);
        }
        let n = self.algorithm.block_len();
        let mut iv = [0u8; 16];
        // SAFETY: the output slice has at least n writable bytes.
        if unsafe { lc::RAND_bytes(iv.as_mut_ptr(), n) } != 1 {
            return Err(RecordError);
        }
        let mut out = vec![0u8; self.encrypted_len(plain.len())];
        out[..n].copy_from_slice(&iv[..n]);
        let ctx = Context::new(self, true)?;
        let mut length = 0;
        // SAFETY: input and output buffers are disjoint, all pointer lengths match
        // their allocations, and this context is exclusively owned by this call.
        let ok = unsafe {
            lc::EVP_AEAD_CTX_seal(
                &ctx.0,
                out[n..].as_mut_ptr(),
                &mut length,
                out.len() - n,
                iv.as_ptr(),
                n,
                plain.as_ptr(),
                plain.len(),
                aad.as_ptr(),
                aad.len(),
            )
        };
        if ok != 1 || length != out.len() - n {
            return Err(RecordError);
        }
        Ok(out)
    }
    /// Every MAC/padding failure has the same result. AWS-LC verifies both in constant time.
    pub fn open(&self, aad: &[u8; 11], record: &[u8]) -> Result<Zeroizing<Vec<u8>>, RecordError> {
        let n = self.algorithm.block_len();
        if record.len() < 2 * n || record.len() > 16384 + 2048 {
            return Err(RecordError);
        }
        if matches!(self.algorithm, Algorithm::Aes256Sha256) {
            return sha256::open(&self.key, aad, record);
        }
        let ctx = Context::new(self, false)?;
        let mut out = Zeroizing::new(vec![0u8; record.len() - n]);
        let mut length = 0;
        // SAFETY: all buffers are valid and disjoint; the context is local.
        let ok = unsafe {
            lc::EVP_AEAD_CTX_open(
                &ctx.0,
                out.as_mut_ptr(),
                &mut length,
                out.len(),
                record.as_ptr(),
                n,
                record[n..].as_ptr(),
                record.len() - n,
                aad.as_ptr(),
                aad.len(),
            )
        };
        if ok != 1 || length > 16384 {
            return Err(RecordError);
        }
        out.truncate(length);
        Ok(out)
    }
}
struct Context(lc::EVP_AEAD_CTX);
impl Context {
    fn new(key: &RecordKey, seal: bool) -> Result<Self, RecordError> {
        let mut storage = std::mem::MaybeUninit::uninit();
        // SAFETY: EVP_AEAD_CTX_zero initializes the entire C context; cleanup is
        // valid on both failed and successful initialization. No alias escapes.
        let mut ctx = unsafe {
            lc::EVP_AEAD_CTX_zero(storage.as_mut_ptr());
            Self(storage.assume_init())
        };
        let direction = if seal {
            lc::evp_aead_direction_t_evp_aead_seal
        } else {
            lc::evp_aead_direction_t_evp_aead_open
        };
        let ok = unsafe {
            lc::EVP_AEAD_CTX_init_with_direction(
                &mut ctx.0,
                key.algorithm.aead(),
                key.key.as_ptr(),
                key.key.len(),
                0,
                direction,
            )
        };
        if ok != 1 {
            return Err(RecordError);
        }
        Ok(ctx)
    }
}
impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: context was initialized or zeroed and is exclusively owned.
        unsafe { lc::EVP_AEAD_CTX_cleanup(&mut self.0) }
    }
}
