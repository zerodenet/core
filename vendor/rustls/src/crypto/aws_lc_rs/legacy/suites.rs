use super::super::{
    hash,
    tls12::{Tls12Prf, AES128_GCM, AES256_GCM, TLS12_ECDSA_SCHEMES, TLS12_RSA_SCHEMES},
};
use super::records::Cbc;
use crate::{
    crypto::KeyExchangeAlgorithm, suites::CipherSuiteCommon, CipherSuite, SupportedCipherSuite,
    Tls12CipherSuite,
};
use rustls_legacy_crypto::Algorithm;
macro_rules! suite {
    ($name:ident, $kx:ident, $hash:ident, $prf:ident, $sign:ident, $record:expr, $limit:expr) => {
        #[doc = concat!("Opt-in TLS 1.2 compatibility suite `", stringify!($name), "`.")]
        pub static $name: SupportedCipherSuite = SupportedCipherSuite::Tls12(&Tls12CipherSuite {
            common: CipherSuiteCommon {
                suite: CipherSuite::$name,
                hash_provider: &hash::$hash,
                confidentiality_limit: $limit,
            },
            kx: KeyExchangeAlgorithm::$kx,
            sign: $sign,
            aead_alg: $record,
            prf_provider: &Tls12Prf(&aws_lc_rs::tls_prf::$prf),
        });
    };
}
suite!(
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_ECDSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha1),
    1 << 24
);
suite!(
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_ECDSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha1),
    1 << 24
);
suite!(
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha1),
    1 << 24
);
suite!(
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha1),
    1 << 24
);
suite!(
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_ECDSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha256),
    1 << 24
);
suite!(
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
    ECDHE,
    SHA384,
    P_SHA384,
    TLS12_ECDSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha384),
    1 << 24
);
suite!(
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha256),
    1 << 24
);
suite!(
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,
    ECDHE,
    SHA384,
    P_SHA384,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha384),
    1 << 24
);
suite!(
    TLS_RSA_WITH_AES_128_CBC_SHA,
    RSA,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha1),
    1 << 24
);
suite!(
    TLS_RSA_WITH_AES_256_CBC_SHA,
    RSA,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha1),
    1 << 24
);
suite!(
    TLS_RSA_WITH_AES_128_CBC_SHA256,
    RSA,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha256),
    1 << 24
);
suite!(
    TLS_RSA_WITH_AES_128_GCM_SHA256,
    RSA,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &AES128_GCM,
    1 << 24
);
suite!(
    TLS_RSA_WITH_AES_256_GCM_SHA384,
    RSA,
    SHA384,
    P_SHA384,
    TLS12_RSA_SCHEMES,
    &AES256_GCM,
    1 << 24
);
suite!(
    TLS_RSA_WITH_3DES_EDE_CBC_SHA,
    RSA,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::TripleDesSha1),
    1 << 9
);
suite!(
    TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::TripleDesSha1),
    1 << 9
);
suite!(
    TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA,
    ECDHE,
    SHA256,
    P_SHA256,
    TLS12_ECDSA_SCHEMES,
    &Cbc(Algorithm::TripleDesSha1),
    1 << 9
);
suite!(
    TLS_RSA_WITH_AES_256_CBC_SHA256,
    RSA,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha256),
    1 << 24
);
suite!(
    TLS_DHE_RSA_WITH_AES_128_CBC_SHA,
    DHE,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes128Sha1),
    1 << 24
);
suite!(
    TLS_DHE_RSA_WITH_AES_256_CBC_SHA,
    DHE,
    SHA256,
    P_SHA256,
    TLS12_RSA_SCHEMES,
    &Cbc(Algorithm::Aes256Sha1),
    1 << 24
);

/// Additional suites, not installed in the default provider.
pub static CIPHER_SUITES: &[SupportedCipherSuite] = &[
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
    TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
    TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
    TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,
    TLS_RSA_WITH_AES_128_CBC_SHA,
    TLS_RSA_WITH_AES_256_CBC_SHA,
    TLS_RSA_WITH_AES_128_CBC_SHA256,
    TLS_RSA_WITH_AES_128_GCM_SHA256,
    TLS_RSA_WITH_AES_256_GCM_SHA384,
    TLS_RSA_WITH_3DES_EDE_CBC_SHA,
    TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
    TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA,
    TLS_RSA_WITH_AES_256_CBC_SHA256,
    TLS_DHE_RSA_WITH_AES_128_CBC_SHA,
    TLS_DHE_RSA_WITH_AES_256_CBC_SHA,
];
