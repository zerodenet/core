use super::*;
use aws_lc_rs::agreement::{self, PrivateKey, UnparsedPublicKey, ECDH_P521};

#[derive(Debug)]
pub(super) struct P521;

impl SupportedKxGroup for P521 {
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, Error> {
        let private = PrivateKey::generate(&ECDH_P521)
            .map_err(|_| Error::General("P-521 key generation failed".into()))?;
        let public = private
            .compute_public_key()
            .map_err(|_| Error::General("P-521 public key derivation failed".into()))?
            .as_ref()
            .to_vec();
        Ok(Box::new(Exchange { private, public }))
    }
    fn name(&self) -> NamedGroup {
        NamedGroup::secp521r1
    }
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }
}

struct Exchange {
    private: PrivateKey,
    public: Vec<u8>,
}
impl ActiveKeyExchange for Exchange {
    fn complete(self: Box<Self>, share: &[u8]) -> Result<SharedSecret, Error> {
        // TLS permits only uncompressed SEC1 points, including fixed-width x/y.
        if share.len() != 133 || share[0] != 4 {
            return Err(invalid_share());
        }
        agreement::agree(
            &self.private,
            UnparsedPublicKey::new(&ECDH_P521, share),
            invalid_share(),
            |secret| Ok(SharedSecret::from(secret)),
        )
    }
    fn pub_key(&self) -> &[u8] {
        &self.public
    }
    fn group(&self) -> NamedGroup {
        NamedGroup::secp521r1
    }
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }
}
