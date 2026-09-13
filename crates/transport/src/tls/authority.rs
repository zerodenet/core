//! Runtime certificate issuance from configured certificate authorities.
use super::{
    certificates::{load_certs, load_private_key},
    config::invalid,
};
use rcgen::{
    CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::{pki_types::PrivateKeyDer, sign::CertifiedKey};
use std::{
    io,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const CERTIFICATE_LIFETIME_SECS: i64 = 60 * 60;
pub(super) const EXPIRY_MARGIN_SECS: i64 = 2 * 60;

#[derive(Debug)]
pub(super) struct Authority {
    certificates: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    build_chain: bool,
    not_before: i64,
    not_after: i64,
}

#[derive(Debug)]
pub(super) struct IssuedCertificate {
    pub(super) key: Arc<CertifiedKey>,
    pub(super) not_after: i64,
}

#[derive(Debug)]
pub(super) struct IssuedCertificateMaterial {
    pub(super) certificates: Vec<Vec<u8>>,
    pub(super) private_key: Vec<u8>,
    pub(super) not_after: i64,
}

pub(super) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub(super) fn expires_soon(not_after: i64, current_time: i64) -> bool {
    not_after <= current_time.saturating_add(EXPIRY_MARGIN_SECS)
}

impl Authority {
    pub(super) fn load(
        files: &zero_traits::TlsCertificateFiles,
        provider: &rustls::crypto::CryptoProvider,
    ) -> io::Result<Self> {
        let result = Self::load_for_issuance(files)?;
        CertifiedKey::from_der(
            result.certificates.clone(),
            result.key.clone_key(),
            provider,
        )
        .map_err(invalid)?;
        Ok(result)
    }

    pub(super) fn load_for_issuance(files: &zero_traits::TlsCertificateFiles) -> io::Result<Self> {
        let certificates = load_certs(files.cert_path.as_ref()).map_err(invalid)?;
        let key = load_private_key(files.key_path.as_ref()).map_err(invalid)?;

        // Validate both the key format and its relationship with the authority
        // certificate before replacing a last-known-good refresh value.
        let rcgen_key = KeyPair::try_from(&key).map_err(invalid)?;
        Issuer::from_ca_cert_der(&certificates[0], rcgen_key).map_err(invalid)?;

        let (_, parsed) =
            x509_parser::parse_x509_certificate(certificates[0].as_ref()).map_err(invalid)?;
        let not_before = parsed.validity().not_before.timestamp();
        let not_after = parsed.validity().not_after.timestamp();
        Ok(Self {
            certificates,
            key,
            build_chain: files.build_chain,
            not_before,
            not_after,
        })
    }

    pub(super) fn issue(
        &self,
        domain: &str,
        provider: &rustls::crypto::CryptoProvider,
    ) -> io::Result<IssuedCertificate> {
        let material = self.issue_material(domain)?;
        let certificates = material
            .certificates
            .into_iter()
            .map(rustls::pki_types::CertificateDer::from)
            .collect();
        let key = PrivateKeyDer::Pkcs8(material.private_key.into());
        let key = CertifiedKey::from_der(certificates, key, provider).map_err(invalid)?;
        Ok(IssuedCertificate {
            key: Arc::new(key),
            not_after: material.not_after,
        })
    }

    pub(super) fn issue_material(&self, domain: &str) -> io::Result<IssuedCertificateMaterial> {
        let current_time = now();
        let not_before = self
            .not_before
            .max(current_time.saturating_sub(CERTIFICATE_LIFETIME_SECS));
        let not_after = self
            .not_after
            .min(current_time.saturating_add(CERTIFICATE_LIFETIME_SECS));
        if not_after <= not_before || expires_soon(not_after, current_time) {
            return Err(invalid(
                "TLS issuing authority is expired or expires too soon",
            ));
        }

        let key = KeyPair::generate().map_err(invalid)?;
        let authority_key = KeyPair::try_from(&self.key).map_err(invalid)?;
        let issuer =
            Issuer::from_ca_cert_der(&self.certificates[0], authority_key).map_err(invalid)?;
        let mut parameters = CertificateParams::new(vec![domain.to_owned()]).map_err(invalid)?;
        parameters.distinguished_name = DistinguishedName::new();
        parameters
            .distinguished_name
            .push(DnType::CommonName, domain);
        parameters.not_before = x509_parser::time::ASN1Time::from_timestamp(not_before)
            .map_err(invalid)?
            .to_datetime();
        parameters.not_after = x509_parser::time::ASN1Time::from_timestamp(not_after)
            .map_err(invalid)?
            .to_datetime();
        parameters.is_ca = IsCa::NoCa;
        parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        parameters.use_authority_key_identifier_extension = true;

        let certificate = parameters.signed_by(&key, &issuer).map_err(invalid)?;
        let mut certificates = vec![certificate.der().as_ref().to_vec()];
        if self.build_chain {
            certificates.extend(
                self.certificates
                    .iter()
                    .map(|certificate| certificate.as_ref().to_vec()),
            );
        }
        Ok(IssuedCertificateMaterial {
            certificates,
            private_key: key.serialize_der(),
            not_after,
        })
    }
}

#[cfg(test)]
#[path = "../../tests/tls/authority.rs"]
mod tests;
