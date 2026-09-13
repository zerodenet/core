//! Additional key shares retain their real private state, including ML-KEM.
use rustls::crypto::ActiveKeyExchange;
use std::{io, sync::Arc};
#[derive(Default)]
pub struct KeyShares(
    Vec<(u16, Box<dyn ActiveKeyExchange>)>,
    bool,
    Option<Arc<rustls::crypto::CryptoProvider>>,
);
impl KeyShares {
    pub fn for_profile(profile: super::ClientHelloProfile) -> Self {
        Self(
            Vec::new(),
            super::wire::resolve(profile) == super::ClientHelloProfile::Firefox148,
            None,
        )
    }
    pub fn with_provider(
        mut self,
        provider: Option<Arc<rustls::crypto::CryptoProvider>>,
        custom_groups: bool,
    ) -> Self {
        self.2 = provider;
        if custom_groups {
            self.1 = false;
        }
        self
    }
    pub fn reset_for_retry(&mut self) {
        self.0.clear();
        self.1 = false;
    }
    pub fn reuses_x25519(&self) -> bool {
        self.1
    }

    pub fn offer(&mut self, group: u16, x25519: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if group == 29 {
            if self.1 {
                let public = self
                    .0
                    .iter()
                    .find(|(g, _)| *g == 4588)
                    .and_then(|(_, k)| k.hybrid_component())
                    .ok_or_else(|| io::Error::other("missing hybrid component"))?
                    .1;
                return Ok(Some(public.to_vec()));
            }
            return Ok(Some(x25519.to_vec()));
        }
        let provider = self
            .2
            .clone()
            .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
        let algorithm = provider
            .kx_groups
            .iter()
            .find(|g| u16::from(g.name()) == group)
            .ok_or_else(|| {
                io::Error::other(format!("unsupported ClientHello key share {group}"))
            })?;
        let key = algorithm.start().map_err(io::Error::other)?;
        let public = key.pub_key().to_vec();
        self.0.push((group, key));
        Ok(Some(public))
    }
    pub fn complete(&mut self, group: u16, public: &[u8]) -> io::Result<Vec<u8>> {
        if group == 29 && self.1 {
            let i = self
                .0
                .iter()
                .position(|(g, _)| *g == 4588)
                .ok_or_else(|| io::Error::other("missing hybrid key"))?;
            return self
                .0
                .remove(i)
                .1
                .complete_hybrid_component(public)
                .map(|s| s.secret_bytes().to_vec())
                .map_err(io::Error::other);
        }

        let i = self
            .0
            .iter()
            .position(|(g, _)| *g == group)
            .ok_or_else(|| io::Error::other("server selected an unoffered key share"))?;
        self.0
            .remove(i)
            .1
            .complete(public)
            .map(|s| s.secret_bytes().to_vec())
            .map_err(io::Error::other)
    }
}
/// Extract the selected group and public key from a ServerHello record.
pub fn selected(record: &[u8]) -> io::Result<(u16, Vec<u8>)> {
    use crate::buf_reader::BufReader;
    let mut r = BufReader::new(record);
    r.skip(5 + 4 + 2 + 32)?;
    let n = r.read_u8()? as usize;
    r.skip(n + 3)?;
    let n = r.read_u16_be()? as usize;
    let mut r = BufReader::new(r.read_slice(n)?);
    while !r.is_consumed() {
        let k = r.read_u16_be()?;
        let n = r.read_u16_be()? as usize;
        let data = r.read_slice(n)?;
        if k == 51 {
            let mut r = BufReader::new(data);
            let group = r.read_u16_be()?;
            let n = r.read_u16_be()? as usize;
            let key = r.read_slice(n)?.to_vec();
            if !r.is_consumed() {
                return Err(io::Error::other("invalid ServerHello key share"));
            }
            return Ok((group, key));
        }
    }
    Err(io::Error::other("missing ServerHello key share"))
}
