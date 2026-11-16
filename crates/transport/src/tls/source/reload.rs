//! Same original snapshots, protocol policy and leaf-bound OCSP publication.
use super::*;
use zero_platform_tokio::contract::scope::WeakTaskRegistrar;
fn invalid(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component).with_source(source)
}
fn authority(
    material: &FileMaterial,
    provider: &rustls::crypto::CryptoProvider,
) -> io::Result<Arc<super::super::authority::Authority>> {
    let value = super::super::authority::Authority::from_material(
        &material.files,
        &material.cert,
        &material.key,
    )?;
    let certs = super::super::certificates::parse_certs(
        std::io::Cursor::new(&material.cert),
        material.files.cert_path.as_ref(),
    )?;
    let key = super::super::certificates::parse_private_key(
        &material.key,
        material.files.key_path.as_ref(),
    )?;
    rustls::sign::CertifiedKey::from_der(certs, key, provider).map_err(io::Error::other)?;
    Ok(Arc::new(value))
}
impl PreparedServer {
    pub(super) async fn load_initial(
        &self,
        resources: &dyn ResourceAccess,
    ) -> Result<(), ChannelError> {
        let mut material = Vec::new();
        for files in &self.files {
            material.push(self.material(resources, files).await?);
        }
        match &self.backend {
            Backend::Rustls {
                certificates,
                authorities,
                provider,
                ..
            } => {
                let mut certs = Vec::new();
                let mut cas = Vec::new();
                for material in &material {
                    if material.files.usage == zero_traits::TlsCertificateUsage::AuthorityIssue {
                        cas.push(authority(material, provider).map_err(invalid)?);
                    } else {
                        certs.push(server::from_material(material, provider).map_err(invalid)?);
                    }
                }
                certificates.publish(certs);
                authorities.publish(cas);
            }
            Backend::OpenSsl(context) => context
                .publish_material(&self.profile, &material)
                .map_err(invalid)?,
        }
        Ok(())
    }
    async fn reload_rustls(
        &self,
        resources: &dyn ResourceAccess,
        index: usize,
        stop: &CancellationToken,
    ) -> Result<(), ChannelError> {
        let Backend::Rustls {
            certificates,
            authorities,
            provider,
            ..
        } = &self.backend
        else {
            unreachable!()
        };
        let files = &self.files[index];
        let material = self.material(resources, files).await?;
        let is_ca = files.usage == zero_traits::TlsCertificateUsage::AuthorityIssue;
        let slot = self.files[..index]
            .iter()
            .filter(|file| {
                (file.usage == zero_traits::TlsCertificateUsage::AuthorityIssue) == is_ca
            })
            .count();
        if is_ca {
            let value = authority(&material, provider).map_err(invalid)?;
            let mut next = authorities.current().as_ref().clone();
            next[slot] = value;
            if !stop.is_cancelled() {
                authorities.publish(next);
            }
        } else {
            let mut value = server::from_material(&material, provider).map_err(invalid)?;
            let mut next = certificates.current().as_ref().clone();
            let previous = &next[slot];
            if files.ocsp_path.is_none() && value.key.cert == previous.key.cert {
                Arc::make_mut(&mut value.key).ocsp = previous.key.ocsp.clone();
            }
            next[slot] = value;
            if !stop.is_cancelled() {
                certificates.publish(next);
            }
        }
        Ok(())
    }
    pub(super) async fn refresh(
        &self,
        resources: &dyn ResourceAccess,
        driver: &TaskJoinWitness,
        tasks: &WeakTaskRegistrar,
        stop: &CancellationToken,
    ) {
        let changed = tokio::sync::Notify::new();
        // These are continuations inside this original registered worker, not
        // new tasks. A pending OCSP response must not block material rotation.
        tokio::join!(
            self.refresh_files(resources, stop, &changed),
            self.refresh_staples(resources, driver, tasks, stop, &changed),
        );
    }
    async fn refresh_files(
        &self,
        resources: &dyn ResourceAccess,
        stop: &CancellationToken,
        changed: &tokio::sync::Notify,
    ) {
        let reload =
            std::time::Duration::from_secs(if self.profile.options.reload_interval_secs == 0 {
                3600
            } else {
                self.profile.options.reload_interval_secs
            });
        let intervals: Vec<_> = self
            .files
            .iter()
            .map(|files| {
                if matches!(self.backend, Backend::Rustls { .. })
                    && files.usage != zero_traits::TlsCertificateUsage::AuthorityIssue
                    && files.ocsp_stapling_secs != 0
                {
                    std::time::Duration::from_secs(files.ocsp_stapling_secs)
                } else {
                    reload
                }
            })
            .collect();
        let now = tokio::time::Instant::now();
        let mut due: Vec<_> = intervals.iter().map(|interval| now + *interval).collect();
        loop {
            let next = due
                .iter()
                .copied()
                .min()
                .expect("nonempty actual TLS files");
            tokio::select! {biased;_=stop.cancelled()=>return,_=tokio::time::sleep_until(next)=>{}}
            let now = tokio::time::Instant::now();
            if matches!(self.backend, Backend::OpenSsl(_)) {
                if let Err(error) = self.load_initial(resources).await {
                    tracing::warn!(%error,"hosted OpenSSL TLS refresh retained its previous context");
                } else {
                    changed.notify_one();
                }
                for next in &mut due {
                    *next = tokio::time::Instant::now() + reload;
                }
            } else {
                for index in 0..due.len() {
                    if due[index] <= now {
                        if let Err(error) = self.reload_rustls(resources, index, stop).await {
                            tracing::warn!(%error,"hosted TLS refresh retained its previous certificate/authority");
                        }
                        due[index] = tokio::time::Instant::now() + intervals[index];
                    }
                }
            }
        }
    }
    async fn refresh_staples(
        &self,
        resources: &dyn ResourceAccess,
        driver: &TaskJoinWitness,
        tasks: &WeakTaskRegistrar,
        stop: &CancellationToken,
        changed: &tokio::sync::Notify,
    ) {
        let mut due: Vec<_> = self
            .files
            .iter()
            .map(|files| {
                (files.usage != zero_traits::TlsCertificateUsage::AuthorityIssue
                    && files.ocsp_stapling_secs != 0
                    && files.ocsp_path.is_none())
                .then(tokio::time::Instant::now)
            })
            .collect();
        let client = super::super::ocsp::Client::Hosted {
            resources,
            driver,
            tasks,
        };
        loop {
            let next = due.iter().flatten().copied().min();
            tokio::select! {
                biased;
                _=stop.cancelled()=>return,
                _=changed.notified()=>{
                    for next in due.iter_mut().flatten() { *next = tokio::time::Instant::now(); }
                },
                _=async { match next {Some(next)=>tokio::time::sleep_until(next).await,None=>std::future::pending().await} }=>{},
            }
            for index in 0..due.len() {
                if due[index].is_some_and(|next| next <= tokio::time::Instant::now()) {
                    if let Err(error) = self.refresh_ocsp(&client, index, stop).await {
                        tracing::warn!(%error,"hosted TLS OCSP refresh retained its previous same-leaf staple");
                    }
                    due[index] = Some(
                        tokio::time::Instant::now()
                            + std::time::Duration::from_secs(self.files[index].ocsp_stapling_secs),
                    );
                }
            }
        }
    }
    async fn refresh_ocsp(
        &self,
        client: &super::super::ocsp::Client<'_>,
        index: usize,
        stop: &CancellationToken,
    ) -> io::Result<()> {
        let slot = self.files[..index]
            .iter()
            .filter(|files| files.usage != zero_traits::TlsCertificateUsage::AuthorityIssue)
            .count();
        match &self.backend {
            Backend::Rustls { certificates, .. } => {
                let current = certificates.current();
                let original = &current[slot];
                let staple = super::super::ocsp::retrieve_with(client, &original.key.cert).await?;
                let mut next = certificates.current().as_ref().clone();
                if !stop.is_cancelled() && next[slot].key.cert == original.key.cert {
                    Arc::make_mut(&mut next[slot].key).ocsp = Some(staple);
                    certificates.publish(next);
                }
            }
            Backend::OpenSsl(context) => {
                let targets = context.owned_ocsp_targets();
                let target = targets
                    .iter()
                    .find(|target| {
                        target.source == std::path::Path::new(&self.files[index].cert_path)
                    })
                    .ok_or_else(|| io::Error::other("original OCSP target is absent"))?;
                let staple =
                    super::super::ocsp::retrieve_with(client, &target.certificates).await?;
                ::openssl::ocsp::OcspResponse::from_der(&staple).map_err(io::Error::other)?;
                if !stop.is_cancelled() {
                    *target.staple.write().unwrap_or_else(|e| e.into_inner()) = Some(staple);
                }
            }
        }
        Ok(())
    }
}
