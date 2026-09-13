use bytes::Bytes;
use foreign_types::ForeignType;
use openssl::ssl::Ssl;
use quinn_proto::crypto::{
    ExportKeyingMaterialError, HeaderKey, KeyPair, Keys, PacketKey, Session,
};
use quinn_proto::transport_parameters::TransportParameters;
use quinn_proto::{ConnectionId, Side, TransportError, TransportErrorCode};
use std::any::Any;

use super::callbacks::{CallbackState, Level};
use super::keys::{CipherSuite, Version};
use super::{ffi, keys};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandshakeData {
    pub protocol: Option<Vec<u8>>,
    pub server_name: Option<String>,
}

pub(super) struct OpenSslQuicSession {
    ssl: Option<Ssl>,
    callbacks: Box<CallbackState>,
    local_transport_params: Vec<u8>,
    version: Version,
    side: Side,
    quinn_level: Level,
    handshake_data: Option<HandshakeData>,
    handshake_data_reported: bool,
    complete: bool,
    failure: Option<String>,
    next_local_secret: Option<Vec<u8>>,
    next_remote_secret: Option<Vec<u8>>,
    next_suite: Option<CipherSuite>,
}

impl OpenSslQuicSession {
    pub(super) fn new(
        ssl: std::io::Result<Ssl>,
        version: Version,
        side: Side,
        params: &TransportParameters,
    ) -> Self {
        let mut this = Self {
            ssl: ssl.ok(),
            callbacks: Box::new(CallbackState::default()),
            local_transport_params: Vec::new(),
            version,
            side,
            quinn_level: Level::Initial,
            handshake_data: None,
            handshake_data_reported: false,
            complete: false,
            failure: None,
            next_local_secret: None,
            next_remote_secret: None,
            next_suite: None,
        };
        if this.ssl.is_none() {
            this.failure = Some("failed to create OpenSSL QUIC server session".into());
            return this;
        }
        if let Err(error) = this.initialize(params) {
            this.failure = Some(error);
        }
        this
    }

    fn initialize(&mut self, params: &TransportParameters) -> Result<(), String> {
        let ssl = self.ssl.as_ref().unwrap().as_ptr();
        let dispatch = CallbackState::dispatch();
        params.write(&mut self.local_transport_params);
        unsafe {
            if self.side.is_server() {
                openssl_sys::SSL_set_accept_state(ssl);
            } else {
                openssl_sys::SSL_set_connect_state(ssl);
            }
            if ffi::SSL_set_quic_tls_cbs(
                ssl,
                dispatch.as_ptr(),
                (&mut *self.callbacks as *mut CallbackState).cast(),
            ) != 1
            {
                return Err("OpenSSL rejected the QUIC TLS callbacks".into());
            }
            if ffi::SSL_set_quic_tls_transport_params(
                ssl,
                self.local_transport_params.as_ptr(),
                self.local_transport_params.len(),
            ) != 1
            {
                return Err("OpenSSL rejected local QUIC transport parameters".into());
            }
        }
        Ok(())
    }

    fn drive(&mut self) -> Result<(), TransportError> {
        if let Some(reason) = self.failure.clone() {
            return Err(self.transport_error(reason));
        }
        if let Some(reason) = self.callbacks.failure().map(str::to_owned) {
            self.failure = Some(reason.clone());
            return Err(self.transport_error(reason));
        }
        let ssl = self.ssl.as_ref().unwrap().as_ptr();
        let result = unsafe { openssl_sys::SSL_do_handshake(ssl) };
        if result == 1 {
            self.complete = true;
            if self.callbacks.peer_transport_params().is_none() {
                let reason = "OpenSSL completed QUIC TLS without peer transport parameters";
                self.failure = Some(reason.into());
                return Err(self.transport_error(reason.into()));
            }
        } else {
            let error = unsafe { openssl_sys::SSL_get_error(ssl, result) };
            if error != ffi::SSL_ERROR_WANT_READ && error != ffi::SSL_ERROR_WANT_WRITE {
                let reason = if error == ffi::SSL_ERROR_SSL {
                    openssl::error::ErrorStack::get().to_string()
                } else {
                    format!("OpenSSL QUIC handshake failed with SSL error {error}")
                };
                self.failure = Some(reason.clone());
                return Err(self.transport_error(reason));
            }
        }
        if let Some(reason) = self.callbacks.failure().map(str::to_owned) {
            self.failure = Some(reason.clone());
            return Err(self.transport_error(reason));
        }
        self.refresh_handshake_data();
        Ok(())
    }

    fn refresh_handshake_data(&mut self) {
        let Some(ssl) = &self.ssl else {
            return;
        };
        let ssl = ssl.as_ptr();
        let protocol = unsafe { ffi::selected_alpn(ssl) };
        let server_name = unsafe { ffi::server_name(ssl) };
        if !protocol.is_empty() || server_name.is_some() || self.complete {
            self.handshake_data = Some(HandshakeData {
                protocol: (!protocol.is_empty()).then_some(protocol),
                server_name,
            });
        }
    }

    fn transport_error(&self, reason: String) -> TransportError {
        TransportError {
            code: self
                .callbacks
                .alert()
                .map(TransportErrorCode::crypto)
                .unwrap_or(TransportErrorCode::PROTOCOL_VIOLATION),
            frame: None,
            reason,
        }
    }

    fn next_keys(&mut self) -> Option<Keys> {
        let next_level = match self.quinn_level {
            Level::Initial => Level::Handshake,
            Level::Handshake => Level::Application,
            Level::Early | Level::Application => return None,
        };
        let (suite, local, remote) = self.callbacks.take_secret_pair(next_level)?;
        let keys = keys::keys_from_pair(self.version, suite, &local, &remote).ok()?;
        self.quinn_level = next_level;
        if next_level == Level::Application {
            self.next_local_secret = Some(local);
            self.next_remote_secret = Some(remote);
            self.next_suite = Some(suite);
        }
        Some(keys)
    }
}

impl Drop for OpenSslQuicSession {
    fn drop(&mut self) {
        // SSL_free may consult per-SSL callbacks. Keep their boxed argument alive
        // until the OpenSSL object has been released.
        self.ssl.take();
    }
}

pub(super) fn decode_transport_parameters(
    side: Side,
    params: &[u8],
) -> Result<TransportParameters, TransportError> {
    TransportParameters::read(side, &mut Bytes::copy_from_slice(params)).map_err(|error| {
        TransportError {
            code: TransportErrorCode::TRANSPORT_PARAMETER_ERROR,
            frame: None,
            reason: format!("invalid peer QUIC transport parameters: {error}"),
        }
    })
}

impl Session for OpenSslQuicSession {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: Side) -> Keys {
        keys::initial_keys(self.version, dst_cid, side)
            .expect("supported OpenSSL QUIC initial cipher")
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        self.handshake_data
            .clone()
            .map(|data| Box::new(data) as Box<dyn Any>)
    }

    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        None
    }

    fn early_crypto(&self) -> Option<(Box<dyn HeaderKey>, Box<dyn PacketKey>)> {
        None
    }

    fn early_data_accepted(&self) -> Option<bool> {
        None
    }

    fn is_handshaking(&self) -> bool {
        !self.complete
    }

    fn read_handshake(&mut self, buf: &[u8]) -> Result<bool, TransportError> {
        self.callbacks.push_input(buf).map_err(|reason| {
            self.failure = Some(reason.into());
            self.transport_error(reason.into())
        })?;
        self.drive()?;
        if !self.handshake_data_reported && self.handshake_data.is_some() {
            self.handshake_data_reported = true;
            return Ok(true);
        }
        Ok(false)
    }

    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        let Some(params) = self.callbacks.peer_transport_params() else {
            return Ok(None);
        };
        decode_transport_parameters(self.side, params).map(Some)
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<Keys> {
        self.callbacks.drain_output(self.quinn_level, buf);
        self.next_keys()
    }

    fn next_1rtt_keys(&mut self) -> Option<KeyPair<Box<dyn PacketKey>>> {
        let suite = self.next_suite?;
        let local =
            keys::next_secret(self.version, suite, self.next_local_secret.as_deref()?).ok()?;
        let remote =
            keys::next_secret(self.version, suite, self.next_remote_secret.as_deref()?).ok()?;
        let keys = keys::keys_from_pair(self.version, suite, &local, &remote).ok()?;
        self.next_local_secret = Some(local);
        self.next_remote_secret = Some(remote);
        Some(keys.packet)
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        keys::valid_retry(self.version, orig_dst_cid, header, payload)
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ExportKeyingMaterialError> {
        let ssl = self.ssl.as_ref().ok_or(ExportKeyingMaterialError)?.as_ptr();
        if unsafe { ffi::export_keying_material(ssl, output, label, context) } {
            Ok(())
        } else {
            Err(ExportKeyingMaterialError)
        }
    }
}
