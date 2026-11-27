//! Authenticated keyset cursors, bound to the current tenant/namespace/policy
//! and endpoint/filter. They describe sampled pagination, not snapshot cuts.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use rescrow_application::{Error, canonical};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

pub struct Cursors {
    key: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    scope: String,
    after: String,
    expires_at_ms: u64,
}
impl Cursors {
    pub fn new(encoded: &str) -> Result<Self, Error> {
        if encoded.len() != 64 {
            return Err(Error::input(
                "cursor key must contain 64 hexadecimal characters",
            ));
        }
        let mut key = Vec::with_capacity(32);
        for pair in encoded.as_bytes().chunks_exact(2) {
            let text = std::str::from_utf8(pair)
                .map_err(|_| Error::input("cursor key must be hexadecimal"))?;
            key.push(
                u8::from_str_radix(text, 16)
                    .map_err(|_| Error::input("cursor key must be hexadecimal"))?,
            );
        }
        Ok(Self { key })
    }
    fn mac(&self, bytes: &[u8]) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("fixed-length HMAC key");
        mac.update(b"rescrow.cursor.v1\0");
        mac.update(bytes);
        mac
    }
    pub fn encode(&self, scope: String, after: String, now: u64) -> Result<String, Error> {
        let expires_at_ms = now
            .checked_add(15 * 60 * 1000)
            .ok_or_else(|| Error::input("cursor expiry overflow"))?;
        let bytes = serde_json::to_vec(&Cursor {
            scope,
            after,
            expires_at_ms,
        })
        .expect("cursor strings are serializable");
        let signature = self.mac(&bytes).finalize().into_bytes();
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }
    pub fn decode(&self, value: &str, scope: &str, now: u64) -> Result<String, Error> {
        let invalid = || {
            Error::input(
                "cursor is invalid, expired, or belongs to another query or authorization scope",
            )
        };
        if value.len() > 2048 {
            return Err(invalid());
        }
        let (payload, signature) = value.split_once('.').ok_or_else(invalid)?;
        let bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| invalid())?;
        let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| invalid())?;
        self.mac(&bytes)
            .verify_slice(&signature)
            .map_err(|_| invalid())?;
        let cursor: Cursor = canonical::decode(&bytes).map_err(|_| invalid())?;
        if cursor.scope != scope || cursor.expires_at_ms <= now {
            return Err(invalid());
        }
        rescrow_application::authority::RecordKey::resource(&cursor.after)
            .map_err(|_| invalid())?;
        Ok(cursor.after)
    }
}
