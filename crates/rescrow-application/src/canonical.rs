//! Versioned canonical request encoding. Auth tokens, serving metadata, and
//! transport headers are excluded. Context keys and operation IDs are ASCII;
//! quantities are validated canonical decimal strings before hashing.
use crate::{Error, dto};
use rescrow_domain as d;
use serde::{
    Deserialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt};

/// Reject duplicate object keys at every depth before decoding transfer values.
/// Standard `Value` decoding would silently replace a previous occurrence.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = Strict::deserialize(&mut deserializer)
        .map_err(|e| Error::input(format!("invalid JSON: {e}")))?;
    deserializer
        .end()
        .map_err(|e| Error::input(format!("invalid JSON: {e}")))?;
    serde_json::from_value(value.0)
        .map_err(|e| Error::input(format!("invalid request representation: {e}")))
}
struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON without duplicate object keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(Value::Number(v.into())))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(Value::Number(v.into())))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Strict, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| Strict(Value::Number(n)))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Strict, E> {
                Ok(Strict(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Strict, E> {
                Ok(Strict(Value::String(v)))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Strict, A::Error> {
                let mut values = Vec::new();
                while let Some(Strict(v)) = access.next_element()? {
                    values.push(v);
                }
                Ok(Strict(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Strict, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = access.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON object key"));
                    }
                    let Strict(value) = access.next_value()?;
                    values.insert(key, value);
                }
                Ok(Strict(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(V)
    }
}

pub fn digest(domain: &str, canonical: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(domain.as_bytes());
    h.update([0]);
    h.update(canonical);
    use std::fmt::Write;
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in h.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String is infallible");
    }
    encoded
}
pub fn prepare_bytes(request: &dto::Prepare) -> Result<Vec<u8>, Error> {
    let request: d::Request = request
        .clone()
        .try_into()
        .map_err(|e: d::Error| Error::input(e.to_string()))?;
    // A sorted map makes the field ordering part of the versioned contract.
    let value = BTreeMap::from([
        (
            "context",
            serde_json::to_value(request.context().entries()).expect("string map is serializable"),
        ),
        (
            "hold_ms",
            serde_json::to_value(request.hold_ms()).expect("optional integer is serializable"),
        ),
        (
            "operations",
            serde_json::to_value(
                request
                    .operations()
                    .iter()
                    .map(dto::Operation::from)
                    .collect::<Vec<_>>(),
            )
            .expect("operation DTOs are serializable"),
        ),
    ]);
    serde_json::to_vec(&value).map_err(|e| Error::input(e.to_string()))
}
pub fn prepare_digest(request: &dto::Prepare) -> Result<String, Error> {
    Ok(digest("rescrow.prepare.v1", &prepare_bytes(request)?))
}
pub fn actuals_digest(transaction: &dto::Transaction) -> Result<Option<String>, Error> {
    let tx: d::Transaction = transaction
        .clone()
        .try_into()
        .map_err(|e: d::Error| Error::input(e.to_string()))?;
    let d::State::Committed(actuals) = tx.state() else {
        return Ok(None);
    };
    let values: Vec<_> = actuals
        .entries()
        .iter()
        .map(|(id, delta)| dto::Actual {
            operation_id: id.as_str().into(),
            delta: delta.to_string(),
        })
        .collect();
    let bytes = serde_json::to_vec(
        &serde_json::to_value(BTreeMap::from([("actuals", values)]))
            .expect("actual DTOs are serializable"),
    )
    .expect("actual DTOs are serializable");
    Ok(Some(digest("rescrow.actuals.v1", &bytes)))
}
