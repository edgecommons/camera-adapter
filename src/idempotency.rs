//! Durable command-idempotency validation and canonical request hashing.
//!
//! Correlation identifiers are transport metadata. A caller-provided `requestId` is the
//! durable operation key, while the fingerprint below covers only immutable command arguments.

use serde_json::Value;
use sha2::{Digest, Sha256};
use unicode_general_category::{GeneralCategory, get_general_category};

use crate::{CameraError, ErrorCode, Result};

/// Canonical immutable-argument fingerprint stored in the command ledger.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestHash([u8; 32]);

impl RequestHash {
    /// Builds a fingerprint from its exact bytes, for catalog deserialization.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the stable lower-case hexadecimal representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Debug for RequestHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("RequestHash")
            .field(&hex::encode(self.0))
            .finish()
    }
}

/// Validates the durable `requestId` contract.
///
/// Values are intentionally not trimmed or case-folded. The bound is on UTF-8 bytes, and
/// Unicode general categories `Cc` and `Cf` are rejected to prevent invisible/control keys.
pub fn validate_request_id(request_id: &str) -> Result<()> {
    if request_id.is_empty() || request_id.len() > 256 {
        return Err(CameraError::rejected(
            ErrorCode::InvalidRequest,
            "requestId must contain 1 to 256 UTF-8 bytes",
        ));
    }

    if request_id.chars().any(|character| {
        matches!(
            get_general_category(character),
            GeneralCategory::Control | GeneralCategory::Format
        )
    }) {
        return Err(CameraError::rejected(
            ErrorCode::InvalidRequest,
            "requestId must not contain control or format characters",
        ));
    }
    Ok(())
}

/// Canonicalizes and hashes a closed-schema command request.
///
/// The root `requestId` member is excluded. Object keys use Unicode scalar ordering; arrays retain
/// order except the optional root `instances` string array, which is deduplicated and sorted for the
/// fingerprint only. The caller's parsed request is never mutated.
pub fn canonical_request_hash(request: &Value, sort_instances: bool) -> Result<RequestHash> {
    if !request.is_object() {
        return Err(CameraError::rejected(
            ErrorCode::InvalidRequest,
            "command arguments must be a JSON object",
        ));
    }
    let mut encoded = Vec::new();
    encode_value(
        request,
        &mut encoded,
        CanonicalContext::Root { sort_instances },
    )?;
    Ok(RequestHash(Sha256::digest(encoded).into()))
}

/// Returns the canonical byte representation used by [`canonical_request_hash`].
///
/// This is exposed for shared-vector and cross-language parity tests, not as a wire encoding.
pub fn canonical_request_bytes(request: &Value, sort_instances: bool) -> Result<Vec<u8>> {
    if !request.is_object() {
        return Err(CameraError::rejected(
            ErrorCode::InvalidRequest,
            "command arguments must be a JSON object",
        ));
    }
    let mut encoded = Vec::new();
    encode_value(
        request,
        &mut encoded,
        CanonicalContext::Root { sort_instances },
    )?;
    Ok(encoded)
}

#[derive(Clone, Copy)]
enum CanonicalContext {
    Root { sort_instances: bool },
    Nested,
}

fn encode_value(value: &Value, output: &mut Vec<u8>, context: CanonicalContext) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => {
            output.extend_from_slice(if *value {
                b"true".as_slice()
            } else {
                b"false".as_slice()
            });
        }
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => {
            serde_json::to_writer(output, value)?;
        }
        Value::Array(values) => encode_array(values, output)?,
        Value::Object(values) => {
            output.push(b'{');
            let mut keys: Vec<&str> = values.keys().map(String::as_str).collect();
            keys.sort_unstable();
            let mut first = true;
            for key in keys {
                if matches!(context, CanonicalContext::Root { .. }) && key == "requestId" {
                    continue;
                }
                if !first {
                    output.push(b',');
                }
                first = false;
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');

                let child = values.get(key).ok_or_else(|| {
                    CameraError::Catalog("canonical JSON key disappeared".to_owned())
                })?;
                if key == "instances"
                    && matches!(
                        context,
                        CanonicalContext::Root {
                            sort_instances: true
                        }
                    )
                {
                    encode_sorted_instances(child, output)?;
                } else {
                    encode_value(child, output, CanonicalContext::Nested)?;
                }
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn encode_array(values: &[Value], output: &mut Vec<u8>) -> Result<()> {
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        encode_value(value, output, CanonicalContext::Nested)?;
    }
    output.push(b']');
    Ok(())
}

fn encode_sorted_instances(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    let values = value.as_array().ok_or_else(|| {
        CameraError::rejected(ErrorCode::InvalidRequest, "instances must be an array")
    })?;
    let mut instances = Vec::with_capacity(values.len());
    for value in values {
        let instance = value.as_str().ok_or_else(|| {
            CameraError::rejected(
                ErrorCode::InvalidRequest,
                "instances entries must be strings",
            )
        })?;
        instances.push(instance);
    }
    instances.sort_unstable();
    if instances.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CameraError::rejected(
            ErrorCode::InvalidRequest,
            "instances must not contain duplicates",
        ));
    }

    output.push(b'[');
    for (index, instance) in instances.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        serde_json::to_writer(&mut *output, instance)?;
    }
    output.push(b']');
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    #[test]
    fn request_ids_enforce_byte_and_category_bounds_without_normalizing() {
        assert!(validate_request_id(" Request-Ä ").is_ok());
        assert!(validate_request_id("").is_err());
        assert!(validate_request_id(&"é".repeat(129)).is_err());
        assert!(validate_request_id("line\nfeed").is_err());
        assert!(validate_request_id("hidden\u{200d}joiner").is_err());
    }

    #[test]
    fn canonical_form_sorts_keys_and_excludes_only_root_request_id() {
        let value = json!({
            "z": 2,
            "requestId": "retry-key",
            "nested": {"requestId": "business-value", "a": true},
            "a": "x"
        });
        assert_eq!(
            canonical_request_bytes(&value, false).unwrap(),
            br#"{"a":"x","nested":{"a":true,"requestId":"business-value"},"z":2}"#
        );
    }

    #[test]
    fn group_instances_sort_for_hash_without_mutating_result_order() {
        let value = json!({"requestId":"g", "instances":["cam-z", "cam-a"]});
        let before = value.clone();
        assert_eq!(
            canonical_request_bytes(&value, true).unwrap(),
            br#"{"instances":["cam-a","cam-z"]}"#
        );
        assert_eq!(value, before);
        assert!(
            canonical_request_hash(
                &json!({"requestId":"g", "instances":["cam-a", "cam-a"]}),
                true
            )
            .is_err()
        );
    }

    #[test]
    fn hashes_expose_exact_bytes_and_reject_non_object_or_bad_group_members() {
        let hash = canonical_request_hash(&json!({"requestId":"a","value":1}), false).unwrap();
        assert_eq!(RequestHash::from_bytes(*hash.as_bytes()), hash);
        assert_eq!(hash.to_hex().len(), 64);
        assert!(canonical_request_bytes(&json!(["not", "an", "object"]), false).is_err());
        assert!(canonical_request_hash(&json!({"instances":"camera-a"}), true).is_err());
        assert!(canonical_request_hash(&json!({"instances":["camera-a", 1]}), true).is_err());
    }

    #[test]
    fn request_id_and_object_insertion_order_do_not_change_hash() {
        let left = json!({"b": [3, 2, 1], "a": 1, "requestId": "left"});
        let right = json!({"requestId": "right", "a": 1, "b": [3, 2, 1]});
        assert_eq!(
            canonical_request_hash(&left, false).unwrap(),
            canonical_request_hash(&right, false).unwrap()
        );
    }

    #[test]
    fn canonical_encoding_preserves_json_scalar_and_array_meaning() {
        let value = json!({
            "null": null,
            "false": false,
            "true": true,
            "number": -12.5,
            "escaped": "quote=\" newline=\n",
            "array": [3, null, false, {"z": 2, "a": 1}],
        });

        assert_eq!(
            canonical_request_bytes(&value, false).expect("closed JSON object"),
            br#"{"array":[3,null,false,{"a":1,"z":2}],"escaped":"quote=\" newline=\n","false":false,"null":null,"number":-12.5,"true":true}"#,
        );
    }

    #[test]
    fn instances_are_special_only_at_the_sorting_root() {
        let value = json!({
            "instances": ["camera-b", "camera-a"],
            "nested": {"instances": ["camera-b", "camera-a"]},
        });

        assert_eq!(
            canonical_request_bytes(&value, false).expect("unsorted root request"),
            br#"{"instances":["camera-b","camera-a"],"nested":{"instances":["camera-b","camera-a"]}}"#,
        );
        assert_eq!(
            canonical_request_bytes(&value, true).expect("sorted group request"),
            br#"{"instances":["camera-a","camera-b"],"nested":{"instances":["camera-b","camera-a"]}}"#,
        );
    }

    #[test]
    fn group_instance_validation_rejects_duplicate_and_non_string_entries() {
        for invalid in [
            json!({"instances": ["camera-a", "camera-a"]}),
            json!({"instances": ["camera-a", 1]}),
            json!({"instances": null}),
        ] {
            let error = canonical_request_hash(&invalid, true)
                .expect_err("invalid group targets must not acquire a durable ledger key");
            assert_eq!(error.code(), ErrorCode::InvalidRequest);
        }

        assert_eq!(
            canonical_request_bytes(&json!({"instances": []}), true)
                .expect("empty targets are left for command validation"),
            br#"{"instances":[]}"#,
        );
    }

    #[test]
    fn request_hash_debug_and_hex_are_stable_for_catalog_diagnostics() {
        let hash = canonical_request_hash(&json!({"camera":"camera-a","profile":"full"}), false)
            .expect("valid command object");
        assert_eq!(
            format!("{hash:?}"),
            format!("RequestHash(\"{}\")", hash.to_hex())
        );
        assert!(hash.to_hex().bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    proptest! {
        #[test]
        fn request_id_never_affects_hash(id in "[A-Za-z0-9_-]{1,64}") {
            let value = json!({"requestId": id, "camera": "one", "count": 7});
            let baseline = json!({"requestId": "baseline", "camera": "one", "count": 7});
            prop_assert_eq!(
                canonical_request_hash(&value, false).unwrap(),
                canonical_request_hash(&baseline, false).unwrap()
            );
        }
    }
}
