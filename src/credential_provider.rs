//! Lazy bridge from the EdgeCommons credential vault to protocol backends.
//!
//! Camera configuration retains only standard `$secret` references. Vault reads happen when a
//! session is established, on a bounded blocking lane, and copied values remain in zeroing buffers.

use std::sync::Arc;

use async_trait::async_trait;
use edgecommons::credentials::{CredentialService, Secret};
use serde::Deserialize;
use tokio::sync::Semaphore;
use zeroize::Zeroize;

use crate::backend::net::{CredentialProvider, NetworkCredentials, SecretBytes};
use crate::config::SecretRef;
use crate::{CameraError, Result};

const MAX_CONCURRENT_VAULT_READS: usize = 4;
const MAX_SECRET_BYTES: usize = 1024 * 1024;

/// Production credential provider backed by the component-scoped EdgeCommons vault.
#[derive(Clone)]
pub struct EdgeCommonsCredentialProvider {
    service: Arc<dyn CredentialService>,
    permits: Arc<Semaphore>,
}

impl EdgeCommonsCredentialProvider {
    /// Creates a lazy provider. No secret is resolved during construction.
    #[must_use]
    pub fn new(service: Arc<dyn CredentialService>) -> Self {
        Self {
            service,
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_VAULT_READS)),
        }
    }

    async fn selected_bytes(&self, reference: &SecretRef) -> Result<ZeroizingBytes> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| credential_error("credential read gate is closed"))?;
        let service = Arc::clone(&self.service);
        let secret_name = reference.secret.clone();
        let field = reference.field.clone();
        tokio::task::spawn_blocking(move || {
            // The owned permit deliberately stays in this unabortable closure. A cancelled
            // caller therefore cannot create an unbounded tail of vault/file operations.
            let _permit = permit;
            let secret = service
                .get(&secret_name)
                .map_err(|_| credential_error("credential vault read failed"))?
                .ok_or_else(|| credential_error("referenced credential is unavailable"))?;
            select_secret_bytes(secret, field.as_deref())
        })
        .await
        .map_err(|_| credential_error("credential vault worker failed"))?
    }
}

impl std::fmt::Debug for EdgeCommonsCredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EdgeCommonsCredentialProvider")
            .field("maximum_concurrent_reads", &MAX_CONCURRENT_VAULT_READS)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for EdgeCommonsCredentialProvider {
    async fn resolve_login(&self, reference: &SecretRef) -> Result<Arc<NetworkCredentials>> {
        let selected = self.selected_bytes(reference).await?;
        let document: LoginDocument = serde_json::from_slice(selected.as_slice())
            .map_err(|_| credential_error("camera login secret is not the required JSON object"))?;
        let credentials = NetworkCredentials::new(
            document.username.as_bytes().to_vec(),
            document.password.as_bytes().to_vec(),
        )?;
        Ok(Arc::new(credentials))
    }

    async fn resolve_bytes(&self, reference: &SecretRef) -> Result<Arc<SecretBytes>> {
        let selected = self.selected_bytes(reference).await?;
        Ok(Arc::new(SecretBytes::new(selected.into_vec())))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LoginDocument {
    username: String,
    password: String,
}

impl Drop for LoginDocument {
    fn drop(&mut self) {
        self.username.zeroize();
        self.password.zeroize();
    }
}

struct ZeroizingBytes(Vec<u8>);

impl ZeroizingBytes {
    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn into_vec(mut self) -> Vec<u8> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for ZeroizingBytes {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

fn select_secret_bytes(secret: Secret, field: Option<&str>) -> Result<ZeroizingBytes> {
    if secret.bytes().is_empty() || secret.bytes().len() > MAX_SECRET_BYTES {
        return Err(credential_error("credential value violates its byte bound"));
    }
    let selected = match field {
        None => secret.bytes().to_vec(),
        Some(field) => {
            if field.is_empty() || field.len() > 256 || field.chars().any(char::is_control) {
                return Err(credential_error("credential field selector is invalid"));
            }
            let document: serde_json::Value = serde_json::from_slice(secret.bytes())
                .map_err(|_| credential_error("credential field source is not JSON"))?;
            let document = ZeroizingJson(document);
            let value = document
                .0
                .as_object()
                .and_then(|object| object.get(field))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| credential_error("credential field is absent or is not a string"))?;
            if value.is_empty() || value.len() > MAX_SECRET_BYTES {
                return Err(credential_error("credential field violates its byte bound"));
            }
            value.as_bytes().to_vec()
        }
    };
    Ok(ZeroizingBytes(selected))
}

struct ZeroizingJson(serde_json::Value);

impl Drop for ZeroizingJson {
    fn drop(&mut self) {
        zeroize_json_strings(&mut self.0);
    }
}

fn zeroize_json_strings(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => text.zeroize(),
        serde_json::Value::Array(values) => {
            for value in values {
                zeroize_json_strings(value);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                zeroize_json_strings(value);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn credential_error(message: &'static str) -> CameraError {
    CameraError::Backend {
        backend: "credentials",
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use edgecommons::credentials::{
        CredentialService, DefaultCredentialService, FileKeyProvider, KeyProvider, LocalVault,
        PutOptions,
    };

    use super::*;

    fn provider() -> (tempfile::TempDir, EdgeCommonsCredentialProvider) {
        let directory = tempfile::tempdir().expect("temporary vault directory");
        let key = Arc::new(FileKeyProvider::from_bytes([19_u8; 32])) as Arc<dyn KeyProvider>;
        let vault = LocalVault::open(directory.path().join("vault"), key, 2)
            .expect("test credential vault");
        let service = Arc::new(DefaultCredentialService::new(vault));
        service
            .put(
                "camera/login",
                br#"{"username":"operator","password":"camera-secret"}"#,
                PutOptions::default(),
            )
            .expect("whole login secret");
        service
            .put(
                "camera/bundle",
                br#"{"login":"{\"username\":\"nested\",\"password\":\"nested-secret\"}","ca":"test-ca-pem"}"#,
                PutOptions::default(),
            )
            .expect("field secret");
        (
            directory,
            EdgeCommonsCredentialProvider::new(service as Arc<dyn CredentialService>),
        )
    }

    #[tokio::test]
    async fn resolves_whole_and_field_login_without_debug_disclosure() {
        let (_directory, provider) = provider();
        let whole = provider
            .resolve_login(&SecretRef {
                secret: "camera/login".to_owned(),
                field: None,
            })
            .await
            .expect("whole login");
        let nested = provider
            .resolve_login(&SecretRef {
                secret: "camera/bundle".to_owned(),
                field: Some("login".to_owned()),
            })
            .await
            .expect("field login");
        assert_eq!(format!("{whole:?}"), "NetworkCredentials(<redacted>)");
        assert_eq!(format!("{nested:?}"), "NetworkCredentials(<redacted>)");
    }

    #[tokio::test]
    async fn resolves_opaque_field_and_sanitizes_lookup_errors() {
        let (_directory, provider) = provider();
        let ca = provider
            .resolve_bytes(&SecretRef {
                secret: "camera/bundle".to_owned(),
                field: Some("ca".to_owned()),
            })
            .await
            .expect("CA field");
        assert_eq!(ca.expose(), b"test-ca-pem");
        assert!(!format!("{ca:?}").contains("test-ca-pem"));

        let error = provider
            .resolve_bytes(&SecretRef {
                secret: "camera/bundle".to_owned(),
                field: Some("missing".to_owned()),
            })
            .await
            .expect_err("missing field");
        let text = error.to_string();
        assert!(!text.contains("camera-secret"));
        assert!(!text.contains("nested-secret"));
        assert!(!text.contains("test-ca-pem"));
    }
}
