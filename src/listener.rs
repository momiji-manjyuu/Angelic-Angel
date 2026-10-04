use crate::autopush::{self, ConnectResult};
use crate::config::{self, Registration};
use crate::delivery::{DeliveryConfig, DeliveryWorker};
use crate::error::{AngelicAngelError, Result};
use crate::filter::NotificationFilter;
use crate::outbox::{self, Outbox, SharedOutbox};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};

fn calc_backoff(retry_count: u32) -> u64 {
    5u64.saturating_mul(2u64.saturating_pow(retry_count.saturating_sub(1))).min(300)
}

pub fn validate_registration(registration: &Registration) -> Result<()> {
    let invalid = || AngelicAngelError::Config("saved push session or keys are invalid; repair explicitly before listening".into());
    let keys = &registration.keys;
    if keys.private_key.len() != 32 || keys.public_key.len() != 65 || keys.auth_secret.len() != 16 {
        return Err(invalid());
    }
    let private = p256::SecretKey::from_slice(&keys.private_key).map_err(|_| invalid())?;
    if private.public_key().to_sec1_bytes().as_ref() != keys.public_key.as_slice()
        || uuid::Uuid::parse_str(&registration.autopush.uaid).is_err()
        || uuid::Uuid::parse_str(&registration.autopush.channel_id).is_err() {
        return Err(invalid());
    }
    Ok(())
}

/// Owns both tasks. Signals finish in-progress durable commits before exiting.
/// The registered session is used as-is; expiration needs an explicit registration.
pub async fn listen(registration: Registration, path: PathBuf, filter: NotificationFilter) -> Result<()> {
    validate_registration(&registration)?;
    let endpoint = config::get_webhook_endpoint()?;
    let queue = tokio::task::spawn_blocking(move || Outbox::open(path)).await
        .map_err(|_| AngelicAngelError::BackgroundTask)??;
    let shared = Arc::new(Mutex::new(queue));
    let worker = DeliveryWorker::new(shared.clone(), &endpoint, DeliveryConfig::default())?;
    worker.bind_destination().await?;
    let (stop, stopped) = watch::channel(false);
    let mut source = tokio::spawn(receive_loop(registration, shared.clone(), filter, stopped.clone()));
    let mut delivery = tokio::spawn(worker.run(stopped.clone()));
    let mut health = tokio::spawn(report_health(shared, stopped));
    enum Completed { Source, Delivery, Health, Signal }
    let (completed, result) = tokio::select! {
        result = &mut source => (Completed::Source, result.map_err(|_| AngelicAngelError::BackgroundTask).and_then(|r| r)),
        result = &mut delivery => (Completed::Delivery, result.map_err(|_| AngelicAngelError::BackgroundTask).and_then(|r| r.map_err(Into::into))),
        result = &mut health => (Completed::Health, result.map_err(|_| AngelicAngelError::BackgroundTask).and_then(|r| r)),
        result = shutdown_signal() => (Completed::Signal, result),
    };
    let _ = stop.send(true);
    // Never abort tasks during an atomic storage operation.
    let source_result = if matches!(completed, Completed::Source) { Ok(()) }
        else { source.await.map_err(|_| AngelicAngelError::BackgroundTask).and_then(|r| r) };
    let delivery_result = if matches!(completed, Completed::Delivery) { Ok(()) }
        else { delivery.await.map_err(|_| AngelicAngelError::BackgroundTask).and_then(|r| r.map_err(Into::into)) };
    let health_result = if matches!(completed, Completed::Health) { Ok(()) }
        else { health.await.map_err(|_| AngelicAngelError::BackgroundTask).and_then(|r| r) };
    result.and(source_result).and(delivery_result).and(health_result)
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn cancelled(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() || stop.has_changed().is_err() { return; }
        if stop.changed().await.is_err() { return; }
    }
}

async fn receive_loop(
    registration: Registration, queue: SharedOutbox, filter: NotificationFilter,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut retry_count: u32 = 0;
    loop {
        let connection = tokio::select! {
            biased;
            _ = cancelled(&mut stop) => return Ok(()),
            result = tokio::time::timeout(Duration::from_secs(35),
                autopush::connect_and_listen(&registration.autopush, &registration.keys)) =>
                result.unwrap_or_else(|_| Err(AngelicAngelError::AutoPush("push connection timed out".into()))),
        };
        let (connected, result) = match connection {
            Ok(ConnectResult::Connected(mut client)) => {
                tracing::info!("push connection established");
                (true, run_notification_loop(&mut client, &registration, queue.clone(), &filter, &mut stop).await)
            }
            // Kept defensive for compatibility; AutoPush no longer creates this
            // result by automatically registering a replacement subscription.
            Ok(ConnectResult::NeedsReregistration(_)) => {
                return Err(AngelicAngelError::AutoPush("UAID invalid; explicit re-registration required".into()));
            }
            Err(error) => (false, Err(error)),
        };
        if let Err(AngelicAngelError::Outbox(error)) = result { return Err(error.into()); }
        if *stop.borrow() || stop.has_changed().is_err() { return Ok(()); }
        let delay = match result {
            Err(ref error) if error.to_string().contains("UAID invalid") => return result,
            Err(ref error) if error.to_string().contains("BACKOFF:") => 30 * 60,
            _ if connected => { retry_count = 0; 1 },
            _ => { retry_count = retry_count.saturating_add(1); calc_backoff(retry_count) },
        };
        // Errors from remote peers may contain untrusted data. Do not print them.
        tracing::warn!(delay_secs = delay, "push disconnected; reconnect scheduled");
        tokio::select! {
            _ = cancelled(&mut stop) => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(delay)) => {},
        }
    }
}

async fn run_notification_loop(
    client: &mut autopush::AutoPushClient, registration: &Registration,
    queue: SharedOutbox, filter: &NotificationFilter, stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let notification = tokio::select! {
            biased;
            _ = cancelled(stop) => return Ok(()),
            result = client.next_notification() => result?,
        };
        let Some(notification) = notification else { return Ok(()); };
        if uuid::Uuid::parse_str(&notification.channel_id).ok()
            != uuid::Uuid::parse_str(&registration.autopush.channel_id).ok() {
            return Err(AngelicAngelError::AutoPush("notification channel does not match saved registration".into()));
        }
        let ack_code = match &notification.data {
            Some(data) => match decode_notification(data, &notification.headers, &registration.keys) {
                Ok(payload) if filter.accepts(&payload) => {
                    // Do not race this future against shutdown. A failed/uncertain
                    // commit exits without ANY ACK, allowing upstream redelivery.
                    outbox::enqueue_durable(queue.clone(), notification.channel_id.clone(),
                        notification.version.clone(), payload).await?;
                    autopush::AckCode::Delivered
                },
                Ok(_) => {
                    tracing::info!("notification intentionally excluded by type allowlist");
                    autopush::AckCode::Delivered
                },
                Err(_) => {
                    tracing::warn!("notification could not be decrypted or parsed");
                    autopush::AckCode::DecryptionError
                },
            },
            None => autopush::AckCode::Delivered,
        };
        tokio::time::timeout(Duration::from_secs(10), client.ack_notification(
            notification.channel_id, notification.version, ack_code)).await
            .map_err(|_| AngelicAngelError::AutoPush("ACK timed out".into()))??;
    }
}

fn decode_notification(
    data: &str, headers: &Option<std::collections::HashMap<String, String>>,
    keys: &crate::config::WebPushKeys,
) -> Result<serde_json::Value> {
    // Bound allocations before decoding untrusted push data.
    if data.len() > 512 * 1024 {
        return Err(AngelicAngelError::Decryption("notification is too large".into()));
    }
    let encrypted = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, data)
        .map_err(|_| AngelicAngelError::Decryption("notification encoding is invalid".into()))?;
    let decrypted = decrypt_ece(&encrypted, headers, keys)?;
    if decrypted.len() > 256 * 1024 {
        return Err(AngelicAngelError::Decryption("notification is too large".into()));
    }
    serde_json::from_slice(&decrypted)
        .map_err(|_| AngelicAngelError::Decryption("notification JSON is invalid".into()))
}

async fn report_health(queue: SharedOutbox, mut stop: watch::Receiver<bool>) -> Result<()> {
    loop {
        let snapshot = queue.lock().await.snapshot();
        tracing::info!(pending = snapshot.pending, delivered = snapshot.delivered,
            dead_letter = snapshot.dead_letter, storage_healthy = snapshot.storage_healthy,
            next_attempt_unix_ms = ?snapshot.next_attempt_unix_ms, "queue health");
        if snapshot.dead_letter > 0 { tracing::warn!(count = snapshot.dead_letter, "dead letters require operator review"); }
        tokio::select! {
            _ = cancelled(&mut stop) => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(30)) => {},
        }
    }
}

fn decrypt_ece(
    encrypted: &[u8],
    headers: &Option<std::collections::HashMap<String, String>>,
    keys: &crate::config::WebPushKeys,
) -> Result<Vec<u8>> {
    tracing::debug!(
        private_key_len = keys.private_key.len(),
        public_key_len = keys.public_key.len(),
        auth_secret_len = keys.auth_secret.len(),
        encrypted_len = encrypted.len(),
        "starting ECE decryption"
    );

    let key_pair = ece::EcKeyComponents::new(keys.private_key.clone(), keys.public_key.clone());

    let encoding = headers
        .as_ref()
        .and_then(|h| h.get("encoding"))
        .map(|s| s.as_str());

    match encoding {
        Some("aesgcm") => {
            tracing::debug!("decrypting with aesgcm encoding");
            decrypt_aesgcm(encrypted, headers, &key_pair, &keys.auth_secret)
        }
        Some("aes128gcm") | None => {
            tracing::debug!("decrypting with aes128gcm encoding");
            let decrypted = ece::decrypt(&key_pair, &keys.auth_secret, encrypted).map_err(|e| {
                tracing::debug!(error = %e, "aes128gcm decryption error");
                AngelicAngelError::Decryption(format!("aes128gcm decryption failed: {}", e))
            })?;

            tracing::debug!(decrypted_len = decrypted.len(), "aes128gcm decryption succeeded");
            Ok(decrypted)
        }
        Some(other) => Err(AngelicAngelError::Decryption(format!(
            "unsupported encoding: {}",
            other
        ))),
    }
}

fn decrypt_aesgcm(
    encrypted: &[u8],
    headers: &Option<std::collections::HashMap<String, String>>,
    key_pair: &ece::EcKeyComponents,
    auth_secret: &[u8],
) -> Result<Vec<u8>> {
    let headers = headers.as_ref().ok_or_else(|| {
        AngelicAngelError::Decryption("aesgcm encoding requires headers but none were provided".to_string())
    })?;

    let dh_b64 = parse_header_param(headers.get("crypto_key"), "dh")?;
    let sender_public_key =
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, &dh_b64)
            .map_err(|e| {
                AngelicAngelError::Decryption(format!("failed to base64-decode sender public key: {}", e))
            })?;

    tracing::debug!(sender_public_key_len = sender_public_key.len(), "parsed sender public key");

    let salt_b64 = parse_header_param(headers.get("encryption"), "salt")?;
    let salt = base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, &salt_b64)
        .map_err(|e| AngelicAngelError::Decryption(format!("failed to base64-decode salt: {}", e)))?;

    tracing::debug!(salt_len = salt.len(), "parsed salt");

    let block = ece::legacy::AesGcmEncryptedBlock::new(
        &sender_public_key,
        &salt,
        4096,
        encrypted.to_vec(),
    )
    .map_err(|e| AngelicAngelError::Decryption(format!("failed to construct AesGcmEncryptedBlock: {}", e)))?;

    let decrypted = ece::legacy::decrypt_aesgcm(key_pair, auth_secret, &block).map_err(|e| {
        tracing::debug!(error = %e, "aesgcm decryption error");
        AngelicAngelError::Decryption(format!("aesgcm decryption failed: {}", e))
    })?;

    tracing::debug!(decrypted_len = decrypted.len(), "aesgcm decryption succeeded");
    Ok(decrypted)
}

/// Extracts a named parameter from a semicolon-delimited header value.
///
/// Example: given `"dh=abc123;p256ecdsa=xyz"` and param `"dh"`, returns `"abc123"`.
fn parse_header_param(header_value: Option<&String>, param_name: &str) -> Result<String> {
    let header = header_value.ok_or_else(|| {
        AngelicAngelError::Decryption(format!("missing required header for param '{}'", param_name))
    })?;

    for part in header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(&format!("{}=", param_name)) {
            return Ok(value.to_string());
        }
    }

    Err(AngelicAngelError::Decryption(format!(
        "required header parameter '{}' is missing",
        param_name
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_registration() -> Registration {
        let private = p256::SecretKey::from_slice(&[1u8; 32]).unwrap();
        Registration {
            endpoint: "https://example.invalid/synthetic".into(),
            autopush: config::AutoPushSession {
                uaid: "00000000-0000-4000-8000-000000000001".into(),
                channel_id: "00000000-0000-4000-8000-000000000002".into(),
            },
            keys: config::WebPushKeys {
                private_key: private.to_bytes().to_vec(),
                public_key: private.public_key().to_sec1_bytes().to_vec(),
                auth_secret: vec![2; 16],
            },
        }
    }

    #[test]
    fn invalid_local_keys_fail_before_intake() {
        let mut registration = valid_registration();
        validate_registration(&registration).unwrap();
        registration.keys.public_key[1] ^= 1;
        assert!(validate_registration(&registration).is_err());
        registration = valid_registration();
        registration.keys.auth_secret.clear();
        assert!(validate_registration(&registration).is_err());
        registration = valid_registration();
        registration.autopush.channel_id = "not-a-session".into();
        assert!(validate_registration(&registration).is_err());
    }

    #[test]
    fn reconnect_backoff_is_bounded() {
        assert_eq!(calc_backoff(1), 5);
        assert_eq!(calc_backoff(2), 10);
        assert_eq!(calc_backoff(u32::MAX), 300);
    }
}
