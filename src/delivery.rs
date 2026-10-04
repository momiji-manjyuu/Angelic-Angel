//! Async, serial delivery of the durable outbox. Instantiate exactly one worker per
//! outbox. Reuse its HTTP client; start it with a watch channel and send `true` for
//! shutdown. Dropping the sender also stops the worker. Await the worker's task during
//! graceful shutdown so an in-progress storage commit can finish.
//!
//! Cancellation during an HTTP request leaves a pending record, since whether the
//! receiver accepted it is unknown. Receivers must implement Idempotency-Key dedup if
//! they require duplicate-free side effects. Queue IDs remain stable across retries.
//! Attempt counts are committed before sending; a crash/cancellation can consume an
//! attempt even if no bytes reached the receiver. Exhausted records become dead letters.
//!
//! Only HTTPS endpoints without URL credentials/fragments are accepted in production.
//! Redirects and environment-configured proxies are disabled. No response body is read,
//! logged, or saved. All public errors are sanitized; do not add URL/payload/request
//! debug logging. HTTP 408/425/429, 5xx and transport errors retry; other non-2xx status
//! codes are terminal (including redirects). Retry-After supports integer seconds and
//! IMF-fixdate HTTP dates; obsolete HTTP-date formats fall back to exponential jitter.
//! Server delay is a lower bound and can exceed max_retry_delay. Check filesystem
//! pending timestamps/health counts for unexpectedly long server-requested delays.
//! Call bind_destination().await before starting the notification source. run()
//! verifies the binding again before it can issue any HTTP request. Binding failures
//! are fatal; never delete/rewrite the binding just to accept a changed endpoint.

use std::time::Duration;

use rand::Rng;
use reqwest::{Client, Url, header::RETRY_AFTER};
use tokio::sync::watch;

use crate::outbox::{self, FailureReason, Outbox, OutboxError, SharedOutbox};

#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    #[error("delivery endpoint or retry/timeouts configuration is invalid")]
    InvalidConfiguration,
    #[error("delivery HTTP client initialization failed")]
    ClientInitialization,
    #[error("delivery stopped because durable storage failed: {0}")]
    Storage(#[from] OutboxError),
}

#[derive(Clone, Debug)]
pub struct DeliveryConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub idle_poll_interval: Duration,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub max_attempts: u32,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self { connect_timeout: Duration::from_secs(10), request_timeout: Duration::from_secs(30),
            idle_poll_interval: Duration::from_secs(1), initial_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(15 * 60), max_attempts: 12 }
    }
}

/// Intentionally no Debug implementation: the endpoint may contain a secret query.
pub struct DeliveryWorker {
    queue: SharedOutbox,
    client: Client,
    endpoint: Url,
    config: DeliveryConfig,
}

struct HttpOutcome { status: Option<u16>, retry_after_ms: Option<u64> }

impl DeliveryWorker {
    pub fn new(queue: SharedOutbox, webhook_url: &str, config: DeliveryConfig) -> Result<Self, DeliveryError> {
        Self::build(queue, webhook_url, config, false)
    }

    /// Must succeed before starting the source or enqueuing into a new queue. Existing
    /// queues require the same full endpoint; no raw endpoint is persisted or logged.
    pub async fn bind_destination(&self) -> Result<(), DeliveryError> {
        let endpoint = self.endpoint.as_str().to_owned();
        storage(self.queue.clone(), move |queue| queue.bind_destination(&endpoint)).await
    }

    fn build(queue: SharedOutbox, webhook_url: &str, config: DeliveryConfig, allow_test_loopback: bool) -> Result<Self, DeliveryError> {
        if config.max_attempts == 0 || config.max_attempts == u32::MAX
            || config.connect_timeout.is_zero() || config.request_timeout.is_zero()
            || config.idle_poll_interval.is_zero() || config.initial_retry_delay.as_millis() == 0
            || config.initial_retry_delay > config.max_retry_delay
            || config.max_retry_delay.as_millis() > u64::MAX as u128 {
            return Err(DeliveryError::InvalidConfiguration);
        }
        let endpoint = Url::parse(webhook_url).map_err(|_| DeliveryError::InvalidConfiguration)?;
        // The HTTP exception is compiled only for unit-test localhost servers. It
        // cannot be enabled by runtime configuration/environment in a normal build.
        let permit_http = cfg!(test) && allow_test_loopback && endpoint.scheme() == "http"
            && endpoint.host_str().map(|host| host == "127.0.0.1" || host == "[::1]").unwrap_or(false);
        if (endpoint.scheme() != "https" && !permit_http) || endpoint.host_str().is_none()
            || !endpoint.username().is_empty() || endpoint.password().is_some() || endpoint.fragment().is_some() {
            return Err(DeliveryError::InvalidConfiguration);
        }
        let client = Client::builder().connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout).redirect(reqwest::redirect::Policy::none())
            .https_only(!permit_http).no_proxy().build().map_err(|_| DeliveryError::ClientInitialization)?;
        Ok(Self { queue, client, endpoint, config })
    }

    /// Returns on requested shutdown, dropped shutdown sender, or storage failure.
    /// HTTP errors are handled by persisted retry/dead-letter transitions instead.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), DeliveryError> {
        self.bind_destination().await?;
        let _lease = self.queue.lock().await.claim_worker()?;
        loop {
            if stopping(&shutdown) { return Ok(()); }
            let now = outbox::unix_ms()?;
            // Do not race/drop storage tasks on shutdown: finish the durable write.
            let attempt = storage(self.queue.clone(), move |queue| queue.prepare_due(now)).await?;
            if stopping(&shutdown) { return Ok(()); }
            let Some(attempt) = attempt else {
                tokio::select! {
                    biased;
                    _ = cancelled(&mut shutdown) => return Ok(()),
                    _ = tokio::time::sleep(self.config.idle_poll_interval) => {},
                }
                continue;
            };
            if attempt.attempts > self.config.max_attempts {
                let id = attempt.id;
                let now = outbox::unix_ms()?;
                storage(self.queue.clone(), move |queue|
                    queue.dead_letter(&id, now, None, FailureReason::AttemptsExhausted)).await?;
                continue;
            }
            let response = tokio::select! {
                biased;
                _ = cancelled(&mut shutdown) => return Ok(()),
                result = self.client.post(self.endpoint.clone())
                    .header("Idempotency-Key", &attempt.id).json(&attempt.payload).send() => result,
            };
            let now = outbox::unix_ms()?;
            let outcome = match response {
                Ok(response) => HttpOutcome { status: Some(response.status().as_u16()),
                    retry_after_ms: response.headers().get(RETRY_AFTER).and_then(|header| header.to_str().ok())
                        .and_then(|value| retry_after_delay_ms(value, now)) },
                // Never store, display, or log the reqwest error: it can contain the URL.
                Err(_) => HttpOutcome { status: None, retry_after_ms: None },
            };
            let id = attempt.id;
            if outcome.status.map(|status| (200..300).contains(&status)).unwrap_or(false) {
                let status = outcome.status.expect("success status is present");
                storage(self.queue.clone(), move |queue| queue.delivered(&id, now, status)).await?;
            } else if !retryable(outcome.status) {
                storage(self.queue.clone(), move |queue|
                    queue.dead_letter(&id, now, outcome.status, FailureReason::PermanentHttp)).await?;
            } else if attempt.attempts >= self.config.max_attempts {
                storage(self.queue.clone(), move |queue|
                    queue.dead_letter(&id, now, outcome.status, FailureReason::AttemptsExhausted)).await?;
            } else {
                let delay = backoff_ms(&self.config, attempt.attempts)
                    .max(outcome.retry_after_ms.unwrap_or(0));
                let next = now.saturating_add(delay);
                storage(self.queue.clone(), move |queue| queue.retry(&id, next, outcome.status)).await?;
            }
        }
    }
}

async fn storage<T: Send + 'static>(queue: SharedOutbox,
    action: impl FnOnce(&mut Outbox) -> outbox::Result<T> + Send + 'static,
) -> Result<T, DeliveryError> {
    tokio::task::spawn_blocking(move || action(&mut queue.blocking_lock()))
        .await.map_err(|_| OutboxError::WorkerFailed)?.map_err(Into::into)
}

fn stopping(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

async fn cancelled(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if stopping(shutdown) { return; }
        if shutdown.changed().await.is_err() { return; }
    }
}

fn retryable(status: Option<u16>) -> bool {
    match status { None | Some(408 | 425 | 429 | 500..=599) => true, _ => false }
}

fn backoff_ms(config: &DeliveryConfig, attempt: u32) -> u64 {
    let initial = config.initial_retry_delay.as_millis() as u64;
    let maximum = config.max_retry_delay.as_millis() as u64;
    let factor = 1u64.checked_shl(attempt.saturating_sub(1)).unwrap_or(u64::MAX);
    let ceiling = initial.saturating_mul(factor).min(maximum);
    // Equal jitter, rounded upward, ensures a nonzero retry delay.
    let floor = (ceiling / 2).max(1);
    rand::thread_rng().gen_range(floor..=ceiling)
}

fn retry_after_delay_ms(value: &str, now: u64) -> Option<u64> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value.parse::<u64>().ok().map(|seconds| seconds.saturating_mul(1000));
    }
    http_date_unix_ms(value).map(|date| date.saturating_sub(now))
}

/// Parse the unambiguous IMF-fixdate form, without adding a date/time dependency.
fn http_date_unix_ms(value: &str) -> Option<u64> {
    if value.len() != 29 || !value.is_ascii() { return None; }
    let bytes = value.as_bytes();
    if !matches!(&value[0..3], "Mon" | "Tue" | "Wed" | "Thu" | "Fri" | "Sat" | "Sun")
        || &value[3..5] != ", " || bytes[7] != b' ' || bytes[11] != b' '
        || bytes[16] != b' ' || bytes[19] != b':' || bytes[22] != b':' || &value[25..29] != " GMT" {
        return None;
    }
    let day: u32 = digits(&value[5..7])?;
    let month: u32 = match &value[8..11] {
        "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
        "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12, _ => return None,
    };
    let year: u32 = digits(&value[12..16])?;
    let hour: u32 = digits(&value[17..19])?;
    let minute: u32 = digits(&value[20..22])?;
    let second: u32 = digits(&value[23..25])?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = match month { 2 => if leap { 29 } else { 28 }, 4 | 6 | 9 | 11 => 30, _ => 31 };
    if year < 1970 || day == 0 || day > month_days || hour > 23 || minute > 59 || second > 59 { return None; }
    // Gregorian days since 1970. A bounded year loop is intentionally straightforward.
    let mut days = 0u64;
    for previous_year in 1970..year {
        days += if previous_year % 4 == 0 && (previous_year % 100 != 0 || previous_year % 400 == 0) { 366 } else { 365 };
    }
    for previous_month in 1..month {
        days += match previous_month { 2 => if leap { 29 } else { 28 }, 4 | 6 | 9 | 11 => 30, _ => 31 };
    }
    days += u64::from(day - 1);
    Some((days * 86_400 + u64::from(hour) * 3600 + u64::from(minute) * 60 + u64::from(second)) * 1000)
}

fn digits(value: &str) -> Option<u32> {
    if value.bytes().all(|byte| byte.is_ascii_digit()) { value.parse().ok() } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf, sync::Arc};
    use serde_json::json;
    use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, sync::{Mutex, mpsc}};
    use uuid::Uuid;

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self { Self(std::env::temp_dir().join(format!("angelic-delivery-test-{}", Uuid::new_v4()))) }
        fn queue(&self) -> SharedOutbox { Arc::new(Mutex::new(Outbox::open(&self.0).unwrap())) }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
    }

    fn test_config() -> DeliveryConfig {
        DeliveryConfig { connect_timeout: Duration::from_millis(100), request_timeout: Duration::from_millis(100),
            idle_poll_interval: Duration::from_millis(5), initial_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(20), max_attempts: 3 }
    }

    /// Minimal local HTTP mock. Test definitions only; do not run without execution approval.
    async fn server(responses: Vec<&'static str>) -> (String, mpsc::UnboundedReceiver<String>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut connection, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0u8; 2048];
                    let count = connection.read(&mut buffer).await.unwrap();
                    if count == 0 { break; }
                    request.extend_from_slice(&buffer[..count]);
                    assert!(request.len() <= 1024 * 1024);
                    if let Some(header_end) = request.windows(4).position(|chunk| chunk == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        let length = headers.lines().find_map(|line| line.split_once(':').and_then(|(name, value)| {
                            if name.eq_ignore_ascii_case("content-length") { value.trim().parse::<usize>().ok() } else { None }
                        })).unwrap_or(0);
                        if request.len() >= header_end + 4 + length { break; }
                    }
                }
                sender.send(String::from_utf8(request).unwrap()).unwrap();
                connection.write_all(response.as_bytes()).await.unwrap();
                connection.shutdown().await.unwrap();
            }
        });
        (format!("http://{address}/webhook"), receiver, task)
    }

    async fn wait_for(queue: &SharedOutbox, delivered: usize, dead_letter: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = queue.lock().await.snapshot();
                if snapshot.delivered == delivered && snapshot.dead_letter == dead_letter { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn retries_503_with_stable_idempotency_key_then_delivers() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        let (url, mut requests, mock) = server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
        ]).await;
        let worker = DeliveryWorker::build(queue.clone(), &url, test_config(), true).unwrap();
        worker.bind_destination().await.unwrap();
        let id = outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!({"text":"test"})).await.unwrap().id;
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(worker.run(shutdown));
        wait_for(&queue, 1, 0).await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        mock.await.unwrap();
        for _ in 0..2 {
            let request = requests.recv().await.unwrap();
            assert!(request.to_ascii_lowercase().contains(&format!("idempotency-key: {id}")));
        }
        assert_eq!(queue.lock().await.snapshot().total_attempts, 2);
    }

    #[tokio::test]
    async fn redirect_and_permanent_client_error_are_dead_letters() {
        for status in ["302 Found", "400 Bad Request"] {
            let directory = TestDirectory::new();
            let queue = directory.queue();
            let response = if status == "302 Found" {
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/never-follow\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            } else {
                "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            };
            let (url, _requests, mock) = server(vec![response]).await;
            let worker = DeliveryWorker::build(queue.clone(), &url, test_config(), true).unwrap();
            worker.bind_destination().await.unwrap();
            outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!(null)).await.unwrap();
            let (stop, shutdown) = watch::channel(false);
            let task = tokio::spawn(worker.run(shutdown));
            wait_for(&queue, 0, 1).await;
            stop.send(true).unwrap();
            task.await.unwrap().unwrap();
            mock.await.unwrap();
            assert_eq!(queue.lock().await.snapshot().total_attempts, 1);
        }
    }

    #[tokio::test]
    async fn retry_after_is_persisted_before_shutdown_and_recovers() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        let (url, _requests, mock) = server(vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ]).await;
        let worker = DeliveryWorker::build(queue.clone(), &url, test_config(), true).unwrap();
        worker.bind_destination().await.unwrap();
        outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!({})).await.unwrap();
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(worker.run(shutdown));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if queue.lock().await.snapshot().next_attempt_unix_ms.unwrap() > outbox::unix_ms().unwrap() + 100_000 { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        mock.await.unwrap();
        drop(queue);
        let reopened = Outbox::open(&directory.0).unwrap();
        assert_eq!(reopened.snapshot().pending, 1);
        assert!(reopened.snapshot().next_attempt_unix_ms.unwrap() > outbox::unix_ms().unwrap() + 100_000);
    }

    #[tokio::test]
    async fn cancellation_keeps_inflight_request_pending() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/slow", listener.local_addr().unwrap());
        let mut config = test_config();
        config.request_timeout = Duration::from_secs(60);
        let worker = DeliveryWorker::build(queue.clone(), &endpoint, config, true).unwrap();
        worker.bind_destination().await.unwrap();
        outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!({})).await.unwrap();
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(worker.run(shutdown));
        let (_connection, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
        let snapshot = queue.lock().await.snapshot();
        assert_eq!(snapshot.pending, 1);
        assert_eq!(snapshot.delivered, 0);
        assert_eq!(snapshot.dead_letter, 0);
    }

    #[tokio::test]
    async fn repeated_transient_errors_exhaust_into_dead_letter() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        let response = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let (url, _requests, mock) = server(vec![response; 3]).await;
        let worker = DeliveryWorker::build(queue.clone(), &url, test_config(), true).unwrap();
        worker.bind_destination().await.unwrap();
        outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!({})).await.unwrap();
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(worker.run(shutdown));
        wait_for(&queue, 0, 1).await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        mock.await.unwrap();
        assert_eq!(queue.lock().await.snapshot().total_attempts, 3);
    }

    #[tokio::test]
    async fn request_timeout_is_bounded_and_becomes_dead_letter_at_limit() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/no-response", listener.local_addr().unwrap());
        let mut config = test_config();
        config.max_attempts = 1;
        let worker = DeliveryWorker::build(queue.clone(), &endpoint, config, true).unwrap();
        worker.bind_destination().await.unwrap();
        outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!({})).await.unwrap();
        let (stop, shutdown) = watch::channel(false);
        let task = tokio::spawn(worker.run(shutdown));
        let (_connection, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
        wait_for(&queue, 0, 1).await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(queue.lock().await.snapshot().total_attempts, 1);
    }

    #[tokio::test]
    async fn changed_endpoint_is_rejected_before_any_request() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let original = DeliveryWorker::build(queue.clone(), &format!("http://{address}/original"), test_config(), true).unwrap();
        original.bind_destination().await.unwrap();
        outbox::enqueue_durable(queue.clone(), "c".into(), "1".into(), json!({})).await.unwrap();
        let changed = DeliveryWorker::build(queue.clone(), &format!("http://{address}/different-recipient"), test_config(), true).unwrap();
        let (_stop, shutdown) = watch::channel(false);
        assert!(matches!(changed.run(shutdown).await, Err(DeliveryError::Storage(OutboxError::DestinationMismatch))));
        assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err());
        assert_eq!(queue.lock().await.snapshot().total_attempts, 0);
        original.bind_destination().await.unwrap();
    }

    #[test]
    fn production_constructor_rejects_http_and_url_credentials() {
        let directory = TestDirectory::new();
        let queue = directory.queue();
        assert!(matches!(DeliveryWorker::new(queue.clone(), "http://127.0.0.1/test", test_config()), Err(DeliveryError::InvalidConfiguration)));
        assert!(matches!(DeliveryWorker::new(queue, "https://user:password@example.invalid/test", test_config()), Err(DeliveryError::InvalidConfiguration)));
    }

    #[test]
    fn status_and_retry_delay_classification() {
        for status in [408, 425, 429, 500, 503, 599] { assert!(retryable(Some(status))); }
        for status in [200, 301, 400, 401, 403, 404, 422] { assert!(!retryable(Some(status))); }
        assert!(retryable(None));
        assert_eq!(retry_after_delay_ms("120", 1000), Some(120_000));
        assert_eq!(retry_after_delay_ms("Thu, 01 Jan 1970 00:00:02 GMT", 1000), Some(1000));
        assert_eq!(retry_after_delay_ms("Thu, 01 Jan 1970 00:00:00 GMT", 1000), Some(0));
        assert_eq!(retry_after_delay_ms("Fri, 30 Feb 2024 12:00:00 GMT", 0), None);
        assert_eq!(retry_after_delay_ms("-1", 0), None);
        assert_eq!(retry_after_delay_ms("1.5", 0), None);
        assert_eq!(retry_after_delay_ms("invalid", 0), None);
        let config = test_config();
        for attempt in 1..100 {
            let delay = backoff_ms(&config, attempt);
            assert!(delay > 0 && delay <= config.max_retry_delay.as_millis() as u64);
        }
    }
}
