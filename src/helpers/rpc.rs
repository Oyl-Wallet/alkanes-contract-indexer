use anyhow::Result;
use deezel_common::traits::{DeezelProvider, JsonRpcProvider};
use once_cell::sync::Lazy;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{debug, warn, error};
use std::future::Future;

// Global concurrency cap for outbound RPCs (shared across threads)
static GLOBAL_PERMITS: Lazy<Semaphore> = Lazy::new(|| {
    let permits = std::env::var("RPC_MAX_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1 && n <= 1024)
        .unwrap_or(64);
    Semaphore::new(permits)
});

// Simple circuit breaker (half-open after cool-down)
static CB_OPEN: AtomicBool = AtomicBool::new(false);
static CB_LAST_OPEN_MS: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 { (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()) as u64 }

fn circuit_open() -> bool {
    if !CB_OPEN.load(Ordering::Relaxed) { return false; }
    let cool_ms = std::env::var("RPC_CIRCUIT_COOLDOWN_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(5_000);
    let last = CB_LAST_OPEN_MS.load(Ordering::Relaxed);
    let age = now_ms().saturating_sub(last);
    if age >= cool_ms {
        // transition to half-open (allow a trial call)
        CB_OPEN.store(false, Ordering::Relaxed);
        return false;
    }
    true
}

fn trip_circuit() {
    CB_OPEN.store(true, Ordering::Relaxed);
    CB_LAST_OPEN_MS.store(now_ms(), Ordering::Relaxed);
}

/// Resilient JSON-RPC call with timeout, retries, exponential backoff, concurrency cap, and circuit breaker
pub async fn resilient_call<P: DeezelProvider + JsonRpcProvider + Send + Sync>(
    provider: &P,
    url: &str,
    method: &str,
    params: JsonValue,
    id: u64,
) -> Result<JsonValue> {
    if circuit_open() {
        return Err(anyhow::anyhow!("rpc_circuit_open"));
    }

    let _permit = GLOBAL_PERMITS.acquire().await.expect("semaphore poisoned");

    let max_attempts = std::env::var("RPC_MAX_RETRIES").ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(5);
    let base_delay_ms = std::env::var("RPC_BASE_BACKOFF_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(200);
    let max_delay_ms = std::env::var("RPC_MAX_BACKOFF_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(5_000);
    let timeout_ms = std::env::var("RPC_TIMEOUT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(20_000);

    let mut attempt: u32 = 0;
    let start = Instant::now();
    loop {
        attempt += 1;
        let call_fut = provider.call(url, method, params.clone(), id);
        match timeout(Duration::from_millis(timeout_ms), call_fut).await {
            Ok(Ok(val)) => {
                debug!(%method, attempt, elapsed_ms = start.elapsed().as_millis() as u64, "rpc ok");
                return Ok(val);
            }
            Ok(Err(e)) => {
                warn!(%method, attempt, error = %e, "rpc error");
                // Heuristic: trip circuit on network-wide symptoms
                let msg = format!("{e}").to_ascii_lowercase();
                if msg.contains("connection") || msg.contains("timeout") || msg.contains("too many requests") || msg.contains("503") {
                    // continue with backoff
                }
            }
            Err(_elapsed) => {
                warn!(%method, attempt, "rpc timeout");
            }
        }

        if attempt >= max_attempts {
            error!(%method, attempts = attempt, "rpc failed; opening circuit");
            trip_circuit();
            return Err(anyhow::anyhow!("rpc_failed_after_retries"));
        }

        let jitter = fastrand::u64(0..base_delay_ms);
        let delay = ((base_delay_ms as u128) * (1u128 << (attempt - 1) as usize) + jitter as u128)
            .min(max_delay_ms as u128) as u64;
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}

/// Generic resilient wrapper for provider operations that return Result<T>
/// Applies the same timeout/retry/backoff, semaphore, and circuit logic.
pub async fn resilient_provider_call<T, F, Fut, E>(label: &str, op: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    if circuit_open() {
        return Err(anyhow::anyhow!("rpc_circuit_open"));
    }

    let _permit = GLOBAL_PERMITS.acquire().await.expect("semaphore poisoned");

    let max_attempts = std::env::var("RPC_MAX_RETRIES").ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(5);
    let base_delay_ms = std::env::var("RPC_BASE_BACKOFF_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(200);
    let max_delay_ms = std::env::var("RPC_MAX_BACKOFF_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(5_000);
    let timeout_ms = std::env::var("RPC_TIMEOUT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(20_000);

    let mut attempt: u32 = 0;
    let start = Instant::now();
    loop {
        attempt += 1;
        let fut = op();
        match timeout(Duration::from_millis(timeout_ms), fut).await {
            Ok(Ok(val)) => {
                debug!(method = %label, attempt, elapsed_ms = start.elapsed().as_millis() as u64, "provider op ok");
                return Ok(val);
            }
            Ok(Err(e)) => {
                warn!(method = %label, attempt, error = %e, "provider op error");
            }
            Err(_elapsed) => {
                warn!(method = %label, attempt, "provider op timeout");
            }
        }

        if attempt >= max_attempts {
            error!(method = %label, attempts = attempt, "provider op failed; opening circuit");
            trip_circuit();
            return Err(anyhow::anyhow!("provider_failed_after_retries"));
        }

        let jitter = fastrand::u64(0..base_delay_ms);
        let delay = ((base_delay_ms as u128) * (1u128 << (attempt - 1) as usize) + jitter as u128)
            .min(max_delay_ms as u128) as u64;
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}


