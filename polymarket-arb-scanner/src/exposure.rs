//! Live-position exposure tracker.
//!
//! Tracks reserved USD exposure per event and in total across all live-executed arb legs.
//! Thread-safe via `tokio::sync::Mutex`, shared via `Arc`.
//!
//! The live executor intentionally keeps exposure reserved after a basket is
//! submitted or filled, because the project does not yet have a settlement/
//! close-out subsystem that can prove the economic exposure has been removed.
//! Release reservations only after an order is cancelled, a position is closed,
//! or a downstream reconciliation step confirms the exposure is gone.
//!
//! Usage:
//! ```
//! let tracker = Arc::new(ExposureTracker::new());
//! // Before placing orders:
//! tracker.check_and_reserve_with_total(&event_id, position_usd, max_event_exposure, max_total_exposure).await?;
//! // After orders are cancelled or a downstream reconciler closes the position:
//! tracker.release(&event_id, position_usd).await;
//! ```

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

const LIVE_EXPOSURE_LEDGER_FILE: &str = "live_exposure_ledger.jsonl";

/// Shared, cloneable handle to the exposure tracker.
pub type SharedExposureTracker = Arc<ExposureTracker>;

/// Tracks open USD exposure keyed by event_id.
#[derive(Debug, Default)]
pub struct ExposureTracker {
    inner: Mutex<HashMap<String, f64>>,
    ledger_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ExposureLedgerRecord {
    timestamp: String,
    event_id: String,
    delta_usd: f64,
    state: String,
    source: String,
}

impl ExposureTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_ledger(root_dir: &Path) -> Result<Self> {
        fs::create_dir_all(root_dir)
            .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
        let ledger_path = root_dir.join(LIVE_EXPOSURE_LEDGER_FILE);
        let snapshot = read_exposure_ledger_snapshot(&ledger_path)?;
        Ok(Self {
            inner: Mutex::new(snapshot),
            ledger_path: Some(ledger_path),
        })
    }

    /// Returns the current total exposure for an event (USD).
    #[cfg(test)]
    pub async fn current(&self, event_id: &str) -> f64 {
        let map = self.inner.lock().await;
        map.get(event_id).copied().unwrap_or(0.0)
    }

    #[cfg(test)]
    pub async fn check_and_reserve(
        &self,
        event_id: &str,
        amount_usd: f64,
        max_usd: f64,
    ) -> Result<()> {
        self.check_and_reserve_with_total(event_id, amount_usd, max_usd, f64::INFINITY)
            .await
    }

    /// Atomically checks whether adding `amount_usd` would exceed either the
    /// per-event cap or total exposure cap, and if not, reserves the amount.
    ///
    /// Returns an error (without recording anything) if either cap would be breached.
    pub async fn check_and_reserve_with_total(
        &self,
        event_id: &str,
        amount_usd: f64,
        max_event_usd: f64,
        max_total_usd: f64,
    ) -> Result<()> {
        if !amount_usd.is_finite() || amount_usd <= 0.0 {
            bail!(
                "exposure reservation amount must be positive and finite, got {}",
                amount_usd
            );
        }
        if !max_event_usd.is_finite() || max_event_usd <= 0.0 {
            bail!(
                "event exposure cap must be positive and finite, got {}",
                max_event_usd
            );
        }
        if max_total_usd.is_nan() || max_total_usd <= 0.0 {
            bail!(
                "total exposure cap must be positive or infinite, got {}",
                max_total_usd
            );
        }

        let mut map = self.inner.lock().await;
        self.refresh_from_ledger_locked(&mut map)?;
        let current_event = map.get(event_id).copied().unwrap_or(0.0);
        let new_event_total = current_event + amount_usd;

        if new_event_total > max_event_usd {
            bail!(
                "exposure cap breach: event={} current=${:.2} + new=${:.2} = ${:.2} > max=${:.2}",
                event_id,
                current_event,
                amount_usd,
                new_event_total,
                max_event_usd,
            );
        }

        let current_total: f64 = map.values().sum();
        let new_total = current_total + amount_usd;
        if new_total > max_total_usd {
            bail!(
                "total exposure cap breach: current=${:.2} + new=${:.2} = ${:.2} > max=${:.2}",
                current_total,
                amount_usd,
                new_total,
                max_total_usd,
            );
        }

        self.append_ledger_delta(event_id, amount_usd, "reserved")?;
        *map.entry(event_id.to_string()).or_insert(0.0) += amount_usd;
        debug!(
            "Exposure reserved: event={} added=${:.2} event_total=${:.2} total=${:.2}",
            event_id, amount_usd, new_event_total, new_total
        );
        Ok(())
    }

    /// Release (reduce) exposure for an event after a position is closed or cancelled.
    pub async fn release(&self, event_id: &str, amount_usd: f64) {
        if !amount_usd.is_finite() || amount_usd <= 0.0 {
            return;
        }
        let mut map = self.inner.lock().await;
        if let Err(err) = self.refresh_from_ledger_locked(&mut map) {
            warn!("failed to refresh exposure ledger before release event={event_id}: {err:#}");
        }
        let current = map.get(event_id).copied().unwrap_or(0.0);
        if current <= f64::EPSILON {
            return;
        }
        let released = amount_usd.min(current);
        let remaining = (current - released).max(0.0);
        debug!(
            "Exposure released: event={} released=${:.2} remaining=${:.2}",
            event_id, released, remaining
        );
        if remaining <= f64::EPSILON {
            map.remove(event_id);
        } else {
            map.insert(event_id.to_string(), remaining);
        }
        if let Err(err) = self.append_ledger_delta(event_id, -released, "released") {
            warn!(
                "failed to persist exposure release for event={} amount=${:.2}: {err:#}",
                event_id, released
            );
        }
    }

    /// Snapshot of all current exposure positions (for logging/summary).
    #[cfg(test)]
    pub async fn snapshot(&self) -> HashMap<String, f64> {
        self.inner.lock().await.clone()
    }

    /// Current total retained exposure across all events (USD).
    #[cfg(test)]
    pub async fn total(&self) -> f64 {
        self.inner.lock().await.values().sum()
    }

    fn append_ledger_delta(&self, event_id: &str, delta_usd: f64, state: &str) -> Result<()> {
        let Some(path) = &self.ledger_path else {
            return Ok(());
        };
        append_exposure_ledger_delta_path(path, event_id, delta_usd, state, "exposure_tracker")
    }

    fn refresh_from_ledger_locked(&self, map: &mut HashMap<String, f64>) -> Result<()> {
        let Some(path) = &self.ledger_path else {
            return Ok(());
        };
        *map = read_exposure_ledger_snapshot(path)?;
        Ok(())
    }
}

pub fn append_exposure_ledger_delta(
    root_dir: &Path,
    event_id: &str,
    delta_usd: f64,
    state: &str,
    source: &str,
) -> Result<PathBuf> {
    fs::create_dir_all(root_dir)
        .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
    let path = root_dir.join(LIVE_EXPOSURE_LEDGER_FILE);
    append_exposure_ledger_delta_path(&path, event_id, delta_usd, state, source)?;
    Ok(path)
}

fn append_exposure_ledger_delta_path(
    path: &Path,
    event_id: &str,
    delta_usd: f64,
    state: &str,
    source: &str,
) -> Result<()> {
    if event_id.trim().is_empty() {
        bail!("exposure ledger event_id must be non-empty");
    }
    if !delta_usd.is_finite() || delta_usd.abs() <= f64::EPSILON {
        bail!("exposure ledger delta must be finite and non-zero");
    }
    let record = ExposureLedgerRecord {
        timestamp: Utc::now().to_rfc3339(),
        event_id: event_id.to_string(),
        delta_usd,
        state: state.to_string(),
        source: source.to_string(),
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening exposure ledger {}", path.display()))?;
    serde_json::to_writer(&mut file, &record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn read_exposure_ledger_snapshot(path: &Path) -> Result<HashMap<String, f64>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let file =
        File::open(path).with_context(|| format!("opening exposure ledger {}", path.display()))?;
    let mut snapshot: HashMap<String, f64> = HashMap::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "reading exposure ledger {} line {}",
                path.display(),
                idx + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let record: ExposureLedgerRecord = serde_json::from_str(&line).with_context(|| {
            format!(
                "parsing exposure ledger {} line {}",
                path.display(),
                idx + 1
            )
        })?;
        if !record.delta_usd.is_finite() {
            bail!(
                "exposure ledger {} line {} has non-finite delta",
                path.display(),
                idx + 1
            );
        }
        let entry = snapshot.entry(record.event_id).or_insert(0.0);
        *entry = (*entry + record.delta_usd).max(0.0);
    }
    snapshot.retain(|_, amount| *amount > f64::EPSILON);
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_exposure_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-exposure-{name}-{suffix}"))
    }

    #[tokio::test]
    async fn test_reserve_and_release() {
        let tracker = ExposureTracker::new();
        tracker
            .check_and_reserve("event-1", 100.0, 200.0)
            .await
            .unwrap();
        assert!((tracker.current("event-1").await - 100.0).abs() < f64::EPSILON);

        tracker
            .check_and_reserve("event-1", 50.0, 200.0)
            .await
            .unwrap();
        assert!((tracker.current("event-1").await - 150.0).abs() < f64::EPSILON);

        tracker.release("event-1", 150.0).await;
        assert_eq!(tracker.current("event-1").await, 0.0);
        // Key should be cleaned up
        assert!(tracker.snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn test_invalid_inputs_rejected() {
        let tracker = ExposureTracker::new();
        assert!(tracker
            .check_and_reserve("event-bad", 0.0, 10.0)
            .await
            .is_err());
        assert!(tracker
            .check_and_reserve("event-bad", f64::NAN, 10.0)
            .await
            .is_err());
        assert!(tracker
            .check_and_reserve("event-bad", 1.0, 0.0)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_cap_breach_rejected() {
        let tracker = ExposureTracker::new();
        tracker
            .check_and_reserve("event-2", 150.0, 200.0)
            .await
            .unwrap();
        // This would push to 300 > 200
        let result = tracker.check_and_reserve("event-2", 150.0, 200.0).await;
        assert!(result.is_err());
        // Exposure must NOT have changed
        assert!((tracker.current("event-2").await - 150.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_independent_events() {
        let tracker = ExposureTracker::new();
        tracker
            .check_and_reserve("event-a", 200.0, 200.0)
            .await
            .unwrap();
        // Different event is independent
        tracker
            .check_and_reserve("event-b", 200.0, 200.0)
            .await
            .unwrap();
        assert!((tracker.current("event-b").await - 200.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_total_cap_breach_rejected_without_mutation() {
        let tracker = ExposureTracker::new();
        tracker
            .check_and_reserve_with_total("event-a", 200.0, 500.0, 300.0)
            .await
            .unwrap();

        let result = tracker
            .check_and_reserve_with_total("event-b", 150.0, 500.0, 300.0)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("total exposure cap breach"));
        assert!((tracker.current("event-a").await - 200.0).abs() < f64::EPSILON);
        assert_eq!(tracker.current("event-b").await, 0.0);
        assert!((tracker.total().await - 200.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn ledger_rehydrates_retained_exposure() {
        let dir = temp_exposure_dir("rehydrate");
        let tracker = ExposureTracker::new_with_ledger(&dir).unwrap();
        tracker
            .check_and_reserve_with_total("event-a", 125.0, 200.0, 500.0)
            .await
            .unwrap();
        tracker
            .check_and_reserve_with_total("event-b", 50.0, 200.0, 500.0)
            .await
            .unwrap();

        let rehydrated = ExposureTracker::new_with_ledger(&dir).unwrap();

        assert!((rehydrated.current("event-a").await - 125.0).abs() < f64::EPSILON);
        assert!((rehydrated.current("event-b").await - 50.0).abs() < f64::EPSILON);
        assert!((rehydrated.total().await - 175.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn ledger_replays_release_and_preserves_total_cap() {
        let dir = temp_exposure_dir("release");
        let tracker = ExposureTracker::new_with_ledger(&dir).unwrap();
        tracker
            .check_and_reserve_with_total("event-a", 125.0, 200.0, 500.0)
            .await
            .unwrap();
        tracker.release("event-a", 100.0).await;

        let rehydrated = ExposureTracker::new_with_ledger(&dir).unwrap();

        assert!((rehydrated.current("event-a").await - 25.0).abs() < f64::EPSILON);
        let result = rehydrated
            .check_and_reserve_with_total("event-b", 80.0, 200.0, 100.0)
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("total exposure cap breach"));
    }

    #[tokio::test]
    async fn ledger_refreshes_before_reserve_after_external_release() {
        let dir = temp_exposure_dir("external-release-refresh");
        let tracker = ExposureTracker::new_with_ledger(&dir).unwrap();
        tracker
            .check_and_reserve_with_total("event-a", 100.0, 200.0, 100.0)
            .await
            .unwrap();
        append_exposure_ledger_delta(&dir, "event-a", -100.0, "released", "rfq_finality").unwrap();

        tracker
            .check_and_reserve_with_total("event-b", 80.0, 200.0, 100.0)
            .await
            .unwrap();

        assert_eq!(tracker.current("event-a").await, 0.0);
        assert!((tracker.current("event-b").await - 80.0).abs() < f64::EPSILON);
        let rehydrated = ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(rehydrated.current("event-a").await, 0.0);
        assert!((rehydrated.current("event-b").await - 80.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn release_refreshes_ledger_and_skips_already_released_event() {
        let dir = temp_exposure_dir("external-release-before-release");
        let tracker = ExposureTracker::new_with_ledger(&dir).unwrap();
        tracker
            .check_and_reserve_with_total("event-a", 100.0, 200.0, 100.0)
            .await
            .unwrap();
        append_exposure_ledger_delta(&dir, "event-a", -100.0, "released", "rfq_finality").unwrap();

        tracker.release("event-a", 100.0).await;

        assert_eq!(tracker.current("event-a").await, 0.0);
        let body = fs::read_to_string(dir.join(LIVE_EXPOSURE_LEDGER_FILE)).unwrap();
        assert_eq!(body.lines().count(), 2);
        let rehydrated = ExposureTracker::new_with_ledger(&dir).unwrap();
        assert_eq!(rehydrated.current("event-a").await, 0.0);
    }

    #[tokio::test]
    async fn ledger_refreshes_before_reserve_after_external_reserve() {
        let dir = temp_exposure_dir("external-reserve-refresh");
        let tracker = ExposureTracker::new_with_ledger(&dir).unwrap();
        append_exposure_ledger_delta(&dir, "event-a", 90.0, "reserved", "rfq_finality").unwrap();

        let result = tracker
            .check_and_reserve_with_total("event-b", 20.0, 200.0, 100.0)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("total exposure cap breach"));
        assert!((tracker.current("event-a").await - 90.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_concurrent_reservations() {
        let tracker = Arc::new(ExposureTracker::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let t = Arc::clone(&tracker);
                tokio::spawn(async move {
                    // Each tries to add $25 with a $200 cap — only 8 should succeed
                    t.check_and_reserve("event-conc", 25.0, 200.0).await
                })
            })
            .collect();

        let mut successes = 0usize;
        for h in handles {
            if h.await.unwrap().is_ok() {
                successes += 1;
            }
        }

        // Exactly 8 fits under $200 cap ($25 × 8 = $200)
        assert_eq!(successes, 8);
        let exp = tracker.current("event-conc").await;
        assert!((exp - 200.0).abs() < f64::EPSILON);
    }
}
