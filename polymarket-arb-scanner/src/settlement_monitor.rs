use crate::config::Config;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

pub const SETTLEMENT_RECEIPTS_FILE: &str = "settlement_receipts.jsonl";
pub const SETTLEMENT_HAZARD_REPORT_FILE: &str = "settlement_hazard_report.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SettlementReceiptRecord {
    pub generated_at: Option<String>,
    pub source: Option<String>,
    pub route: Option<String>,
    pub transaction_hash: Option<String>,
    pub order_hash: Option<String>,
    pub rfq_id: Option<String>,
    pub quote_id: Option<String>,
    pub maker_id: Option<String>,
    pub counterparty_id: Option<String>,
    pub block_number: Option<u64>,
    pub status: String,
    pub success: bool,
    pub revert_reason: Option<String>,
    pub failure_category: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementFailureCategorySummary {
    pub category: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SettlementHazardReport {
    pub generated_at: String,
    pub receipts_path: String,
    pub monitor_enabled: bool,
    pub max_receipt_age_secs: u64,
    pub min_samples: usize,
    pub max_revert_rate: f64,
    pub raw_records_seen: usize,
    pub malformed_records: usize,
    pub stale_records: usize,
    pub recent_records: usize,
    pub successful_receipts: usize,
    pub failed_receipts: usize,
    pub failed_receipt_categories: Vec<SettlementFailureCategorySummary>,
    pub revert_rate: f64,
    pub latest_receipt_at: Option<String>,
    pub status: String,
    pub blockers: Vec<String>,
}

pub fn build_settlement_hazard_report(config: &Config) -> Result<SettlementHazardReport> {
    let receipts_path = config.diagnostics_dir.join(SETTLEMENT_RECEIPTS_FILE);
    let (raw_records_seen, malformed_records, records) = read_settlement_receipts(&receipts_path)?;
    Ok(build_settlement_hazard_report_from_records(
        config,
        &receipts_path,
        raw_records_seen,
        malformed_records,
        &records,
    ))
}

pub fn write_settlement_hazard_report(config: &Config) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let report = build_settlement_hazard_report(config)?;
    let path = config.diagnostics_dir.join(SETTLEMENT_HAZARD_REPORT_FILE);
    fs::write(&path, serde_json::to_string_pretty(&report)?)
        .with_context(|| format!("writing settlement hazard report {}", path.display()))?;
    Ok(path)
}

pub fn settlement_counterparty_blockers(config: &Config, counterparty_id: &str) -> Vec<String> {
    let counterparty_id = counterparty_id.trim();
    if counterparty_id.is_empty() {
        return Vec::new();
    }
    if !config.settlement_monitor_enabled {
        return vec![format!(
            "settlement_counterparty_monitor_disabled:{counterparty_id}"
        )];
    }
    let receipts_path = config.diagnostics_dir.join(SETTLEMENT_RECEIPTS_FILE);
    let Ok((_raw_records_seen, _malformed_records, records)) =
        read_settlement_receipts(&receipts_path)
    else {
        return vec![format!(
            "settlement_counterparty_receipts_unavailable:{counterparty_id}"
        )];
    };
    let now = Utc::now();
    let max_age_secs = config.settlement_receipt_max_age_secs.max(1);
    let recent_counterparty_records = records
        .iter()
        .filter(|record| record_has_scope_key(record))
        .filter(|record| record_matches_counterparty(record, counterparty_id))
        .filter(|record| {
            record
                .generated_at
                .as_deref()
                .and_then(parse_rfc3339_timestamp)
                .map(|timestamp| {
                    timestamp <= now + chrono::Duration::seconds(5)
                        && now.signed_duration_since(timestamp).num_seconds().max(0) as u64
                            <= max_age_secs
                })
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    let successful = recent_counterparty_records
        .iter()
        .filter(|record| record.success)
        .count();
    let failed = recent_counterparty_records
        .iter()
        .copied()
        .filter(|record| !record.success)
        .collect::<Vec<_>>();

    let mut blockers = Vec::new();
    let min_samples = config.combo_rfq_counterparty_min_settlement_samples.max(1);
    if successful < min_samples {
        blockers.push(format!(
            "settlement_counterparty_success_samples_insufficient:{counterparty_id}:{successful}<{min_samples}"
        ));
    }
    if !failed.is_empty() {
        let reason = failed
            .iter()
            .rev()
            .find_map(|record| record.revert_reason.as_deref())
            .unwrap_or("unknown");
        let categories = settlement_failure_category_summaries(failed.iter().copied())
            .into_iter()
            .map(|summary| format!("{}={}", summary.category, summary.count))
            .collect::<Vec<_>>()
            .join(",");
        blockers.push(format!(
            "settlement_counterparty_failed_recent:{counterparty_id}:{}:{categories}:{reason}",
            failed.len(),
        ));
    }
    blockers
}

fn build_settlement_hazard_report_from_records(
    config: &Config,
    receipts_path: &Path,
    raw_records_seen: usize,
    malformed_records: usize,
    records: &[SettlementReceiptRecord],
) -> SettlementHazardReport {
    let now = Utc::now();
    let max_age_secs = config.settlement_receipt_max_age_secs.max(1);
    let mut stale_records = 0usize;
    let mut future_records = 0usize;
    let mut unscoped_records = 0usize;
    let mut recent = Vec::new();
    for record in records {
        if !record_has_scope_key(record) {
            unscoped_records += 1;
            continue;
        }
        match record
            .generated_at
            .as_deref()
            .and_then(parse_rfc3339_timestamp)
        {
            Some(timestamp) if timestamp > now + chrono::Duration::seconds(5) => {
                future_records += 1;
            }
            Some(timestamp)
                if now.signed_duration_since(timestamp).num_seconds().max(0) as u64
                    > max_age_secs =>
            {
                stale_records += 1;
            }
            _ => recent.push(record),
        }
    }

    let successful_receipts = recent.iter().filter(|record| record.success).count();
    let failed_receipts = recent.len().saturating_sub(successful_receipts);
    let failed_receipt_categories = settlement_failure_category_summaries(
        recent.iter().filter(|record| !record.success).copied(),
    );
    let revert_rate = if recent.is_empty() {
        0.0
    } else {
        failed_receipts as f64 / recent.len() as f64
    };
    let latest_receipt_at = latest_receipt_timestamp(records);

    let mut blockers = Vec::new();
    if !config.settlement_monitor_enabled {
        blockers.push("SETTLEMENT_MONITOR_ENABLED=false".to_string());
    }
    if raw_records_seen == 0 {
        blockers.push("missing_settlement_receipts".to_string());
    }
    if malformed_records > 0 {
        blockers.push(format!("malformed_settlement_receipts:{malformed_records}"));
    }
    if unscoped_records > 0 {
        blockers.push(format!("unscoped_settlement_receipts:{unscoped_records}"));
    }
    if future_records > 0 {
        blockers.push(format!("future_settlement_receipts:{future_records}"));
    }
    let missing_recent_timestamps = recent
        .iter()
        .filter(|record| {
            record
                .generated_at
                .as_deref()
                .and_then(parse_rfc3339_timestamp)
                .is_none()
        })
        .count();
    if missing_recent_timestamps > 0 {
        blockers.push(format!(
            "settlement_receipt_timestamp_missing:{missing_recent_timestamps}"
        ));
    }
    if recent.len() < config.settlement_revert_hazard_min_samples {
        blockers.push(format!(
            "insufficient_settlement_receipts:{}/{}",
            recent.len(),
            config.settlement_revert_hazard_min_samples
        ));
    }
    if revert_rate > config.settlement_revert_hazard_max_rate {
        blockers.push(format!(
            "settlement_revert_rate_too_high:{revert_rate:.4}>{:.4}",
            config.settlement_revert_hazard_max_rate
        ));
    }

    SettlementHazardReport {
        generated_at: now.to_rfc3339(),
        receipts_path: receipts_path.display().to_string(),
        monitor_enabled: config.settlement_monitor_enabled,
        max_receipt_age_secs: max_age_secs,
        min_samples: config.settlement_revert_hazard_min_samples,
        max_revert_rate: config.settlement_revert_hazard_max_rate,
        raw_records_seen,
        malformed_records,
        stale_records,
        recent_records: recent.len(),
        successful_receipts,
        failed_receipts,
        failed_receipt_categories,
        revert_rate,
        latest_receipt_at,
        status: if blockers.is_empty() {
            "ready".to_string()
        } else {
            "blocked".to_string()
        },
        blockers,
    }
}

fn record_has_scope_key(record: &SettlementReceiptRecord) -> bool {
    record
        .transaction_hash
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
        || record
            .order_hash
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
        || (record
            .rfq_id
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
            && record
                .quote_id
                .as_deref()
                .map(str::trim)
                .is_some_and(|value| !value.is_empty()))
}

fn record_matches_counterparty(record: &SettlementReceiptRecord, counterparty_id: &str) -> bool {
    [
        record.maker_id.as_deref(),
        record.counterparty_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .any(|value| value.eq_ignore_ascii_case(counterparty_id))
}

fn read_settlement_receipts(path: &Path) -> Result<(usize, usize, Vec<SettlementReceiptRecord>)> {
    if !path.exists() {
        return Ok((0, 0, Vec::new()));
    }
    let file = File::open(path)
        .with_context(|| format!("opening settlement receipts {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut raw_records_seen = 0usize;
    let mut malformed_records = 0usize;
    let mut records = Vec::new();
    for line in reader.lines() {
        let line =
            line.with_context(|| format!("reading settlement receipts {}", path.display()))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        raw_records_seen += 1;
        match serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|value| normalize_settlement_receipt(&value))
        {
            Some(record) => records.push(record),
            None => malformed_records += 1,
        }
    }
    Ok((raw_records_seen, malformed_records, records))
}

fn normalize_settlement_receipt(value: &Value) -> Option<SettlementReceiptRecord> {
    let status = text_value(
        value,
        &[
            "status",
            "receiptStatus",
            "receipt_status",
            "executionStatus",
            "execution_status",
        ],
    )?;
    let success = settlement_status_is_success(&status)?;
    let revert_reason = text_value(value, &["revertReason", "revert_reason", "error", "reason"]);
    let failure_category = if success {
        None
    } else {
        Some(
            text_value(value, &["failureCategory", "failure_category", "category"])
                .unwrap_or_else(|| classify_settlement_failure(&status, revert_reason.as_deref()))
                .to_string(),
        )
    };
    Some(SettlementReceiptRecord {
        generated_at: text_value(value, &["generatedAt", "generated_at", "timestamp", "time"]),
        source: text_value(value, &["source"]),
        route: text_value(value, &["route"]),
        transaction_hash: text_value(value, &["transactionHash", "transaction_hash", "txHash"]),
        order_hash: text_value(value, &["orderHash", "order_hash", "orderId", "order_id"]),
        rfq_id: text_value(value, &["rfqId", "rfq_id"]),
        quote_id: text_value(value, &["quoteId", "quote_id"]),
        maker_id: text_value(value, &["makerId", "maker_id", "maker"]),
        counterparty_id: text_value(
            value,
            &["counterpartyId", "counterparty_id", "counterparty"],
        ),
        block_number: number_value(value, &["blockNumber", "block_number"]),
        status: normalize_status(&status),
        success,
        revert_reason,
        failure_category,
    })
}

fn settlement_failure_category_summaries<'a>(
    records: impl Iterator<Item = &'a SettlementReceiptRecord>,
) -> Vec<SettlementFailureCategorySummary> {
    let mut counts = BTreeMap::<String, usize>::new();
    for record in records {
        let category = record.failure_category.clone().unwrap_or_else(|| {
            classify_settlement_failure(&record.status, record.revert_reason.as_deref())
        });
        *counts.entry(category).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(category, count)| SettlementFailureCategorySummary { category, count })
        .collect()
}

fn classify_settlement_failure(status: &str, reason: Option<&str>) -> String {
    let text = format!("{} {}", status, reason.unwrap_or_default()).to_ascii_lowercase();
    if text.contains("allowance") || text.contains("approval") || text.contains("approved") {
        "allowance_revoked"
    } else if text.contains("nonce")
        || text.contains("cancel")
        || text.contains("cancelled")
        || text.contains("canceled")
        || text.contains("invalidated")
    {
        "nonce_or_cancel_invalidation"
    } else if text.contains("balance")
        || text.contains("insufficient")
        || text.contains("fund")
        || text.contains("collateral")
    {
        "balance_or_collateral_drain"
    } else if text.contains("proxy")
        || text.contains("1271")
        || text.contains("signature")
        || text.contains("safe")
    {
        "proxy_or_signature_trap"
    } else if text.contains("revert") {
        "generic_revert"
    } else {
        "unknown_failure"
    }
    .to_string()
}

fn settlement_status_is_success(status: &str) -> Option<bool> {
    match normalize_status(status).as_str() {
        "1" | "TRUE" | "SUCCESS" | "SUCCEEDED" | "CONFIRMED" | "MINED_SUCCESS" => Some(true),
        "0" | "FALSE" | "FAILED" | "FAILURE" | "REVERTED" | "REVERT" | "DROPPED"
        | "MINED_REVERTED" => Some(false),
        _ => None,
    }
}

fn normalize_status(status: &str) -> String {
    status.trim().to_ascii_uppercase().replace(['-', ' '], "_")
}

fn latest_receipt_timestamp(records: &[SettlementReceiptRecord]) -> Option<String> {
    records
        .iter()
        .filter_map(|record| {
            let raw = record.generated_at.as_deref()?;
            let parsed = parse_rfc3339_timestamp(raw)?;
            Some((parsed, raw.to_string()))
        })
        .max_by_key(|(timestamp, _)| *timestamp)
        .map(|(_, raw)| raw)
}

fn parse_rfc3339_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn text_value(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(text) = text_value_at(value, key) {
            return Some(text);
        }
        for container in ["payload", "data", "receipt", "transaction", "trade"] {
            if let Some(nested) = value
                .get(container)
                .and_then(|nested| text_value_at(nested, key))
            {
                return Some(nested);
            }
        }
    }
    None
}

fn text_value_at(value: &Value, key: &str) -> Option<String> {
    let field = value.get(key)?;
    let text = match field {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(value) => value.to_string(),
        _ => return None,
    };
    let trimmed = text.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn number_value(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(number) = number_value_at(value, key) {
            return Some(number);
        }
        for container in ["payload", "data", "receipt", "transaction", "trade"] {
            if let Some(nested) = value
                .get(container)
                .and_then(|nested| number_value_at(nested, key))
            {
                return Some(nested);
            }
        }
    }
    None
}

fn number_value_at(value: &Value, key: &str) -> Option<u64> {
    let field = value.get(key)?;
    match field {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-settlement-monitor-{name}-{suffix}"))
    }

    #[test]
    fn settlement_hazard_report_blocks_failed_recent_receipts() {
        let dir = temp_dir("failed");
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now().to_rfc3339();
        fs::write(
            dir.join(SETTLEMENT_RECEIPTS_FILE),
            format!(
                concat!(
                    r#"{{"generatedAt":"{now}","transactionHash":"0x1","status":"1"}}"#,
                    "\n",
                    r#"{{"generatedAt":"{now}","transactionHash":"0x2","status":"reverted","revertReason":"execution reverted"}}"#,
                    "\n"
                ),
                now = now
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.settlement_monitor_enabled = true;
        cfg.settlement_revert_hazard_min_samples = 2;
        cfg.settlement_revert_hazard_max_rate = 0.0;

        let report = build_settlement_hazard_report(&cfg).unwrap();

        assert_eq!(report.recent_records, 2);
        assert_eq!(report.successful_receipts, 1);
        assert_eq!(report.failed_receipts, 1);
        assert_eq!(
            report.failed_receipt_categories,
            vec![SettlementFailureCategorySummary {
                category: "generic_revert".into(),
                count: 1,
            }]
        );
        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("settlement_revert_rate_too_high:")));
    }

    #[test]
    fn settlement_hazard_report_ready_with_clean_receipts() {
        let dir = temp_dir("clean");
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now().to_rfc3339();
        fs::write(
            dir.join(SETTLEMENT_RECEIPTS_FILE),
            format!(
                concat!(
                    r#"{{"generatedAt":"{now}","transactionHash":"0x1","status":"success"}}"#,
                    "\n",
                    r#"{{"generatedAt":"{now}","transactionHash":"0x2","receiptStatus":1}}"#,
                    "\n"
                ),
                now = now
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.settlement_monitor_enabled = true;
        cfg.settlement_revert_hazard_min_samples = 2;

        let report = build_settlement_hazard_report(&cfg).unwrap();

        assert_eq!(report.status, "ready");
        assert_eq!(report.revert_rate, 0.0);
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn settlement_counterparty_blockers_flag_failed_recent_maker_receipts() {
        let dir = temp_dir("counterparty-failed");
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now().to_rfc3339();
        fs::write(
            dir.join(SETTLEMENT_RECEIPTS_FILE),
            format!(
                concat!(
                    r#"{{"generatedAt":"{now}","transactionHash":"0x1","makerId":"maker-bad","status":"reverted","revertReason":"allowance revoked"}}"#,
                    "\n",
                    r#"{{"generatedAt":"{now}","transactionHash":"0x2","makerId":"maker-good","status":"1"}}"#,
                    "\n"
                ),
                now = now
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.settlement_monitor_enabled = true;
        cfg.combo_rfq_counterparty_min_settlement_samples = 1;

        let blockers = settlement_counterparty_blockers(&cfg, "maker-bad");

        assert_eq!(blockers.len(), 2);
        assert!(blockers.iter().any(|blocker| blocker
            .contains("settlement_counterparty_success_samples_insufficient:maker-bad:0<1")));
        assert!(blockers.iter().any(|blocker| blocker
            .contains("settlement_counterparty_failed_recent:maker-bad:1:allowance_revoked=1:allowance revoked")));
        assert!(settlement_counterparty_blockers(&cfg, "maker-good").is_empty());
    }

    #[test]
    fn settlement_counterparty_blockers_require_enabled_monitor_and_success_samples() {
        let dir = temp_dir("counterparty-proof");
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now().to_rfc3339();
        fs::write(
            dir.join(SETTLEMENT_RECEIPTS_FILE),
            format!(
                r#"{{"generatedAt":"{now}","transactionHash":"0x1","makerId":"maker-good","status":"success"}}"#
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.combo_rfq_counterparty_min_settlement_samples = 2;

        let disabled = settlement_counterparty_blockers(&cfg, "maker-good");
        assert_eq!(
            disabled,
            vec!["settlement_counterparty_monitor_disabled:maker-good".to_string()]
        );

        cfg.settlement_monitor_enabled = true;
        let insufficient = settlement_counterparty_blockers(&cfg, "maker-good");
        assert_eq!(
            insufficient,
            vec!["settlement_counterparty_success_samples_insufficient:maker-good:1<2".to_string()]
        );
    }

    #[test]
    fn settlement_hazard_report_rejects_unscoped_receipts() {
        let dir = temp_dir("unscoped");
        fs::create_dir_all(&dir).unwrap();
        let now = Utc::now().to_rfc3339();
        fs::write(
            dir.join(SETTLEMENT_RECEIPTS_FILE),
            format!(
                concat!(
                    r#"{{"generatedAt":"{now}","status":"success"}}"#,
                    "\n",
                    r#"{{"generatedAt":"{now}","status":"success"}}"#,
                    "\n"
                ),
                now = now
            ),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.settlement_monitor_enabled = true;
        cfg.settlement_revert_hazard_min_samples = 1;

        let report = build_settlement_hazard_report(&cfg).unwrap();

        assert_eq!(report.recent_records, 0);
        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .contains(&"unscoped_settlement_receipts:2".to_string()));
        assert!(report
            .blockers
            .contains(&"insufficient_settlement_receipts:0/1".to_string()));
    }

    #[test]
    fn settlement_hazard_report_rejects_future_receipts() {
        let dir = temp_dir("future");
        fs::create_dir_all(&dir).unwrap();
        let future = (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
        fs::write(
            dir.join(SETTLEMENT_RECEIPTS_FILE),
            format!(r#"{{"generatedAt":"{future}","transactionHash":"0x1","status":"success"}}"#),
        )
        .unwrap();
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir;
        cfg.settlement_monitor_enabled = true;
        cfg.settlement_revert_hazard_min_samples = 1;

        let report = build_settlement_hazard_report(&cfg).unwrap();

        assert_eq!(report.recent_records, 0);
        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .contains(&"future_settlement_receipts:1".to_string()));
        assert!(report
            .blockers
            .contains(&"insufficient_settlement_receipts:0/1".to_string()));
    }
}
