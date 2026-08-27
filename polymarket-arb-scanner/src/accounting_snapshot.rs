//! Polymarket Data API accounting snapshot checks.
//!
//! The Data API exposes a ZIP containing positions.csv and equity.csv. This
//! module parses that ZIP as an independent source of account state before live
//! execution is allowed to retain or add exposure.

use std::fs;
use std::io::{Cursor, Read};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use polymarket_client_sdk_v2::types::Address;
use reqwest::header::ACCEPT;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use zip::ZipArchive;

use crate::config::Config;

pub const ACCOUNTING_SNAPSHOT_REPORT_FILE: &str = "accounting_snapshot_report.json";
const POSITION_DUST: f64 = 0.0000001;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountingCsvSummary {
    pub file_name: String,
    pub headers: Vec<String>,
    pub rows: usize,
    pub nonempty_rows: usize,
    pub numeric_cells: usize,
    pub numeric_abs_sum: f64,
    pub quantity_columns: Vec<String>,
    pub exposure_rows: usize,
    pub exposure_quantity_abs_sum: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountingSnapshotReport {
    pub generated_at: String,
    pub account_address: String,
    pub endpoint: String,
    pub zip_bytes: usize,
    pub positions: AccountingCsvSummary,
    pub equity: AccountingCsvSummary,
    pub status: String,
    pub blockers: Vec<String>,
}

impl AccountingSnapshotReport {
    pub fn blocks_live(&self) -> bool {
        !self.blockers.is_empty()
    }
}

pub fn accounting_snapshot_endpoint(config: &Config, account_address: Address) -> String {
    format!(
        "{}/v1/accounting/snapshot?user={}",
        config.polymarket_data_api_url.trim_end_matches('/'),
        account_address
    )
}

pub async fn fetch_accounting_snapshot_report(
    http: &Client,
    config: &Config,
    account_address: Address,
) -> Result<AccountingSnapshotReport> {
    let endpoint = accounting_snapshot_endpoint(config, account_address);
    let response = http
        .get(&endpoint)
        .header(ACCEPT, "application/zip")
        .timeout(std::time::Duration::from_secs(
            config.api_timeout_secs.max(1),
        ))
        .send()
        .await
        .with_context(|| format!("fetching Polymarket accounting snapshot {endpoint}"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("reading Polymarket accounting snapshot response {endpoint}"))?;
    if !status.is_success() {
        let sample = String::from_utf8_lossy(&body);
        bail!(
            "Polymarket accounting snapshot failed status={} body={}",
            status,
            sample.chars().take(256).collect::<String>()
        );
    }

    build_accounting_snapshot_report_from_zip(
        account_address,
        endpoint,
        &body,
        config.live_accounting_snapshot_max_position_rows,
    )
}

pub async fn fetch_and_write_accounting_snapshot_report(
    http: &Client,
    config: &Config,
    account_address: Address,
) -> Result<AccountingSnapshotReport> {
    let report = fetch_accounting_snapshot_report(http, config, account_address).await?;
    write_accounting_snapshot_report(config, &report)?;
    Ok(report)
}

pub fn write_accounting_snapshot_report(
    config: &Config,
    report: &AccountingSnapshotReport,
) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let path = config.diagnostics_dir.join(ACCOUNTING_SNAPSHOT_REPORT_FILE);
    fs::write(&path, serde_json::to_string_pretty(report)?)
        .with_context(|| format!("writing accounting snapshot report {}", path.display()))?;
    Ok(path)
}

pub fn build_accounting_snapshot_report_from_zip(
    account_address: Address,
    endpoint: impl Into<String>,
    zip_bytes: &[u8],
    max_position_rows: usize,
) -> Result<AccountingSnapshotReport> {
    let endpoint = endpoint.into();
    let mut archive = ZipArchive::new(Cursor::new(zip_bytes))
        .context("parsing Polymarket accounting snapshot ZIP")?;
    let positions_body = read_zip_member(&mut archive, "positions.csv")?;
    let equity_body = read_zip_member(&mut archive, "equity.csv")?;
    let positions = summarize_csv("positions.csv", &positions_body, true)?;
    let equity = summarize_csv("equity.csv", &equity_body, false)?;

    let mut blockers = Vec::new();
    if positions.exposure_rows > max_position_rows {
        blockers.push(format!(
            "accounting_snapshot_position_rows_exceed_limit:{}>{}",
            positions.exposure_rows, max_position_rows
        ));
    }

    let status = if blockers.is_empty() {
        "clean".to_string()
    } else {
        "blocked".to_string()
    };

    Ok(AccountingSnapshotReport {
        generated_at: Utc::now().to_rfc3339(),
        account_address: account_address.to_string(),
        endpoint,
        zip_bytes: zip_bytes.len(),
        positions,
        equity,
        status,
        blockers,
    })
}

fn read_zip_member(archive: &mut ZipArchive<Cursor<&[u8]>>, target: &str) -> Result<String> {
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .with_context(|| format!("reading accounting snapshot ZIP member {index}"))?;
        if !file.is_file() {
            continue;
        }
        let name = file.name().rsplit('/').next().unwrap_or(file.name());
        if name != target {
            continue;
        }
        let mut body = String::new();
        file.read_to_string(&mut body)
            .with_context(|| format!("reading accounting snapshot {target} as UTF-8 CSV"))?;
        return Ok(body);
    }
    bail!("accounting snapshot missing required ZIP member {target}");
}

fn summarize_csv(
    file_name: &str,
    body: &str,
    detect_position_exposure: bool,
) -> Result<AccountingCsvSummary> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_reader(body.as_bytes());
    let headers: Vec<String> = reader
        .headers()
        .with_context(|| format!("reading accounting snapshot {file_name} headers"))?
        .iter()
        .map(str::to_string)
        .collect();
    let quantity_indexes = if detect_position_exposure {
        position_quantity_indexes(&headers)
    } else {
        Vec::new()
    };
    let quantity_columns: Vec<String> = quantity_indexes
        .iter()
        .filter_map(|index| headers.get(*index).cloned())
        .collect();

    let mut rows = 0usize;
    let mut nonempty_rows = 0usize;
    let mut numeric_cells = 0usize;
    let mut numeric_abs_sum = 0.0;
    let mut exposure_rows = 0usize;
    let mut exposure_quantity_abs_sum = 0.0;

    for record in reader.records() {
        let record =
            record.with_context(|| format!("reading accounting snapshot {file_name} row"))?;
        rows += 1;
        let nonempty = record.iter().any(|field| !field.trim().is_empty());
        if nonempty {
            nonempty_rows += 1;
        }

        for field in record.iter() {
            if let Some(value) = parse_accounting_number(field) {
                numeric_cells += 1;
                numeric_abs_sum += value.abs();
            }
        }

        if !detect_position_exposure || !nonempty {
            continue;
        }
        if quantity_indexes.is_empty() {
            exposure_rows += 1;
            continue;
        }

        let mut row_has_quantity = false;
        for index in &quantity_indexes {
            let Some(value) = record.get(*index).and_then(parse_accounting_number) else {
                continue;
            };
            let abs = value.abs();
            exposure_quantity_abs_sum += abs;
            if abs > POSITION_DUST {
                row_has_quantity = true;
            }
        }
        if row_has_quantity {
            exposure_rows += 1;
        }
    }

    Ok(AccountingCsvSummary {
        file_name: file_name.to_string(),
        headers,
        rows,
        nonempty_rows,
        numeric_cells,
        numeric_abs_sum,
        quantity_columns,
        exposure_rows,
        exposure_quantity_abs_sum,
    })
}

fn position_quantity_indexes(headers: &[String]) -> Vec<usize> {
    headers
        .iter()
        .enumerate()
        .filter_map(|(index, header)| {
            let normalized = header.trim().to_ascii_lowercase().replace([' ', '-'], "_");
            let looks_like_quantity = normalized.contains("size")
                || normalized.contains("share")
                || normalized.contains("balance")
                || normalized.contains("quantity")
                || normalized == "qty";
            let looks_like_money = normalized.contains("price")
                || normalized.contains("value")
                || normalized.contains("usd")
                || normalized.contains("usdc")
                || normalized.contains("cost")
                || normalized.contains("pnl");
            if looks_like_quantity && !looks_like_money {
                Some(index)
            } else {
                None
            }
        })
        .collect()
}

fn parse_accounting_number(raw: &str) -> Option<f64> {
    let mut value = raw.trim();
    if value.is_empty() {
        return None;
    }
    let negative_parentheses = value.starts_with('(') && value.ends_with(')');
    if negative_parentheses {
        value = &value[1..value.len().saturating_sub(1)];
    }
    let normalized = value
        .trim_start_matches('$')
        .trim_end_matches('%')
        .replace(',', "");
    let parsed = normalized.parse::<f64>().ok()?;
    if !parsed.is_finite() {
        return None;
    }
    Some(if negative_parentheses {
        -parsed
    } else {
        parsed
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::str::FromStr as _;

    fn test_address() -> Address {
        Address::from_str("0x0000000000000000000000000000000000000001").unwrap()
    }

    fn snapshot_zip(files: &[(&str, &str)]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, body) in files {
            writer.start_file(*name, options).unwrap();
            writer.write_all(body.as_bytes()).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn clean_snapshot_accepts_header_only_positions() {
        let zip = snapshot_zip(&[
            ("positions.csv", "asset,size,current_value\n"),
            (
                "equity.csv",
                "timestamp,equity\n2026-01-01T00:00:00Z,100.25\n",
            ),
        ]);

        let report = build_accounting_snapshot_report_from_zip(
            test_address(),
            "https://data-api.polymarket.com/v1/accounting/snapshot?user=0x1",
            &zip,
            0,
        )
        .unwrap();

        assert_eq!(report.status, "clean");
        assert!(!report.blocks_live());
        assert_eq!(report.positions.rows, 0);
        assert_eq!(report.positions.exposure_rows, 0);
        assert_eq!(report.equity.rows, 1);
    }

    #[test]
    fn nonzero_position_quantity_blocks_live() {
        let zip = snapshot_zip(&[
            (
                "positions.csv",
                "asset,size,current_value\n123,0.010000,0.004\n",
            ),
            (
                "equity.csv",
                "timestamp,equity\n2026-01-01T00:00:00Z,100.25\n",
            ),
        ]);

        let report =
            build_accounting_snapshot_report_from_zip(test_address(), "test", &zip, 0).unwrap();

        assert_eq!(report.status, "blocked");
        assert_eq!(report.positions.exposure_rows, 1);
        assert_eq!(
            report.blockers,
            vec!["accounting_snapshot_position_rows_exceed_limit:1>0".to_string()]
        );
    }

    #[test]
    fn zero_position_quantity_does_not_block() {
        let zip = snapshot_zip(&[
            (
                "positions.csv",
                "asset,size,current_value\n123,0.00000000,0.000\n",
            ),
            (
                "equity.csv",
                "timestamp,equity\n2026-01-01T00:00:00Z,100.25\n",
            ),
        ]);

        let report =
            build_accounting_snapshot_report_from_zip(test_address(), "test", &zip, 0).unwrap();

        assert_eq!(report.status, "clean");
        assert_eq!(report.positions.exposure_rows, 0);
    }

    #[test]
    fn unknown_position_schema_blocks_nonempty_rows() {
        let zip = snapshot_zip(&[
            ("positions.csv", "asset,outcome\n123,YES\n"),
            (
                "equity.csv",
                "timestamp,equity\n2026-01-01T00:00:00Z,100.25\n",
            ),
        ]);

        let report =
            build_accounting_snapshot_report_from_zip(test_address(), "test", &zip, 0).unwrap();

        assert_eq!(report.status, "blocked");
        assert_eq!(report.positions.exposure_rows, 1);
    }

    #[test]
    fn missing_required_csv_member_fails_closed() {
        let zip = snapshot_zip(&[("positions.csv", "asset,size\n")]);

        let err =
            build_accounting_snapshot_report_from_zip(test_address(), "test", &zip, 0).unwrap_err();

        assert!(err
            .to_string()
            .contains("accounting snapshot missing required ZIP member equity.csv"));
    }
}
