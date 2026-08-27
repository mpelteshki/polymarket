use crate::config::Config;
use alloy::primitives::keccak256;
use anyhow::{Context, Result};
use chrono::Utc;
use polymarket_client_sdk_v2::types::{Address, B256, U256};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::str::FromStr;

const ORDER_FILLED_V1_EVENT_SIGNATURE: &str =
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)";
const ORDER_FILLED_V2_EVENT_SIGNATURE: &str =
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)";
const INDEXED_ORDER_FILLED_V1_DATA_WORDS: usize = 5;
const INDEXED_ORDER_FILLED_V2_DATA_WORDS: usize = 7;
const UNINDEXED_ORDER_FILLED_V1_DATA_WORDS: usize = 8;
const UNINDEXED_ORDER_FILLED_V2_DATA_WORDS: usize = 10;
pub const ORDER_FILLED_COLLECTOR_REPORT_FILE: &str = "onchain_order_filled_collector_report.json";
pub const ORDER_FILLED_COLLECTOR_RUN_REPORT_FILE: &str =
    "onchain_order_filled_collector_run_report.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnchainLogSummary {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
    pub transaction_hash: Option<String>,
    pub block_number: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderFilledEvent {
    pub protocol: String,
    pub exchange: Address,
    pub order_hash: B256,
    pub maker: Address,
    pub taker: Address,
    pub side: Option<u8>,
    pub token_id: Option<U256>,
    pub maker_asset_id: U256,
    pub taker_asset_id: U256,
    pub maker_amount_filled: U256,
    pub taker_amount_filled: U256,
    pub fee: U256,
    pub builder: Option<B256>,
    pub metadata: Option<B256>,
    pub transaction_hash: Option<String>,
    pub block_number: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderFilledReconciliationReport {
    pub logs_seen: usize,
    pub order_filled_logs: usize,
    pub decoded_order_filled_logs: usize,
    pub account_order_filled_logs: usize,
    pub events: Vec<OrderFilledEvent>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderFilledCollectorReport {
    pub generated_at: String,
    pub chain_id: u64,
    pub collector_enabled: bool,
    pub lookback_blocks: u64,
    pub rpc_url_present: bool,
    pub exchange_address: Option<String>,
    pub account_filter: Option<String>,
    pub v1_topic0: String,
    pub v2_topic0: String,
    pub suggested_eth_get_logs: Vec<OrderFilledCollectorFilter>,
    pub output_path: String,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderFilledCollectorRunReport {
    pub generated_at: String,
    pub chain_id: u64,
    pub latest_block: Option<u64>,
    pub finalized_block: Option<u64>,
    pub finalized_lag_blocks: Option<u64>,
    pub from_block: u64,
    pub to_block: u64,
    pub filters_sent: usize,
    pub raw_logs_fetched: usize,
    pub logs_appended: usize,
    pub decoded_order_filled_logs: usize,
    pub account_order_filled_logs: usize,
    pub output_path: String,
    pub report_path: String,
    pub status: String,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderFilledCollectorFilter {
    pub address: String,
    pub topic0: String,
    pub indexed_order_hash_topic: Option<String>,
    pub indexed_maker_topic: Option<String>,
    pub indexed_taker_topic: Option<String>,
}

#[cfg(test)]
pub fn order_filled_topic() -> B256 {
    order_filled_v1_topic()
}

pub fn order_filled_v1_topic() -> B256 {
    keccak256(ORDER_FILLED_V1_EVENT_SIGNATURE.as_bytes())
}

pub fn order_filled_v2_topic() -> B256 {
    keccak256(ORDER_FILLED_V2_EVENT_SIGNATURE.as_bytes())
}

pub fn build_order_filled_collector_report(config: &Config) -> OrderFilledCollectorReport {
    let rpc_url_present = !config.polygon_rpc_url.trim().is_empty();
    let exchange_address = Address::from_str(config.combo_rfq_exchange_v3_address.trim()).ok();
    let account_filter = configured_collector_account(config);
    let mut blockers = Vec::new();
    if !rpc_url_present {
        blockers.push("POLYGON_RPC_URL_empty".to_string());
    }
    if config.combo_rfq_exchange_v3_address.trim().is_empty() {
        blockers.push("COMBO_RFQ_EXCHANGE_V3_ADDRESS_empty".to_string());
    } else if exchange_address.is_none() {
        blockers.push(format!(
            "COMBO_RFQ_EXCHANGE_V3_ADDRESS_invalid:{}",
            config.combo_rfq_exchange_v3_address.trim()
        ));
    }
    if account_filter.is_none() {
        blockers.push("onchain_order_filled_account_filter_missing".to_string());
    }
    if !config.onchain_order_filled_collector_enabled {
        blockers.push("ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false".to_string());
    }

    let suggested_eth_get_logs = match (exchange_address, account_filter) {
        (Some(exchange), Some(account)) => vec![
            OrderFilledCollectorFilter {
                address: exchange.to_string(),
                topic0: order_filled_v2_topic().to_string(),
                indexed_order_hash_topic: None,
                indexed_maker_topic: Some(account.into_word().to_string()),
                indexed_taker_topic: None,
            },
            OrderFilledCollectorFilter {
                address: exchange.to_string(),
                topic0: order_filled_v2_topic().to_string(),
                indexed_order_hash_topic: None,
                indexed_maker_topic: None,
                indexed_taker_topic: Some(account.into_word().to_string()),
            },
        ],
        _ => Vec::new(),
    };

    OrderFilledCollectorReport {
        generated_at: Utc::now().to_rfc3339(),
        chain_id: config.live_chain_id,
        collector_enabled: config.onchain_order_filled_collector_enabled,
        lookback_blocks: config.onchain_order_filled_collector_lookback_blocks,
        rpc_url_present,
        exchange_address: exchange_address.map(|address| address.to_string()),
        account_filter: account_filter.map(|account| account.to_string()),
        v1_topic0: order_filled_v1_topic().to_string(),
        v2_topic0: order_filled_v2_topic().to_string(),
        suggested_eth_get_logs,
        output_path: config
            .diagnostics_dir
            .join(crate::rfq_finality::COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE)
            .display()
            .to_string(),
        status: if blockers.is_empty() {
            "ready_for_bounded_collector".to_string()
        } else {
            "blocked_no_collector".to_string()
        },
        blockers,
    }
}

pub fn write_order_filled_collector_report(config: &Config) -> Result<PathBuf> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let report = build_order_filled_collector_report(config);
    let path = config
        .diagnostics_dir
        .join(ORDER_FILLED_COLLECTOR_REPORT_FILE);
    fs::write(&path, serde_json::to_string_pretty(&report)?)
        .with_context(|| format!("writing OrderFilled collector report {}", path.display()))?;
    Ok(path)
}

pub async fn collect_recent_order_filled_logs(
    http: &Client,
    config: &Config,
) -> Result<OrderFilledCollectorRunReport> {
    let latest_block = fetch_rpc_block_by_tag(http, config, "latest").await?;
    let finalized_block = fetch_rpc_block_by_tag(http, config, "finalized").await?;
    if finalized_block > latest_block {
        anyhow::bail!(
            "finalized_block_ahead_of_latest finalized={} latest={}",
            finalized_block,
            latest_block
        );
    }
    let finalized_lag_blocks = latest_block.saturating_sub(finalized_block);
    let max_lag = config.polygon_finalized_block_max_lag_blocks.max(1);
    if finalized_lag_blocks > max_lag {
        anyhow::bail!(
            "finalized_block_lag_blocks={}>{} latest={} finalized={}",
            finalized_lag_blocks,
            max_lag,
            latest_block,
            finalized_block
        );
    }
    let lookback = config.onchain_order_filled_collector_lookback_blocks;
    let from_block = finalized_block.saturating_sub(lookback);
    collect_order_filled_logs_once_with_finality(
        http,
        config,
        from_block,
        finalized_block,
        Some(latest_block),
        Some(finalized_block),
        Some(finalized_lag_blocks),
    )
    .await
}

#[cfg(test)]
pub async fn collect_order_filled_logs_once(
    http: &Client,
    config: &Config,
    from_block: u64,
    to_block: u64,
) -> Result<OrderFilledCollectorRunReport> {
    collect_order_filled_logs_once_with_finality(
        http, config, from_block, to_block, None, None, None,
    )
    .await
}

async fn collect_order_filled_logs_once_with_finality(
    http: &Client,
    config: &Config,
    from_block: u64,
    to_block: u64,
    latest_block: Option<u64>,
    finalized_block: Option<u64>,
    finalized_lag_blocks: Option<u64>,
) -> Result<OrderFilledCollectorRunReport> {
    fs::create_dir_all(&config.diagnostics_dir).with_context(|| {
        format!(
            "creating diagnostics directory {}",
            config.diagnostics_dir.display()
        )
    })?;
    let output_path = config
        .diagnostics_dir
        .join(crate::rfq_finality::COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE);
    let report_path = config
        .diagnostics_dir
        .join(ORDER_FILLED_COLLECTOR_RUN_REPORT_FILE);
    let mut blockers = collector_runtime_blockers(config);
    if from_block > to_block {
        blockers.push(format!("invalid_block_range:{from_block}>{to_block}"));
    }

    let mut raw_logs = Vec::new();
    let mut filters_sent = 0usize;
    if blockers.is_empty() {
        let plan = build_order_filled_collector_report(config);
        for filter in &plan.suggested_eth_get_logs {
            filters_sent += 1;
            match fetch_order_filled_logs_for_filter(http, config, filter, from_block, to_block)
                .await
            {
                Ok(mut logs) => raw_logs.append(&mut logs),
                Err(err) => blockers.push(format!("eth_getLogs_failed:{err:#}")),
            }
        }
        dedupe_log_values(&mut raw_logs);
        if raw_logs.is_empty() {
            blockers.push("no_order_filled_logs_collected".to_string());
        }
    }

    let logs_appended = append_order_filled_log_values_deduped(&output_path, &raw_logs)?;
    let summaries = raw_logs
        .iter()
        .enumerate()
        .filter_map(|(idx, value)| match onchain_log_summary_from_value(value) {
            Ok(log) => Some(log),
            Err(err) => {
                blockers.push(format!("malformed_collected_order_filled_log:{idx}:{err}"));
                None
            }
        })
        .collect::<Vec<_>>();
    let reconciliation =
        build_order_filled_reconciliation_report(&summaries, configured_collector_account(config));
    for blocker in reconciliation.blockers {
        if blocker.starts_with("malformed_order_filled_log") {
            blockers.push(blocker);
        }
    }

    let report = OrderFilledCollectorRunReport {
        generated_at: Utc::now().to_rfc3339(),
        chain_id: config.live_chain_id,
        latest_block,
        finalized_block,
        finalized_lag_blocks,
        from_block,
        to_block,
        filters_sent,
        raw_logs_fetched: raw_logs.len(),
        logs_appended,
        decoded_order_filled_logs: reconciliation.decoded_order_filled_logs,
        account_order_filled_logs: reconciliation.account_order_filled_logs,
        output_path: output_path.display().to_string(),
        report_path: report_path.display().to_string(),
        status: if blockers.is_empty() {
            "collected".to_string()
        } else {
            "blocked".to_string()
        },
        blockers,
    };
    fs::write(&report_path, serde_json::to_string_pretty(&report)?).with_context(|| {
        format!(
            "writing OrderFilled collector run report {}",
            report_path.display()
        )
    })?;
    Ok(report)
}

fn configured_collector_account(config: &Config) -> Option<Address> {
    let raw = config.live_funder_address.trim();
    if raw.is_empty() {
        None
    } else {
        Address::from_str(raw).ok()
    }
}

fn collector_runtime_blockers(config: &Config) -> Vec<String> {
    let mut blockers = Vec::new();
    if !config.onchain_order_filled_collector_enabled {
        blockers.push("ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false".to_string());
    }
    if config.polygon_rpc_url.trim().is_empty() {
        blockers.push("POLYGON_RPC_URL_empty".to_string());
    }
    if config.combo_rfq_exchange_v3_address.trim().is_empty() {
        blockers.push("COMBO_RFQ_EXCHANGE_V3_ADDRESS_empty".to_string());
    } else if Address::from_str(config.combo_rfq_exchange_v3_address.trim()).is_err() {
        blockers.push(format!(
            "COMBO_RFQ_EXCHANGE_V3_ADDRESS_invalid:{}",
            config.combo_rfq_exchange_v3_address.trim()
        ));
    }
    if configured_collector_account(config).is_none() {
        blockers.push("onchain_order_filled_account_filter_missing".to_string());
    }
    blockers
}

async fn fetch_rpc_block_by_tag(http: &Client, config: &Config, tag: &str) -> Result<u64> {
    let rpc_url = config.polygon_rpc_url.trim();
    if rpc_url.is_empty() {
        anyhow::bail!("POLYGON_RPC_URL_empty");
    }
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [tag, false]
    });
    let response = http
        .post(rpc_url)
        .json(&request)
        .send()
        .await
        .with_context(|| format!("sending eth_getBlockByNumber({tag}) request"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("parsing eth_getBlockByNumber({tag}) response status={status}"))?;
    if !status.is_success() {
        anyhow::bail!("eth_getBlockByNumber({tag})_http_status:{status}");
    }
    if let Some(error) = body.get("error") {
        anyhow::bail!("eth_getBlockByNumber({tag})_rpc_error:{error}");
    }
    let Some(result) = body.get("result") else {
        anyhow::bail!("eth_getBlockByNumber({tag})_missing_result");
    };
    if result.is_null() {
        anyhow::bail!("eth_getBlockByNumber({tag})_null_result");
    }
    parse_u64_rpc_quantity(result.get("number"))
        .with_context(|| format!("eth_getBlockByNumber({tag}) number missing/invalid: {body}"))
}

async fn fetch_order_filled_logs_for_filter(
    http: &Client,
    config: &Config,
    filter: &OrderFilledCollectorFilter,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Value>> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [eth_get_logs_filter_value(filter, from_block, to_block)]
    });
    let response = http
        .post(config.polygon_rpc_url.trim())
        .json(&request)
        .send()
        .await
        .context("sending eth_getLogs request")?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("parsing eth_getLogs response status={status}"))?;
    if !status.is_success() {
        anyhow::bail!("eth_getLogs_http_status:{status}");
    }
    if let Some(error) = body.get("error") {
        anyhow::bail!("eth_getLogs_rpc_error:{error}");
    }
    match body.get("result") {
        Some(Value::Array(logs)) => Ok(logs.clone()),
        _ => anyhow::bail!("eth_getLogs_result_not_array"),
    }
}

fn eth_get_logs_filter_value(
    filter: &OrderFilledCollectorFilter,
    from_block: u64,
    to_block: u64,
) -> Value {
    let topics = if let Some(maker) = filter.indexed_maker_topic.as_deref() {
        json!([filter.topic0, Value::Null, maker])
    } else if let Some(taker) = filter.indexed_taker_topic.as_deref() {
        json!([filter.topic0, Value::Null, Value::Null, taker])
    } else if let Some(order_hash) = filter.indexed_order_hash_topic.as_deref() {
        json!([filter.topic0, order_hash])
    } else {
        json!([filter.topic0])
    };
    json!({
        "address": filter.address,
        "fromBlock": rpc_quantity(from_block),
        "toBlock": rpc_quantity(to_block),
        "topics": topics
    })
}

fn append_order_filled_log_values_deduped(path: &PathBuf, logs: &[Value]) -> Result<usize> {
    if logs.is_empty() {
        return Ok(0);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating diagnostics directory {}", parent.display()))?;
    }
    let mut seen = existing_log_identities(path)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut written = 0usize;
    for log in logs {
        if seen.insert(log_identity(log)) {
            writeln!(file, "{}", serde_json::to_string(log)?)
                .with_context(|| format!("writing {}", path.display()))?;
            written += 1;
        }
    }
    Ok(written)
}

fn existing_log_identities(path: &PathBuf) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut identities = HashSet::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("reading {} line {}", path.display(), idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(&line) {
            identities.insert(log_identity(&value));
        }
    }
    Ok(identities)
}

fn dedupe_log_values(logs: &mut Vec<Value>) {
    let mut seen = HashSet::new();
    logs.retain(|log| seen.insert(log_identity(log)));
}

fn log_identity(log: &Value) -> String {
    let tx = text_field(
        log,
        &["transactionHash", "transaction_hash", "txHash", "tx_hash"],
    )
    .unwrap_or_default()
    .to_ascii_lowercase();
    let log_index = text_field(log, &["logIndex", "log_index"])
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !tx.is_empty() && !log_index.is_empty() {
        return format!("{tx}:{log_index}");
    }
    serde_json::to_string(log).unwrap_or_default()
}

fn rpc_quantity(value: u64) -> String {
    format!("0x{value:x}")
}

pub fn decode_order_filled_log(
    log: &OnchainLogSummary,
) -> Result<Option<OrderFilledEvent>, String> {
    let Some(topic0) = log.topics.first() else {
        return Ok(None);
    };
    if *topic0 == order_filled_v1_topic() {
        return match log.topics.len() {
            4 => decode_indexed_order_filled_v1_log(log).map(Some),
            1 => decode_unindexed_order_filled_v1_log(log).map(Some),
            len => Err(format!("order_filled_v1_unexpected_topic_count:{len}")),
        };
    }
    if *topic0 == order_filled_v2_topic() {
        return match log.topics.len() {
            4 => decode_indexed_order_filled_v2_log(log).map(Some),
            1 => decode_unindexed_order_filled_v2_log(log).map(Some),
            len => Err(format!("order_filled_v2_unexpected_topic_count:{len}")),
        };
    }

    Ok(None)
}

pub fn build_order_filled_reconciliation_report(
    logs: &[OnchainLogSummary],
    account: Option<Address>,
) -> OrderFilledReconciliationReport {
    let mut blockers = Vec::new();
    let mut order_filled_logs = 0;
    let mut events = Vec::new();

    for (idx, log) in logs.iter().enumerate() {
        match decode_order_filled_log(log) {
            Ok(Some(event)) => {
                order_filled_logs += 1;
                events.push(event);
            }
            Ok(None) => {}
            Err(err) => {
                order_filled_logs += 1;
                blockers.push(format!("malformed_order_filled_log:{idx}:{err}"));
            }
        }
    }

    let account_order_filled_logs = account
        .map(|account| {
            events
                .iter()
                .filter(|event| event.maker == account || event.taker == account)
                .count()
        })
        .unwrap_or(events.len());

    if order_filled_logs == 0 {
        blockers.push("missing_order_filled_logs".to_string());
    }
    if account.is_some() && account_order_filled_logs == 0 {
        blockers.push("missing_account_order_filled_logs".to_string());
    }

    OrderFilledReconciliationReport {
        logs_seen: logs.len(),
        order_filled_logs,
        decoded_order_filled_logs: events.len(),
        account_order_filled_logs,
        events,
        blockers,
    }
}

pub fn onchain_log_summary_from_value(value: &Value) -> Result<OnchainLogSummary, String> {
    let address = parse_address_field(value, &["address", "exchange", "contractAddress"])?;
    let topics = match value.get("topics") {
        Some(Value::Array(topics)) => topics
            .iter()
            .enumerate()
            .map(|(idx, topic)| {
                topic
                    .as_str()
                    .ok_or_else(|| format!("topic_not_string:{idx}"))
                    .and_then(|raw| parse_b256(raw, &format!("topic:{idx}")))
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("topics_not_array".to_string()),
        None => return Err("missing_topics".to_string()),
    };
    let data = match value.get("data") {
        Some(Value::String(raw)) => parse_hex_bytes(raw, "data")?,
        Some(Value::Array(bytes)) => bytes
            .iter()
            .enumerate()
            .map(|(idx, value)| {
                let byte = value
                    .as_u64()
                    .ok_or_else(|| format!("data_byte_not_u64:{idx}"))?;
                u8::try_from(byte).map_err(|_| format!("data_byte_out_of_range:{idx}:{byte}"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err("data_not_hex_or_byte_array".to_string()),
        None => Vec::new(),
    };

    Ok(OnchainLogSummary {
        address,
        topics,
        data,
        transaction_hash: text_field(value, &["transactionHash", "transaction_hash", "txHash"])
            .map(str::to_string),
        block_number: u64_field(value, &["blockNumber", "block_number"]),
    })
}

fn decode_indexed_order_filled_v1_log(log: &OnchainLogSummary) -> Result<OrderFilledEvent, String> {
    ensure_word_count(&log.data, INDEXED_ORDER_FILLED_V1_DATA_WORDS)?;
    let order_hash = log.topics[1];
    let maker = address_from_word(log.topics[2], "maker_topic")?;
    let taker = address_from_word(log.topics[3], "taker_topic")?;

    Ok(OrderFilledEvent {
        protocol: "ctf_exchange_v1".to_string(),
        exchange: log.address,
        order_hash,
        maker,
        taker,
        side: None,
        token_id: None,
        maker_asset_id: u256_data_word(&log.data, 0)?,
        taker_asset_id: u256_data_word(&log.data, 1)?,
        maker_amount_filled: u256_data_word(&log.data, 2)?,
        taker_amount_filled: u256_data_word(&log.data, 3)?,
        fee: u256_data_word(&log.data, 4)?,
        builder: None,
        metadata: None,
        transaction_hash: log.transaction_hash.clone(),
        block_number: log.block_number,
    })
}

fn decode_unindexed_order_filled_v1_log(
    log: &OnchainLogSummary,
) -> Result<OrderFilledEvent, String> {
    ensure_word_count(&log.data, UNINDEXED_ORDER_FILLED_V1_DATA_WORDS)?;

    Ok(OrderFilledEvent {
        protocol: "ctf_exchange_v1".to_string(),
        exchange: log.address,
        order_hash: b256_data_word(&log.data, 0)?,
        maker: address_from_word(b256_data_word(&log.data, 1)?, "maker_data")?,
        taker: address_from_word(b256_data_word(&log.data, 2)?, "taker_data")?,
        side: None,
        token_id: None,
        maker_asset_id: u256_data_word(&log.data, 3)?,
        taker_asset_id: u256_data_word(&log.data, 4)?,
        maker_amount_filled: u256_data_word(&log.data, 5)?,
        taker_amount_filled: u256_data_word(&log.data, 6)?,
        fee: u256_data_word(&log.data, 7)?,
        builder: None,
        metadata: None,
        transaction_hash: log.transaction_hash.clone(),
        block_number: log.block_number,
    })
}

fn decode_indexed_order_filled_v2_log(log: &OnchainLogSummary) -> Result<OrderFilledEvent, String> {
    ensure_word_count(&log.data, INDEXED_ORDER_FILLED_V2_DATA_WORDS)?;
    let order_hash = log.topics[1];
    let maker = address_from_word(log.topics[2], "maker_topic")?;
    let taker = address_from_word(log.topics[3], "taker_topic")?;
    let side = u8_data_word(&log.data, 0, "side")?;
    let token_id = u256_data_word(&log.data, 1)?;
    let (maker_asset_id, taker_asset_id) = v2_asset_ids(side, token_id)?;

    Ok(OrderFilledEvent {
        protocol: "ctf_exchange_v2".to_string(),
        exchange: log.address,
        order_hash,
        maker,
        taker,
        side: Some(side),
        token_id: Some(token_id),
        maker_asset_id,
        taker_asset_id,
        maker_amount_filled: u256_data_word(&log.data, 2)?,
        taker_amount_filled: u256_data_word(&log.data, 3)?,
        fee: u256_data_word(&log.data, 4)?,
        builder: Some(b256_data_word(&log.data, 5)?),
        metadata: Some(b256_data_word(&log.data, 6)?),
        transaction_hash: log.transaction_hash.clone(),
        block_number: log.block_number,
    })
}

fn decode_unindexed_order_filled_v2_log(
    log: &OnchainLogSummary,
) -> Result<OrderFilledEvent, String> {
    ensure_word_count(&log.data, UNINDEXED_ORDER_FILLED_V2_DATA_WORDS)?;
    let side = u8_data_word(&log.data, 3, "side")?;
    let token_id = u256_data_word(&log.data, 4)?;
    let (maker_asset_id, taker_asset_id) = v2_asset_ids(side, token_id)?;

    Ok(OrderFilledEvent {
        protocol: "ctf_exchange_v2".to_string(),
        exchange: log.address,
        order_hash: b256_data_word(&log.data, 0)?,
        maker: address_from_word(b256_data_word(&log.data, 1)?, "maker_data")?,
        taker: address_from_word(b256_data_word(&log.data, 2)?, "taker_data")?,
        side: Some(side),
        token_id: Some(token_id),
        maker_asset_id,
        taker_asset_id,
        maker_amount_filled: u256_data_word(&log.data, 5)?,
        taker_amount_filled: u256_data_word(&log.data, 6)?,
        fee: u256_data_word(&log.data, 7)?,
        builder: Some(b256_data_word(&log.data, 8)?),
        metadata: Some(b256_data_word(&log.data, 9)?),
        transaction_hash: log.transaction_hash.clone(),
        block_number: log.block_number,
    })
}

fn ensure_word_count(data: &[u8], expected: usize) -> Result<(), String> {
    if !data.len().is_multiple_of(32) {
        return Err(format!("abi_data_not_word_aligned_len:{}", data.len()));
    }
    let actual = data.len() / 32;
    if actual != expected {
        return Err(format!("abi_data_word_count:{actual}:expected:{expected}"));
    }
    Ok(())
}

fn b256_data_word(data: &[u8], index: usize) -> Result<B256, String> {
    let word = data_word(data, index)?;
    Ok(B256::from(word))
}

fn u256_data_word(data: &[u8], index: usize) -> Result<U256, String> {
    let word = data_word(data, index)?;
    Ok(U256::from_be_bytes(word))
}

fn u8_data_word(data: &[u8], index: usize, label: &str) -> Result<u8, String> {
    let word = data_word(data, index)?;
    if word[..31].iter().any(|byte| *byte != 0) {
        return Err(format!("{label}_uint8_word_overflow"));
    }
    Ok(word[31])
}

fn data_word(data: &[u8], index: usize) -> Result<[u8; 32], String> {
    let start = index * 32;
    let end = start + 32;
    let Some(slice) = data.get(start..end) else {
        return Err(format!("missing_abi_word:{index}"));
    };
    let mut word = [0u8; 32];
    word.copy_from_slice(slice);
    Ok(word)
}

fn address_from_word(word: B256, label: &str) -> Result<Address, String> {
    if word.as_slice()[..12].iter().any(|byte| *byte != 0) {
        return Err(format!("{label}_has_nonzero_prefix"));
    }
    Ok(Address::from_word(word))
}

fn v2_asset_ids(side: u8, token_id: U256) -> Result<(U256, U256), String> {
    match side {
        0 => Ok((U256::ZERO, token_id)),
        1 => Ok((token_id, U256::ZERO)),
        other => Err(format!("invalid_v2_order_side:{other}")),
    }
}

fn parse_address_field(value: &Value, keys: &[&str]) -> Result<Address, String> {
    let raw = text_field(value, keys).ok_or_else(|| format!("missing_{}", keys[0]))?;
    Address::from_str(raw).map_err(|err| format!("invalid_{}:{err}", keys[0]))
}

fn parse_b256(raw: &str, label: &str) -> Result<B256, String> {
    B256::from_str(raw).map_err(|err| format!("invalid_{label}:{err}"))
}

fn parse_hex_bytes(raw: &str, label: &str) -> Result<Vec<u8>, String> {
    let raw = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    if !raw.len().is_multiple_of(2) {
        return Err(format!("{label}_hex_odd_length"));
    }
    let mut bytes = Vec::with_capacity(raw.len() / 2);
    for idx in (0..raw.len()).step_by(2) {
        let hi = hex_nibble(raw.as_bytes()[idx])
            .ok_or_else(|| format!("{label}_hex_invalid_char:{idx}"))?;
        let lo = hex_nibble(raw.as_bytes()[idx + 1])
            .ok_or_else(|| format!("{label}_hex_invalid_char:{}", idx + 1))?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn text_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn u64_field(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| parse_u64_rpc_quantity(value.get(*key)))
}

fn parse_u64_rpc_quantity(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => {
            let trimmed = text.trim();
            if let Some(hex) = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
            {
                u64::from_str_radix(hex, 16).ok()
            } else {
                trimmed.parse::<u64>().ok()
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use std::str::FromStr;

    fn address(raw: &str) -> Address {
        Address::from_str(raw).unwrap()
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let suffix = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
        std::env::temp_dir().join(format!("polymarket-onchain-fills-{name}-{suffix}"))
    }

    fn indexed_address_topic(address: Address) -> B256 {
        address.into_word()
    }

    fn push_b256_word(bytes: &mut Vec<u8>, value: B256) {
        bytes.extend_from_slice(value.as_slice());
    }

    fn push_address_word(bytes: &mut Vec<u8>, address: Address) {
        bytes.extend_from_slice(indexed_address_topic(address).as_slice());
    }

    fn push_u256_word(bytes: &mut Vec<u8>, value: U256) {
        bytes.extend_from_slice(&value.to_be_bytes::<32>());
    }

    fn hex_encode_lower(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    #[test]
    fn order_filled_topic_matches_exchange_contract_signature() {
        assert_eq!(
            order_filled_topic(),
            B256::from_str("0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6")
                .unwrap()
        );
        assert_ne!(order_filled_v1_topic(), order_filled_v2_topic());
    }

    #[test]
    fn order_filled_collector_report_blocks_without_runtime_inputs() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("collector-default");
        cfg.polygon_rpc_url.clear();
        cfg.combo_rfq_exchange_v3_address.clear();
        cfg.live_funder_address.clear();

        let report = build_order_filled_collector_report(&cfg);

        assert_eq!(report.status, "blocked_no_collector");
        assert!(!report.rpc_url_present);
        assert!(report.exchange_address.is_none());
        assert!(report.account_filter.is_none());
        assert!(report.suggested_eth_get_logs.is_empty());
        assert!(report
            .blockers
            .contains(&"POLYGON_RPC_URL_empty".to_string()));
        assert!(report
            .blockers
            .contains(&"COMBO_RFQ_EXCHANGE_V3_ADDRESS_empty".to_string()));
        assert!(report
            .blockers
            .contains(&"onchain_order_filled_account_filter_missing".to_string()));
        assert!(report
            .blockers
            .contains(&"ONCHAIN_ORDER_FILLED_COLLECTOR_ENABLED=false".to_string()));
    }

    #[test]
    fn order_filled_collector_report_plans_account_filtered_v2_log_queries() {
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("collector-ready-inputs");
        cfg.polygon_rpc_url = "https://polygon-rpc.example".into();
        cfg.combo_rfq_exchange_v3_address = "0x00000000000000000000000000000000000000ee".into();
        cfg.live_funder_address = "0x0000000000000000000000000000000000000001".into();
        cfg.onchain_order_filled_collector_enabled = true;

        let path = write_order_filled_collector_report(&cfg).unwrap();
        let report = build_order_filled_collector_report(&cfg);

        assert!(path.exists());
        assert_eq!(report.status, "ready_for_bounded_collector");
        assert!(report.blockers.is_empty());
        assert!(report.rpc_url_present);
        assert_eq!(
            report.exchange_address.as_deref().map(str::to_lowercase),
            Some("0x00000000000000000000000000000000000000ee".to_string())
        );
        assert_eq!(
            report.account_filter.as_deref().map(str::to_lowercase),
            Some("0x0000000000000000000000000000000000000001".to_string())
        );
        assert_eq!(report.suggested_eth_get_logs.len(), 2);
        assert!(report
            .suggested_eth_get_logs
            .iter()
            .all(|filter| filter.topic0 == order_filled_v2_topic().to_string()));
        assert!(report
            .suggested_eth_get_logs
            .iter()
            .any(|filter| filter.indexed_maker_topic.is_some()));
        assert!(report
            .suggested_eth_get_logs
            .iter()
            .any(|filter| filter.indexed_taker_topic.is_some()));
    }

    #[tokio::test]
    async fn bounded_collector_fetches_dedupes_and_writes_order_filled_logs() {
        let server = MockServer::start_async().await;
        let exchange = address("0x00000000000000000000000000000000000000ee");
        let maker = address("0x0000000000000000000000000000000000000001");
        let taker = address("0x0000000000000000000000000000000000000002");
        let order_hash = B256::from([7u8; 32]);
        let mut data = Vec::new();
        push_u256_word(&mut data, U256::ZERO);
        push_u256_word(&mut data, U256::from(202u64));
        push_u256_word(&mut data, U256::from(750_000u64));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::ZERO);
        push_b256_word(&mut data, B256::ZERO);
        push_b256_word(&mut data, B256::ZERO);
        let log = serde_json::json!({
            "address": exchange.to_string(),
            "topics": [
                order_filled_v2_topic().to_string(),
                order_hash.to_string(),
                indexed_address_topic(maker).to_string(),
                indexed_address_topic(taker).to_string()
            ],
            "data": format!("0x{}", hex_encode_lower(&data)),
            "transactionHash": "0xabc",
            "blockNumber": "0x7b",
            "logIndex": "0x0"
        });
        let logs_rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes(r#""method":"eth_getLogs""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [log.clone()]
                }));
            })
            .await;
        let dir = temp_dir("collector-rpc");
        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = dir.clone();
        cfg.polygon_rpc_url = server.base_url();
        cfg.combo_rfq_exchange_v3_address = exchange.to_string();
        cfg.live_funder_address = maker.to_string();
        cfg.onchain_order_filled_collector_enabled = true;

        let report = collect_order_filled_logs_once(&Client::new(), &cfg, 100, 123)
            .await
            .unwrap();

        assert_eq!(report.status, "collected");
        assert_eq!(report.from_block, 100);
        assert_eq!(report.to_block, 123);
        assert_eq!(report.filters_sent, 2);
        assert_eq!(report.raw_logs_fetched, 1);
        assert_eq!(report.logs_appended, 1);
        assert_eq!(report.decoded_order_filled_logs, 1);
        assert_eq!(report.account_order_filled_logs, 1);
        let body = fs::read_to_string(
            dir.join(crate::rfq_finality::COMBO_RFQ_ONCHAIN_ORDER_FILLED_LOGS_FILE),
        )
        .unwrap();
        assert_eq!(body.lines().count(), 1);
        let parsed =
            onchain_log_summary_from_value(&serde_json::from_str(body.trim()).unwrap()).unwrap();
        assert_eq!(parsed.block_number, Some(123));
        logs_rpc.assert_calls_async(2).await;
    }

    #[tokio::test]
    async fn recent_collector_uses_finalized_block_as_upper_bound() {
        let server = MockServer::start_async().await;
        let latest = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes(r#""method":"eth_getBlockByNumber""#)
                    .body_includes(r#""latest""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0x100"}
                }));
            })
            .await;
        let finalized = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes(r#""method":"eth_getBlockByNumber""#)
                    .body_includes(r#""finalized""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"number": "0xf0"}
                }));
            })
            .await;
        let logs_rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/")
                    .body_includes(r#""method":"eth_getLogs""#)
                    .body_includes(r#""fromBlock":"0xe6""#)
                    .body_includes(r#""toBlock":"0xf0""#);
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": []
                }));
            })
            .await;

        let mut cfg = Config::from_env();
        cfg.diagnostics_dir = temp_dir("collector-finalized-range");
        cfg.polygon_rpc_url = server.base_url();
        cfg.combo_rfq_exchange_v3_address = "0x00000000000000000000000000000000000000ee".into();
        cfg.live_funder_address = "0x0000000000000000000000000000000000000001".into();
        cfg.onchain_order_filled_collector_enabled = true;
        cfg.onchain_order_filled_collector_lookback_blocks = 10;
        cfg.polygon_finalized_block_max_lag_blocks = 32;

        let report = collect_recent_order_filled_logs(&Client::new(), &cfg)
            .await
            .unwrap();

        assert_eq!(report.latest_block, Some(256));
        assert_eq!(report.finalized_block, Some(240));
        assert_eq!(report.finalized_lag_blocks, Some(16));
        assert_eq!(report.from_block, 230);
        assert_eq!(report.to_block, 240);
        assert_eq!(report.status, "blocked");
        assert!(report
            .blockers
            .contains(&"no_order_filled_logs_collected".to_string()));
        latest.assert_calls_async(1).await;
        finalized.assert_calls_async(1).await;
        logs_rpc.assert_calls_async(2).await;
    }

    #[test]
    fn decodes_indexed_order_filled_log() {
        let exchange = address("0x00000000000000000000000000000000000000ee");
        let maker = address("0x0000000000000000000000000000000000000001");
        let taker = address("0x0000000000000000000000000000000000000002");
        let order_hash = B256::from([9u8; 32]);
        let mut data = Vec::new();
        push_u256_word(&mut data, U256::from(101u64));
        push_u256_word(&mut data, U256::from(0u8));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::from(670_000u64));
        push_u256_word(&mut data, U256::from(1_000u64));

        let log = OnchainLogSummary {
            address: exchange,
            topics: vec![
                order_filled_topic(),
                order_hash,
                indexed_address_topic(maker),
                indexed_address_topic(taker),
            ],
            data,
            transaction_hash: Some("0xabc".to_string()),
            block_number: Some(123),
        };

        let event = decode_order_filled_log(&log).unwrap().unwrap();

        assert_eq!(event.protocol, "ctf_exchange_v1");
        assert_eq!(event.exchange, exchange);
        assert_eq!(event.order_hash, order_hash);
        assert_eq!(event.maker, maker);
        assert_eq!(event.taker, taker);
        assert_eq!(event.maker_asset_id, U256::from(101u64));
        assert_eq!(event.taker_asset_id, U256::ZERO);
        assert_eq!(event.maker_amount_filled, U256::from(1_000_000u64));
        assert_eq!(event.taker_amount_filled, U256::from(670_000u64));
        assert_eq!(event.fee, U256::from(1_000u64));
        assert_eq!(event.transaction_hash.as_deref(), Some("0xabc"));
        assert_eq!(event.block_number, Some(123));
    }

    #[test]
    fn decodes_unindexed_order_filled_log() {
        let exchange = address("0x00000000000000000000000000000000000000ee");
        let maker = address("0x0000000000000000000000000000000000000001");
        let taker = address("0x0000000000000000000000000000000000000002");
        let order_hash = B256::from([7u8; 32]);
        let mut data = Vec::new();
        push_b256_word(&mut data, order_hash);
        push_address_word(&mut data, maker);
        push_address_word(&mut data, taker);
        push_u256_word(&mut data, U256::ZERO);
        push_u256_word(&mut data, U256::from(202u64));
        push_u256_word(&mut data, U256::from(710_000u64));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::ZERO);

        let log = OnchainLogSummary {
            address: exchange,
            topics: vec![order_filled_topic()],
            data,
            transaction_hash: None,
            block_number: None,
        };

        let event = decode_order_filled_log(&log).unwrap().unwrap();

        assert_eq!(event.protocol, "ctf_exchange_v1");
        assert_eq!(event.exchange, exchange);
        assert_eq!(event.order_hash, order_hash);
        assert_eq!(event.maker, maker);
        assert_eq!(event.taker, taker);
        assert_eq!(event.maker_asset_id, U256::ZERO);
        assert_eq!(event.taker_asset_id, U256::from(202u64));
        assert_eq!(event.maker_amount_filled, U256::from(710_000u64));
        assert_eq!(event.taker_amount_filled, U256::from(1_000_000u64));
        assert_eq!(event.fee, U256::ZERO);
    }

    #[test]
    fn decodes_indexed_v2_order_filled_log_and_derives_asset_ids() {
        let exchange = address("0x00000000000000000000000000000000000000ee");
        let maker = address("0x0000000000000000000000000000000000000001");
        let taker = address("0x0000000000000000000000000000000000000002");
        let order_hash = B256::from([5u8; 32]);
        let builder = B256::from([11u8; 32]);
        let metadata = B256::from([12u8; 32]);
        let token_id = U256::from(202u64);
        let mut data = Vec::new();
        push_u256_word(&mut data, U256::ZERO);
        push_u256_word(&mut data, token_id);
        push_u256_word(&mut data, U256::from(710_000u64));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::from(1_000u64));
        push_b256_word(&mut data, builder);
        push_b256_word(&mut data, metadata);

        let log = OnchainLogSummary {
            address: exchange,
            topics: vec![
                order_filled_v2_topic(),
                order_hash,
                indexed_address_topic(maker),
                indexed_address_topic(taker),
            ],
            data,
            transaction_hash: Some("0xdef".to_string()),
            block_number: Some(456),
        };

        let event = decode_order_filled_log(&log).unwrap().unwrap();

        assert_eq!(event.protocol, "ctf_exchange_v2");
        assert_eq!(event.order_hash, order_hash);
        assert_eq!(event.maker, maker);
        assert_eq!(event.taker, taker);
        assert_eq!(event.side, Some(0));
        assert_eq!(event.token_id, Some(token_id));
        assert_eq!(event.maker_asset_id, U256::ZERO);
        assert_eq!(event.taker_asset_id, token_id);
        assert_eq!(event.maker_amount_filled, U256::from(710_000u64));
        assert_eq!(event.taker_amount_filled, U256::from(1_000_000u64));
        assert_eq!(event.fee, U256::from(1_000u64));
        assert_eq!(event.builder, Some(builder));
        assert_eq!(event.metadata, Some(metadata));
        assert_eq!(event.transaction_hash.as_deref(), Some("0xdef"));
        assert_eq!(event.block_number, Some(456));
    }

    #[test]
    fn v2_order_filled_rejects_unknown_side() {
        let exchange = address("0x00000000000000000000000000000000000000ee");
        let maker = address("0x0000000000000000000000000000000000000001");
        let taker = address("0x0000000000000000000000000000000000000002");
        let mut data = Vec::new();
        push_u256_word(&mut data, U256::from(9u8));
        push_u256_word(&mut data, U256::from(202u64));
        push_u256_word(&mut data, U256::from(710_000u64));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::ZERO);
        push_b256_word(&mut data, B256::ZERO);
        push_b256_word(&mut data, B256::ZERO);

        let log = OnchainLogSummary {
            address: exchange,
            topics: vec![
                order_filled_v2_topic(),
                B256::from([6u8; 32]),
                indexed_address_topic(maker),
                indexed_address_topic(taker),
            ],
            data,
            transaction_hash: None,
            block_number: None,
        };

        let err = decode_order_filled_log(&log).unwrap_err();

        assert!(err.contains("invalid_v2_order_side:9"));
    }

    #[test]
    fn order_filled_reconciliation_blocks_without_logs() {
        let account = address("0x0000000000000000000000000000000000000001");
        let report = build_order_filled_reconciliation_report(&[], Some(account));

        assert_eq!(report.logs_seen, 0);
        assert_eq!(report.order_filled_logs, 0);
        assert_eq!(report.decoded_order_filled_logs, 0);
        assert_eq!(report.account_order_filled_logs, 0);
        assert!(report
            .blockers
            .contains(&"missing_order_filled_logs".to_string()));
        assert!(report
            .blockers
            .contains(&"missing_account_order_filled_logs".to_string()));
    }

    #[test]
    fn order_filled_reconciliation_counts_account_involvement_and_malformed_logs() {
        let exchange = address("0x00000000000000000000000000000000000000ee");
        let account = address("0x0000000000000000000000000000000000000001");
        let taker = address("0x0000000000000000000000000000000000000002");
        let mut data = Vec::new();
        push_u256_word(&mut data, U256::ZERO);
        push_u256_word(&mut data, U256::from(202u64));
        push_u256_word(&mut data, U256::from(710_000u64));
        push_u256_word(&mut data, U256::from(1_000_000u64));
        push_u256_word(&mut data, U256::ZERO);

        let valid = OnchainLogSummary {
            address: exchange,
            topics: vec![
                order_filled_topic(),
                B256::from([1u8; 32]),
                indexed_address_topic(account),
                indexed_address_topic(taker),
            ],
            data,
            transaction_hash: None,
            block_number: None,
        };
        let malformed = OnchainLogSummary {
            address: exchange,
            topics: vec![order_filled_topic(), B256::from([2u8; 32])],
            data: Vec::new(),
            transaction_hash: None,
            block_number: None,
        };

        let report = build_order_filled_reconciliation_report(&[valid, malformed], Some(account));

        assert_eq!(report.logs_seen, 2);
        assert_eq!(report.order_filled_logs, 2);
        assert_eq!(report.decoded_order_filled_logs, 1);
        assert_eq!(report.account_order_filled_logs, 1);
        assert_eq!(report.events.len(), 1);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.starts_with("malformed_order_filled_log:1:")));
        assert!(!report
            .blockers
            .contains(&"missing_account_order_filled_logs".to_string()));
    }
}
