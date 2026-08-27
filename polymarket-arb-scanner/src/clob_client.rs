//! CLOB API client for fetching real orderbook prices from Polymarket.
//!
//! The CLOB (Central Limit Order Book) API provides the actual executable
//! prices — best ask (what you'd pay to buy), orderbook depth, and market-level
//! metadata such as tick size and fee parameters.
//!
//! Read-only operations (price, orderbook, market info) do NOT require authentication.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use reqwest::{header, Client};
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

use crate::config::Config;
use crate::models::{is_external_token_id, Market, MAX_SUPPORTED_CLOB_FEE_EXPONENT};
use crate::ws_client::{Price, PriceCache};

const CLOB_RETRY_WAIT_MAX_MS: u64 = 30_000;
static CLOB_READ_RATE_LIMITS: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();

#[derive(Clone, Copy)]
struct RestSuccessDeadline {
    endpoint: &'static str,
    freshness_kind: &'static str,
    budget_name: &'static str,
    max_ms: u64,
}

#[derive(serde::Deserialize, Debug, Clone)]
struct ClobBook {
    asset_id: Option<String>,
    asks: Option<Vec<ClobBookLevel>>,
    bids: Option<Vec<ClobBookLevel>>,
    tick_size: Option<String>,
    min_order_size: Option<String>,
    neg_risk: Option<bool>,
    timestamp: Option<serde_json::Value>,
    hash: Option<String>,
}

#[derive(serde::Deserialize, Debug, Clone)]
struct ClobBookLevel {
    price: Option<String>,
    size: Option<String>,
}

#[derive(serde::Serialize)]
struct ClobBookRequest<'a> {
    token_id: &'a str,
}

#[derive(serde::Serialize)]
struct ClobPriceRequest<'a> {
    token_id: &'a str,
    side: &'a str,
}

fn observe_clob_http_status(config: &Config, endpoint: &'static str, status: u16) {
    #[cfg(test)]
    if !config.diagnostics_dir.starts_with(std::env::temp_dir()) {
        return;
    }

    if let Err(err) = crate::engine_mode::observe_http_response(
        config,
        "clob_client",
        endpoint,
        status,
        None,
        None,
    ) {
        debug!("Failed to record CLOB engine-mode observation for {endpoint}: {err:#}");
    }
}

fn clob_read_rate_limits() -> &'static Mutex<HashMap<String, Instant>> {
    CLOB_READ_RATE_LIMITS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn clob_read_rate_limit_key(config: &Config, endpoint: &str) -> String {
    format!("{}|{endpoint}", config.clob_api_url.trim_end_matches('/'))
}

fn clob_read_rate_limit_remaining(config: &Config, endpoint: &str) -> Option<Duration> {
    let key = clob_read_rate_limit_key(config, endpoint);
    let now = Instant::now();
    let mut limits = clob_read_rate_limits().lock().ok()?;
    let until = *limits.get(&key)?;
    if until <= now {
        limits.remove(&key);
        None
    } else {
        Some(until.saturating_duration_since(now))
    }
}

fn clob_record_read_rate_limit(config: &Config, endpoint: &str, wait_ms: u64) {
    let key = clob_read_rate_limit_key(config, endpoint);
    let until = Instant::now() + Duration::from_millis(wait_ms.max(1));
    if let Ok(mut limits) = clob_read_rate_limits().lock() {
        limits.insert(key, until);
    }
}

async fn clob_wait_for_read_rate_limit(
    config: &Config,
    endpoint: &'static str,
    deadline: Option<RestSuccessDeadline>,
    elapsed: Duration,
) -> Result<()> {
    let Some(remaining) = clob_read_rate_limit_remaining(config, endpoint) else {
        return Ok(());
    };
    if let Some(deadline) = deadline {
        let elapsed_ms = elapsed.as_millis();
        let max_ms = u128::from(deadline.max_ms);
        if elapsed_ms >= max_ms {
            return Err(anyhow!(
                "CLOB {endpoint} read cooldown deadline exhausted: elapsed={}ms >= {}={}ms",
                elapsed_ms,
                deadline.budget_name,
                deadline.max_ms
            ));
        }
        let remaining_budget = max_ms - elapsed_ms;
        if remaining.as_millis() >= remaining_budget {
            return Err(anyhow!(
                "CLOB {endpoint} read cooldown exceeds {} freshness deadline: retry_after={}ms remaining={}ms {}={}ms",
                deadline.freshness_kind,
                remaining.as_millis(),
                remaining_budget,
                deadline.budget_name,
                deadline.max_ms
            ));
        }
    }

    debug!(
        "CLOB {endpoint} waiting {}ms for shared read rate-limit cooldown",
        remaining.as_millis()
    );
    tokio::time::sleep(remaining).await;
    Ok(())
}

fn retry_after_header_ms(headers: &header::HeaderMap) -> Option<u64> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    if value.is_empty() {
        return None;
    }

    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1_000));
    }

    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
        .and_then(|retry_at| {
            let wait_ms = retry_at
                .signed_duration_since(Utc::now())
                .num_milliseconds();
            (wait_ms > 0).then_some(wait_ms as u64)
        })
}

fn retry_wait_ms(config: &Config, attempt: u32, retry_after_ms: Option<u64>) -> u64 {
    let exp = 2u64.saturating_pow(attempt.saturating_sub(1));
    let backoff_ms = config
        .retry_backoff_base_ms
        .saturating_mul(exp)
        .min(CLOB_RETRY_WAIT_MAX_MS);
    retry_after_ms
        .map(|ms| backoff_ms.max(ms.min(CLOB_RETRY_WAIT_MAX_MS)))
        .unwrap_or(backoff_ms)
}

fn retry_wait_ms_with_deadline(
    config: &Config,
    attempt: u32,
    retry_after_ms: Option<u64>,
    deadline: Option<RestSuccessDeadline>,
    elapsed: Duration,
) -> Result<u64> {
    let wait_ms = retry_wait_ms(config, attempt, retry_after_ms);
    let Some(deadline) = deadline else {
        return Ok(wait_ms);
    };

    let elapsed_ms = elapsed.as_millis();
    let max_ms = u128::from(deadline.max_ms);
    if elapsed_ms >= max_ms {
        return Err(anyhow!(
            "CLOB {} retry deadline exhausted: elapsed={}ms >= {}={}ms",
            deadline.endpoint,
            elapsed_ms,
            deadline.budget_name,
            deadline.max_ms
        ));
    }

    let remaining_ms = max_ms - elapsed_ms;
    if u128::from(wait_ms) >= remaining_ms {
        return Err(anyhow!(
            "CLOB {} retry wait exceeds {} freshness deadline: wait={}ms remaining={}ms {}={}ms",
            deadline.endpoint,
            deadline.freshness_kind,
            wait_ms,
            remaining_ms,
            deadline.budget_name,
            deadline.max_ms
        ));
    }

    Ok(wait_ms)
}

fn live_rest_success_deadline(config: &Config, endpoint: &'static str) -> RestSuccessDeadline {
    RestSuccessDeadline {
        endpoint,
        freshness_kind: "live",
        budget_name: "LIVE_MAX_REFRESH_TO_SUBMIT_MS",
        max_ms: config.live_max_refresh_to_submit_ms.max(1),
    }
}

fn scan_rest_deadline_ms(config: &Config) -> u64 {
    let api_timeout_ms = config.api_timeout_secs.saturating_mul(1_000).max(1);
    let signal_budget_ms = if config.max_signal_age_secs == 0 {
        u64::MAX
    } else {
        config
            .max_signal_age_secs
            .saturating_mul(1_000)
            .saturating_div(2)
            .max(1)
    };

    api_timeout_ms
        .min(signal_budget_ms)
        .min(config.live_max_refresh_to_submit_ms.max(1))
        .clamp(1, 1_200)
}

fn scan_rest_success_deadline(config: &Config, endpoint: &'static str) -> RestSuccessDeadline {
    RestSuccessDeadline {
        endpoint,
        freshness_kind: "scan",
        budget_name: "SCAN_REST_DEADLINE_MS",
        max_ms: scan_rest_deadline_ms(config),
    }
}

fn rest_request_timeout_with_deadline(
    config: &Config,
    deadline: Option<RestSuccessDeadline>,
    elapsed: Duration,
) -> Result<Duration> {
    let api_timeout = Duration::from_millis(config.api_timeout_secs.saturating_mul(1_000).max(1));
    let Some(deadline) = deadline else {
        return Ok(api_timeout);
    };

    let elapsed_ms = elapsed.as_millis();
    let max_ms = u128::from(deadline.max_ms);
    if elapsed_ms >= max_ms {
        return Err(anyhow!(
            "CLOB {} request skipped: {} freshness deadline exhausted elapsed={}ms >= {}={}ms",
            deadline.endpoint,
            deadline.freshness_kind,
            elapsed_ms,
            deadline.budget_name,
            deadline.max_ms
        ));
    }

    let remaining_ms = (max_ms - elapsed_ms).max(1);
    Ok(api_timeout.min(Duration::from_millis(remaining_ms as u64)))
}

fn ensure_rest_success_within_deadline(
    deadline: Option<RestSuccessDeadline>,
    elapsed: Duration,
) -> Result<()> {
    let Some(deadline) = deadline else {
        return Ok(());
    };

    let elapsed_ms = elapsed.as_millis();
    let max_ms = u128::from(deadline.max_ms);
    if elapsed_ms > max_ms {
        return Err(anyhow!(
            "CLOB {} successful response exceeded {} freshness deadline: elapsed={}ms > {}={}ms",
            deadline.endpoint,
            deadline.freshness_kind,
            elapsed_ms,
            deadline.budget_name,
            deadline.max_ms
        ));
    }

    Ok(())
}

#[derive(serde::Deserialize, Debug, Default, Clone)]
struct ClobMarketInfo {
    #[serde(rename = "c", alias = "condition_id")]
    condition_id: Option<String>,
    #[serde(rename = "t", default)]
    tokens: Vec<Option<ClobMarketToken>>,
    #[serde(rename = "mts", alias = "minimum_tick_size")]
    min_tick_size: Option<f64>,
    #[serde(rename = "mos", alias = "minimum_order_size")]
    min_order_size: Option<f64>,
    #[serde(rename = "nr", alias = "negRisk", alias = "neg_risk")]
    neg_risk: Option<bool>,
    #[serde(rename = "rfqe", alias = "rfqEnabled", alias = "rfq_enabled")]
    rfq_enabled: Option<bool>,
    #[serde(rename = "fd")]
    fee_details: Option<ClobFeeDetails>,
    #[serde(alias = "ao")]
    accepting_orders: Option<bool>,
    active: Option<bool>,
    archived: Option<bool>,
    closed: Option<bool>,
    enable_order_book: Option<bool>,
    #[serde(
        default,
        rename = "seconds_delay",
        alias = "sd",
        alias = "itode",
        deserialize_with = "deserialize_present_wire_value"
    )]
    seconds_delay: PresentWireValue,
    #[serde(
        default,
        rename = "oas",
        deserialize_with = "deserialize_present_wire_value"
    )]
    minimum_order_age_seconds: PresentWireValue,
    #[serde(
        default,
        rename = "game_start_time",
        alias = "gameStartTime",
        alias = "gst",
        deserialize_with = "deserialize_present_wire_value"
    )]
    game_start_time: PresentWireValue,
}

#[derive(serde::Deserialize, Debug, Clone)]
struct ClobMarketToken {
    #[serde(rename = "t")]
    token_id: serde_json::Value,
}

impl ClobMarketToken {
    fn token_id(&self) -> Option<String> {
        match &self.token_id {
            serde_json::Value::String(value) => {
                let value = value.trim();
                (!value.is_empty()).then(|| value.to_string())
            }
            serde_json::Value::Number(value) => Some(value.to_string()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct PresentWireValue {
    present: bool,
    value: serde_json::Value,
}

impl Default for PresentWireValue {
    fn default() -> Self {
        Self {
            present: false,
            value: serde_json::Value::Null,
        }
    }
}

fn deserialize_present_wire_value<'de, D>(deserializer: D) -> Result<PresentWireValue, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(PresentWireValue {
        present: true,
        value: serde::Deserialize::deserialize(deserializer)?,
    })
}

fn seconds_delay_value(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Bool(true) => Some(0.25),
        serde_json::Value::Bool(false) => Some(0.0),
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(value) => {
            if value.eq_ignore_ascii_case("true") {
                Some(0.25)
            } else if value.eq_ignore_ascii_case("false") {
                Some(0.0)
            } else {
                value.parse::<f64>().ok()
            }
        }
        _ => None,
    }
}

fn numeric_wire_value(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(value) => value.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_datetime(value: Option<&serde_json::Value>) -> Option<DateTime<Utc>> {
    match value? {
        serde_json::Value::String(raw) => DateTime::parse_from_rfc3339(raw)
            .ok()
            .map(|dt| dt.with_timezone(&Utc)),
        serde_json::Value::Number(number) => number
            .as_i64()
            .and_then(|secs| DateTime::<Utc>::from_timestamp(secs, 0)),
        _ => None,
    }
}

#[derive(serde::Deserialize, Debug, Default, Clone)]
struct ClobFeeDetails {
    #[serde(rename = "r")]
    rate: Option<f64>,
    #[serde(rename = "e")]
    exponent: Option<u32>,
}

#[derive(Debug, Default, Clone)]
struct MarketMetadataSnapshot {
    condition_id: Option<String>,
    token_ids: Vec<String>,
    tick_size: Option<f64>,
    min_order_size: Option<f64>,
    fee_rate: Option<f64>,
    fee_exponent: Option<u32>,
    neg_risk: Option<bool>,
    rfq_enabled: Option<bool>,
    live_orderable: Option<bool>,
    accepting_orders: Option<bool>,
    seconds_delay_present: bool,
    seconds_delay: Option<f64>,
    minimum_order_age_seconds_present: bool,
    minimum_order_age_seconds: Option<f64>,
    game_start_time_present: bool,
    game_start_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LiveClobFeeSchedule {
    pub rate: f64,
    pub exponent: u32,
}

fn clob_market_live_orderable(info: &ClobMarketInfo) -> Option<bool> {
    let flags = [
        info.active,
        info.archived.map(|value| !value),
        info.closed.map(|value| !value),
        info.accepting_orders,
        info.enable_order_book,
    ];
    if flags.iter().flatten().any(|value| !value) {
        return Some(false);
    }
    flags.iter().all(Option::is_some).then_some(true)
}

fn ensure_market_info_orderable(
    info: &MarketMetadataSnapshot,
    expected_condition_id: &str,
    expected_token_ids: Option<&HashSet<String>>,
) -> Result<()> {
    for (name, present, value) in [
        (
            "sd/seconds_delay",
            info.seconds_delay_present,
            info.seconds_delay,
        ),
        (
            "oas/minimum_order_age_seconds",
            info.minimum_order_age_seconds_present,
            info.minimum_order_age_seconds,
        ),
    ] {
        if !present {
            continue;
        }
        let value = value.ok_or_else(|| anyhow!("market metadata {name} is malformed"))?;
        if !value.is_finite() || value < 0.0 {
            return Err(anyhow!("market metadata {name} is invalid: {value}"));
        }
        if value != 0.0 {
            return Err(anyhow!(
                "market metadata requires immediate matching but {name}={value}"
            ));
        }
    }
    if info.game_start_time_present && info.game_start_time.is_none() {
        return Err(anyhow!("market metadata gst/game_start_time is malformed"));
    }

    if let Some(false) = info.live_orderable {
        return Err(anyhow!("{}", clob_market_orderability_detail(info)));
    }

    if info.accepting_orders != Some(true) {
        return Err(anyhow!(
            "compact market metadata requires accepting-orders ao=true"
        ));
    }

    let actual_condition_id = info
        .condition_id
        .as_deref()
        .ok_or_else(|| anyhow!("compact market metadata missing c"))?;
    if actual_condition_id != expected_condition_id {
        return Err(anyhow!(
            "compact market condition mismatch: actual={actual_condition_id} expected={expected_condition_id}"
        ));
    }
    let tick_size = info
        .tick_size
        .filter(|value| value.is_finite() && *value > 0.0 && *value < 1.0)
        .ok_or_else(|| anyhow!("compact market metadata missing valid mts"))?;
    let min_order_size = info
        .min_order_size
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| anyhow!("compact market metadata missing valid mos"))?;
    if info.token_ids.len() < 2 {
        return Err(anyhow!(
            "compact market metadata requires at least two valid t entries"
        ));
    }
    if let Some(expected_token_ids) = expected_token_ids {
        for token_id in expected_token_ids {
            if !info.token_ids.iter().any(|actual| actual == token_id) {
                return Err(anyhow!(
                    "compact market token mapping missing planned token {token_id}"
                ));
            }
        }
    }
    debug!(
        "CLOB compact market orderability accepted: condition_id={} mts={} mos={} tokens={}",
        expected_condition_id,
        tick_size,
        min_order_size,
        info.token_ids.len(),
    );
    Ok(())
}

fn market_metadata_from_info(info: &ClobMarketInfo) -> MarketMetadataSnapshot {
    MarketMetadataSnapshot {
        condition_id: info
            .condition_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        token_ids: info
            .tokens
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(ClobMarketToken::token_id)
            .collect(),
        tick_size: info.min_tick_size,
        min_order_size: info.min_order_size,
        fee_rate: info.fee_details.as_ref().and_then(|fd| fd.rate),
        fee_exponent: info.fee_details.as_ref().and_then(|fd| fd.exponent),
        neg_risk: info.neg_risk,
        rfq_enabled: info.rfq_enabled,
        live_orderable: clob_market_live_orderable(info),
        accepting_orders: info.accepting_orders,
        seconds_delay_present: info.seconds_delay.present,
        seconds_delay: info
            .seconds_delay
            .present
            .then(|| seconds_delay_value(&info.seconds_delay.value))
            .flatten(),
        minimum_order_age_seconds_present: info.minimum_order_age_seconds.present,
        minimum_order_age_seconds: info
            .minimum_order_age_seconds
            .present
            .then(|| numeric_wire_value(&info.minimum_order_age_seconds.value))
            .flatten(),
        // Official V2 represents "no scheduled game start" as gst=null.  Only a
        // present, non-null value must parse as a timestamp.
        game_start_time_present: info.game_start_time.present
            && !info.game_start_time.value.is_null(),
        game_start_time: info
            .game_start_time
            .present
            .then(|| value_to_datetime(Some(&info.game_start_time.value)))
            .flatten(),
    }
}

#[derive(Debug, Default, Clone)]
struct BookSummary {
    best_ask: Option<f64>,
    best_bid: Option<f64>,
    best_ask_size: Option<f64>,
    best_bid_size: Option<f64>,
    ask_depth: Vec<(f64, f64)>,
    bid_depth: Vec<(f64, f64)>,
    tick_size: Option<f64>,
    min_order_size: Option<f64>,
    neg_risk: Option<bool>,
    venue_timestamp_ms: Option<u64>,
    book_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub token_id: String,
    pub asks: Vec<(f64, f64)>,
    pub tick_size: Option<f64>,
    pub min_order_size: Option<f64>,
    pub neg_risk: Option<bool>,
    pub observed_at: Option<Instant>,
    pub venue_timestamp_ms: Option<u64>,
    pub book_hash: Option<String>,
}

impl DepthSnapshot {
    pub fn available_shares_at_price(&self, max_price: f64) -> f64 {
        self.asks
            .iter()
            .filter_map(|(price, size)| {
                if *price <= max_price {
                    Some(*size)
                } else {
                    None
                }
            })
            .sum()
    }

    pub fn average_ask_for_shares(&self, target_shares: f64) -> Option<f64> {
        if target_shares <= f64::EPSILON {
            return None;
        }

        let mut remaining = target_shares;
        let mut total_spent = 0.0;
        let mut total_shares = 0.0;

        for (price, size) in &self.asks {
            let take = (*size).min(remaining);
            total_spent += take * *price;
            total_shares += take;
            remaining -= take;
            if remaining <= f64::EPSILON {
                break;
            }
        }

        if remaining > f64::EPSILON || total_shares <= f64::EPSILON {
            None
        } else {
            Some(total_spent / total_shares)
        }
    }

    pub fn cutoff_ask_for_shares(&self, target_shares: f64) -> Option<f64> {
        if target_shares <= f64::EPSILON {
            return None;
        }

        let mut remaining = target_shares;
        for (price, size) in &self.asks {
            remaining -= *size;
            if remaining <= f64::EPSILON {
                return Some(*price);
            }
        }

        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct QuoteEnrichmentStats {
    pub total_tokens: usize,
    pub cache_hits: usize,
    pub rest_requested: usize,
    pub rest_resolved: usize,
    pub rest_batches: usize,
    pub deferred_tokens: usize,
    pub hard_unresolved_tokens: usize,
    pub no_ask_tokens: usize,
    pub missing_book_tokens: usize,
    pub unresolved_tokens: usize,
    pub unresolved_token_samples: Vec<String>,
}

impl QuoteEnrichmentStats {
    pub fn cache_hit_rate_pct(&self) -> f64 {
        if self.total_tokens == 0 {
            0.0
        } else {
            (self.cache_hits as f64 / self.total_tokens as f64) * 100.0
        }
    }

    pub fn rest_resolution_rate_pct(&self) -> f64 {
        if self.rest_requested == 0 {
            0.0
        } else {
            (self.rest_resolved as f64 / self.rest_requested as f64) * 100.0
        }
    }
}

fn parse_level_price(level: &ClobBookLevel) -> Option<f64> {
    let price = level.price.as_deref()?.parse::<f64>().ok()?;
    if (0.0..=1.0).contains(&price) && price > 0.0 {
        Some(price)
    } else {
        None
    }
}

fn parse_clob_price_value(value: &serde_json::Value) -> Option<f64> {
    let price = match value {
        serde_json::Value::Number(number) => number.as_f64()?,
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if price.is_finite() && price > 0.0 && price <= 1.0 {
        Some(price)
    } else {
        None
    }
}

fn parse_clob_price_entry(value: &serde_json::Value, side: &str) -> Option<f64> {
    if let Some(price) = parse_clob_price_value(value) {
        return Some(price);
    }
    let object = value.as_object()?;
    let side_lower = side.to_ascii_lowercase();
    for key in [side, side_lower.as_str(), "price", "value"] {
        if let Some(price) = object.get(key).and_then(parse_clob_price_value) {
            return Some(price);
        }
    }
    None
}

fn clob_price_record_side_matches(value: &serde_json::Value, side: &str) -> bool {
    let Some(object) = value.as_object() else {
        return true;
    };
    let Some(record_side) = object
        .get("side")
        .or_else(|| object.get("Side"))
        .or_else(|| object.get("SIDE"))
        .and_then(|value| value.as_str())
    else {
        return true;
    };
    record_side.trim().eq_ignore_ascii_case(side)
}

fn collect_clob_prices_response(
    value: &serde_json::Value,
    requested: &HashSet<&str>,
    side: &str,
    out: &mut HashMap<String, f64>,
) {
    if let Some(items) = value.as_array() {
        for item in items {
            collect_clob_prices_response(item, requested, side, out);
        }
        return;
    }

    let Some(object) = value.as_object() else {
        return;
    };

    for token_id in requested {
        if out.contains_key(*token_id) {
            continue;
        }
        if let Some(price) = object
            .get(*token_id)
            .and_then(|entry| parse_clob_price_entry(entry, side))
        {
            out.insert((*token_id).to_string(), price);
        }
    }

    let token_id = object
        .get("token_id")
        .or_else(|| object.get("tokenId"))
        .or_else(|| object.get("asset_id"))
        .or_else(|| object.get("assetId"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|token_id| requested.contains(*token_id));
    if let Some(token_id) = token_id {
        if !out.contains_key(token_id) && clob_price_record_side_matches(value, side) {
            if let Some(price) = parse_clob_price_entry(value, side) {
                out.insert(token_id.to_string(), price);
            }
        }
    }

    for key in ["prices", "data", "result", "results", "items"] {
        if let Some(child) = object.get(key) {
            collect_clob_prices_response(child, requested, side, out);
        }
    }
}

fn parse_clob_prices_response(
    value: &serde_json::Value,
    token_ids: &[String],
    side: &str,
) -> HashMap<String, f64> {
    let requested: HashSet<&str> = token_ids.iter().map(String::as_str).collect();
    let mut out = HashMap::new();
    collect_clob_prices_response(value, &requested, side, &mut out);
    out
}

fn parse_level_size(level: &ClobBookLevel) -> Option<f64> {
    let size = level.size.as_deref()?.parse::<f64>().ok()?;
    if size > 0.0 {
        Some(size)
    } else {
        None
    }
}

fn parse_book_scalar(raw: &Option<String>) -> Option<f64> {
    let value = raw.as_deref()?.parse::<f64>().ok()?;
    if value.is_finite() && value > 0.0 {
        Some(value)
    } else {
        None
    }
}

fn book_timestamp_ms(value: Option<&serde_json::Value>) -> Option<u64> {
    let raw = match value? {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    if !raw.is_finite() || raw < 0.0 {
        return None;
    }
    let millis = if raw < 10_000_000_000.0 {
        raw * 1000.0
    } else {
        raw
    };
    Some(millis.round() as u64)
}

fn best_ask_summary(levels: Vec<ClobBookLevel>) -> (Option<f64>, Option<f64>) {
    let mut best_price: Option<f64> = None;
    let mut best_size: Option<f64> = None;

    for level in levels {
        let Some(price) = parse_level_price(&level) else {
            continue;
        };
        let Some(size) = parse_level_size(&level) else {
            continue;
        };
        match best_price {
            None => {
                best_price = Some(price);
                best_size = Some(size);
            }
            Some(current) if price < current => {
                best_price = Some(price);
                best_size = Some(size);
            }
            Some(current) if (price - current).abs() < 1e-12 => {
                best_size = Some(best_size.unwrap_or(0.0) + size);
            }
            _ => {}
        }
    }

    (best_price, best_size)
}

fn best_bid_summary(levels: Vec<ClobBookLevel>) -> (Option<f64>, Option<f64>) {
    let mut best_price: Option<f64> = None;
    let mut best_size: Option<f64> = None;

    for level in levels {
        let Some(price) = parse_level_price(&level) else {
            continue;
        };
        let Some(size) = parse_level_size(&level) else {
            continue;
        };
        match best_price {
            None => {
                best_price = Some(price);
                best_size = Some(size);
            }
            Some(current) if price > current => {
                best_price = Some(price);
                best_size = Some(size);
            }
            Some(current) if (price - current).abs() < 1e-12 => {
                best_size = Some(best_size.unwrap_or(0.0) + size);
            }
            _ => {}
        }
    }

    (best_price, best_size)
}

fn normalized_depth_from_levels(
    mut levels: Vec<ClobBookLevel>,
    ascending: bool,
) -> Vec<(f64, f64)> {
    levels.sort_by(|a, b| {
        let pa = parse_level_price(a).unwrap_or(if ascending {
            f64::INFINITY
        } else {
            f64::NEG_INFINITY
        });
        let pb = parse_level_price(b).unwrap_or(if ascending {
            f64::INFINITY
        } else {
            f64::NEG_INFINITY
        });
        if ascending {
            pa.total_cmp(&pb)
        } else {
            pb.total_cmp(&pa)
        }
    });

    let mut out: Vec<(f64, f64)> = Vec::new();
    for level in levels {
        let Some(price) = parse_level_price(&level) else {
            continue;
        };
        let Some(size) = parse_level_size(&level) else {
            continue;
        };
        if let Some((last_price, last_size)) = out.last_mut() {
            if (*last_price - price).abs() < 1e-12 {
                *last_size += size;
                continue;
            }
        }
        out.push((price, size));
    }
    out
}

fn token_preview(token_id: &str) -> &str {
    &token_id[..16.min(token_id.len())]
}

fn scan_cache_max_age_ms(config: &Config) -> u64 {
    let max_age = config.ws_quote_max_age_ms.max(1);
    let discovery_bound = config.discovery_interval_secs.max(1).saturating_mul(2) * 1000;
    max_age.min(discovery_bound).min(15000)
}

fn scan_no_ask_cache_max_age_ms(config: &Config) -> u64 {
    config
        .live_max_refresh_to_submit_ms
        .max(config.ws_quote_max_age_ms)
        .clamp(1, 2_000)
}

fn venue_timestamp_stale(venue_timestamp_ms: Option<u64>, max_age_ms: u64) -> bool {
    let Some(venue_timestamp_ms) = venue_timestamp_ms else {
        return false;
    };
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return false;
    };
    let now_ms = now.as_millis() as u64;
    venue_timestamp_ms > now_ms.saturating_add(max_age_ms)
        || now_ms.saturating_sub(venue_timestamp_ms) > max_age_ms
}

fn cached_book_summary_from_snapshot(snapshot: &Price, max_age_ms: u64) -> Option<BookSummary> {
    if !snapshot.snapshot_ready {
        return None;
    }
    if max_age_ms > 0 && snapshot.last_updated.elapsed() > Duration::from_millis(max_age_ms) {
        return None;
    }
    if max_age_ms > 0 && venue_timestamp_stale(snapshot.venue_timestamp_ms, max_age_ms) {
        return None;
    }

    Some(BookSummary {
        best_ask: snapshot.best_ask,
        best_bid: snapshot.best_bid,
        best_ask_size: snapshot.best_ask_size,
        best_bid_size: snapshot.best_bid_size,
        ask_depth: snapshot.ask_depth.clone(),
        bid_depth: snapshot.bid_depth.clone(),
        tick_size: snapshot.tick_size,
        min_order_size: None,
        neg_risk: None,
        venue_timestamp_ms: snapshot.venue_timestamp_ms,
        book_hash: snapshot.book_hash.clone(),
    })
}

async fn cached_book_summary(
    price_cache: &PriceCache,
    config: &Config,
    token_id: &str,
) -> Option<BookSummary> {
    let token_id = token_id.trim();
    if token_id.is_empty() {
        return None;
    }

    let cache = price_cache.read().await;
    let snapshot = cache.get(token_id)?;
    cached_book_summary_from_snapshot(snapshot, config.ws_quote_max_age_ms)
}

async fn cached_book_summary_relaxed(
    price_cache: &PriceCache,
    config: &Config,
    token_id: &str,
) -> Option<BookSummary> {
    let token_id = token_id.trim();
    if token_id.is_empty() {
        return None;
    }

    let cache = price_cache.read().await;
    let snapshot = cache.get(token_id)?;
    let has_executable_ask = snapshot.best_ask.is_some_and(|price| price > 0.0);
    let max_age_ms = if has_executable_ask {
        scan_cache_max_age_ms(config)
    } else {
        scan_no_ask_cache_max_age_ms(config)
    };
    cached_book_summary_from_snapshot(snapshot, max_age_ms)
}

pub async fn get_cached_depth_snapshots(
    price_cache: &PriceCache,
    config: &Config,
    token_ids: &[String],
) -> Option<HashMap<String, DepthSnapshot>> {
    if token_ids.is_empty() {
        return Some(HashMap::new());
    }

    let mut snapshots = HashMap::new();
    let cache = price_cache.read().await;
    for token_id in token_ids {
        let lookup_id = token_id.trim();
        if lookup_id.is_empty() {
            return None;
        }
        let snapshot = cache.get(lookup_id)?;
        let summary = cached_book_summary_from_snapshot(snapshot, config.ws_quote_max_age_ms)?;
        if summary.ask_depth.is_empty() {
            return None;
        }
        snapshots.insert(
            token_id.clone(),
            DepthSnapshot {
                token_id: token_id.clone(),
                asks: summary.ask_depth,
                tick_size: summary.tick_size,
                min_order_size: summary.min_order_size,
                neg_risk: summary.neg_risk,
                observed_at: Some(snapshot.last_updated),
                venue_timestamp_ms: summary.venue_timestamp_ms,
                book_hash: summary.book_hash,
            },
        );
    }
    Some(snapshots)
}

fn summary_has_scan_quote(summary: &BookSummary) -> bool {
    summary.best_ask.unwrap_or(0.0) > 0.0
}

fn book_summary_from_book(book: ClobBook) -> BookSummary {
    let asks = book.asks.unwrap_or_default();
    let bids = book.bids.unwrap_or_default();
    let (best_ask, best_ask_size) = best_ask_summary(asks.clone());
    let (best_bid, best_bid_size) = best_bid_summary(bids.clone());
    BookSummary {
        best_ask,
        best_bid,
        best_ask_size,
        best_bid_size,
        ask_depth: normalized_depth_from_levels(asks, true),
        bid_depth: normalized_depth_from_levels(bids, false),
        tick_size: parse_book_scalar(&book.tick_size),
        min_order_size: parse_book_scalar(&book.min_order_size),
        neg_risk: book.neg_risk,
        venue_timestamp_ms: book_timestamp_ms(book.timestamp.as_ref()),
        book_hash: book.hash,
    }
}

fn depth_snapshot_from_book(
    fallback_token_id: &str,
    book: ClobBook,
    observed_at: Instant,
) -> Option<DepthSnapshot> {
    let token_id = book
        .asset_id
        .clone()
        .unwrap_or_else(|| fallback_token_id.to_string());
    if token_id.trim().is_empty() {
        return None;
    }

    let mut asks: Vec<(f64, f64)> = book
        .asks
        .unwrap_or_default()
        .iter()
        .filter_map(|level| Some((parse_level_price(level)?, parse_level_size(level)?)))
        .filter(|(price, size)| {
            price.is_finite() && *price > 0.0 && size.is_finite() && *size > 0.0
        })
        .collect();
    asks.sort_by(|a, b| a.0.total_cmp(&b.0));
    Some(DepthSnapshot {
        token_id,
        asks,
        tick_size: parse_book_scalar(&book.tick_size),
        min_order_size: parse_book_scalar(&book.min_order_size),
        neg_risk: book.neg_risk,
        observed_at: Some(observed_at),
        venue_timestamp_ms: book_timestamp_ms(book.timestamp.as_ref()),
        book_hash: book.hash,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchFetchOutcome {
    Success,
    RetryableFailure,
    NonRetryableFailure,
    DeadlineExceeded,
}

fn merge_book_summary(target: &mut BookSummary, update: BookSummary) {
    target.best_ask = update.best_ask;
    target.best_bid = update.best_bid;
    target.best_ask_size = update.best_ask_size;
    target.best_bid_size = update.best_bid_size;
    target.ask_depth = update.ask_depth;
    target.bid_depth = update.bid_depth;
    if update.tick_size.is_some() {
        target.tick_size = update.tick_size;
    }
    if update.min_order_size.is_some() {
        target.min_order_size = update.min_order_size;
    }
    if update.neg_risk.is_some() {
        target.neg_risk = update.neg_risk;
    }
    if update.venue_timestamp_ms.is_some() {
        target.venue_timestamp_ms = update.venue_timestamp_ms;
    }
    if update.book_hash.is_some() {
        target.book_hash = update.book_hash;
    }
}

async fn update_quote_cache_from_summaries(
    price_cache: &PriceCache,
    summaries: &HashMap<String, BookSummary>,
) {
    if summaries.is_empty() {
        return;
    }
    let mut cache = price_cache.write().await;
    for (token_id, summary) in summaries {
        let entry = cache.entry(token_id.clone()).or_default();
        if let (Some(current), Some(incoming)) =
            (entry.venue_timestamp_ms, summary.venue_timestamp_ms)
        {
            if incoming < current {
                debug!(
                    "CLOB REST cache update skipped regressive book for {}: incoming_ts={} current_ts={}",
                    token_preview(token_id),
                    incoming,
                    current
                );
                continue;
            }
        }
        entry.best_ask = summary.best_ask;
        entry.best_bid = summary.best_bid;
        entry.best_ask_size = summary.best_ask_size;
        entry.best_bid_size = summary.best_bid_size;
        entry.ask_depth = summary.ask_depth.clone();
        entry.bid_depth = summary.bid_depth.clone();
        if summary.tick_size.is_some() {
            entry.tick_size = summary.tick_size;
        }
        if summary.venue_timestamp_ms.is_some() {
            entry.venue_timestamp_ms = summary.venue_timestamp_ms;
        }
        if summary.book_hash.is_some() {
            entry.book_hash = summary.book_hash.clone();
        }
        entry.snapshot_ready = true;
        entry.last_updated = std::time::Instant::now();
    }
}

async fn fetch_book(client: &Client, config: &Config, token_id: &str) -> Option<ClobBook> {
    let token_id = token_id.trim();
    if token_id.is_empty() {
        return None;
    }

    let url = format!("{}/book", config.clob_api_url);
    let max_attempts = config.max_retries.max(1);
    for attempt in 1..=max_attempts {
        let _ = clob_wait_for_read_rate_limit(config, "GET /book", None, Duration::from_millis(0))
            .await;
        match client
            .get(&url)
            .query(&[("token_id", token_id)])
            .timeout(Duration::from_secs(config.api_timeout_secs))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return resp.json().await.ok(),
            Ok(resp) => {
                let status = resp.status();
                observe_clob_http_status(config, "GET /book", status.as_u16());
                let retry_after_ms = retry_after_header_ms(resp.headers());
                if status.as_u16() == 429 {
                    clob_record_read_rate_limit(
                        config,
                        "GET /book",
                        retry_wait_ms(config, attempt, retry_after_ms),
                    );
                }
                let should_retry =
                    status.as_u16() == 425 || status.as_u16() == 429 || status.is_server_error();
                if should_retry && attempt < max_attempts {
                    let wait_ms = retry_wait_ms(config, attempt, retry_after_ms);
                    warn!(
                        "CLOB book retry for token {}... status {} (attempt {attempt}/{}), waiting {}ms",
                        token_preview(token_id),
                        status,
                        max_attempts,
                        wait_ms,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return None;
            }
            Err(err) => {
                let should_retry = err.is_timeout() || err.is_connect();
                if should_retry && attempt < max_attempts {
                    let wait_ms = retry_wait_ms(config, attempt, None);
                    warn!(
                        "CLOB book transport retry for token {}... (attempt {attempt}/{}), waiting {}ms: {}",
                        token_preview(token_id),
                        max_attempts,
                        wait_ms,
                        err,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return None;
            }
        }
    }
    None
}

async fn fetch_books_batch_chunk(
    client: &Client,
    config: &Config,
    token_ids: &[String],
    success_deadline: Option<RestSuccessDeadline>,
    deadline_started_at: Instant,
) -> (HashMap<String, BookSummary>, BatchFetchOutcome) {
    if token_ids.is_empty() {
        return (HashMap::new(), BatchFetchOutcome::Success);
    }

    let url = format!("{}/books", config.clob_api_url);
    let body: Vec<ClobBookRequest<'_>> = token_ids
        .iter()
        .map(|token_id| ClobBookRequest {
            token_id: token_id.as_str(),
        })
        .collect();
    let max_attempts = config.max_retries.max(1);
    let mut last_outcome = BatchFetchOutcome::RetryableFailure;

    for attempt in 1..=max_attempts {
        let request_timeout = match rest_request_timeout_with_deadline(
            config,
            success_deadline,
            deadline_started_at.elapsed(),
        ) {
            Ok(timeout) => timeout,
            Err(err) => {
                debug!("{err:#}");
                return (HashMap::new(), BatchFetchOutcome::DeadlineExceeded);
            }
        };
        if let Err(err) = clob_wait_for_read_rate_limit(
            config,
            "POST /books",
            success_deadline,
            deadline_started_at.elapsed(),
        )
        .await
        {
            debug!("{err:#}");
            return (HashMap::new(), BatchFetchOutcome::DeadlineExceeded);
        }

        match client
            .post(&url)
            .json(&body)
            .timeout(request_timeout)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Err(err) = ensure_rest_success_within_deadline(
                    success_deadline,
                    deadline_started_at.elapsed(),
                ) {
                    debug!("{err:#}");
                    return (HashMap::new(), BatchFetchOutcome::DeadlineExceeded);
                }

                let books = match resp.json::<Vec<ClobBook>>().await {
                    Ok(books) => books,
                    Err(err) => {
                        debug!(
                            "CLOB books batch decode error for {} tokens: {}",
                            token_ids.len(),
                            err,
                        );
                        return (HashMap::new(), BatchFetchOutcome::NonRetryableFailure);
                    }
                };

                let mut out = HashMap::new();
                for (idx, book) in books.into_iter().enumerate() {
                    let key = book
                        .asset_id
                        .clone()
                        .or_else(|| token_ids.get(idx).cloned());
                    if let Some(token_id) = key {
                        out.insert(token_id, book_summary_from_book(book));
                    }
                }
                return (out, BatchFetchOutcome::Success);
            }
            Ok(resp) => {
                let status = resp.status();
                observe_clob_http_status(config, "POST /books", status.as_u16());
                let retry_after_ms = retry_after_header_ms(resp.headers());
                if status.as_u16() == 429 {
                    clob_record_read_rate_limit(
                        config,
                        "POST /books",
                        retry_wait_ms(config, attempt, retry_after_ms),
                    );
                }
                let should_retry =
                    status.as_u16() == 425 || status.as_u16() == 429 || status.is_server_error();
                last_outcome = if should_retry {
                    BatchFetchOutcome::RetryableFailure
                } else {
                    BatchFetchOutcome::NonRetryableFailure
                };
                if should_retry && attempt < max_attempts {
                    let wait_ms = match retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        retry_after_ms,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    ) {
                        Ok(wait_ms) => wait_ms,
                        Err(err) => {
                            debug!("{err:#}");
                            return (HashMap::new(), BatchFetchOutcome::DeadlineExceeded);
                        }
                    };
                    warn!(
                        "CLOB books batch retry for {} tokens status {} (attempt {attempt}/{}) waiting {}ms",
                        token_ids.len(),
                        status,
                        max_attempts,
                        wait_ms,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return (HashMap::new(), last_outcome);
            }
            Err(err) => {
                let should_retry = err.is_timeout() || err.is_connect();
                last_outcome = if should_retry {
                    BatchFetchOutcome::RetryableFailure
                } else {
                    BatchFetchOutcome::NonRetryableFailure
                };
                if should_retry && attempt < max_attempts {
                    let wait_ms = match retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        None,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    ) {
                        Ok(wait_ms) => wait_ms,
                        Err(err) => {
                            debug!("{err:#}");
                            return (HashMap::new(), BatchFetchOutcome::DeadlineExceeded);
                        }
                    };
                    warn!(
                        "CLOB books batch transport retry for {} tokens (attempt {attempt}/{}) waiting {}ms: {}",
                        token_ids.len(),
                        max_attempts,
                        wait_ms,
                        err,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return (HashMap::new(), last_outcome);
            }
        }
    }

    (HashMap::new(), last_outcome)
}

pub async fn get_depth_snapshots(
    client: &Client,
    config: &Config,
    token_ids: &[String],
) -> Result<HashMap<String, DepthSnapshot>> {
    get_depth_snapshots_with_deadline(client, config, token_ids, None).await
}

pub async fn get_live_depth_snapshots(
    client: &Client,
    config: &Config,
    token_ids: &[String],
) -> Result<HashMap<String, DepthSnapshot>> {
    get_depth_snapshots_with_deadline(
        client,
        config,
        token_ids,
        Some(live_rest_success_deadline(config, "final depth /books")),
    )
    .await
}

pub async fn get_live_sell_prices(
    client: &Client,
    config: &Config,
    token_ids: &[String],
) -> Result<HashMap<String, f64>> {
    get_prices_with_deadline(
        client,
        config,
        token_ids,
        "SELL",
        Some(live_rest_success_deadline(config, "final prices /prices")),
    )
    .await
}

async fn get_prices_with_deadline(
    client: &Client,
    config: &Config,
    token_ids: &[String],
    side: &str,
    success_deadline: Option<RestSuccessDeadline>,
) -> Result<HashMap<String, f64>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let side = side.trim().to_ascii_uppercase();
    if !matches!(side.as_str(), "BUY" | "SELL") {
        return Err(anyhow!(
            "CLOB /prices request received invalid side {side:?}"
        ));
    }

    let url = format!("{}/prices", config.clob_api_url);
    let body: Vec<ClobPriceRequest<'_>> = token_ids
        .iter()
        .map(|token_id| ClobPriceRequest {
            token_id: token_id.as_str(),
            side: side.as_str(),
        })
        .collect();
    let max_attempts = config.max_retries.max(1);
    let deadline_started_at = Instant::now();

    for attempt in 1..=max_attempts {
        clob_wait_for_read_rate_limit(
            config,
            "POST /prices final-prices",
            success_deadline,
            deadline_started_at.elapsed(),
        )
        .await?;
        let request_timeout = rest_request_timeout_with_deadline(
            config,
            success_deadline,
            deadline_started_at.elapsed(),
        )?;
        match client
            .post(&url)
            .json(&body)
            .timeout(request_timeout)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let value = resp.json::<serde_json::Value>().await.map_err(|err| {
                    anyhow!(
                        "CLOB final prices /prices decode error for {} tokens side={}: {}",
                        token_ids.len(),
                        side,
                        err
                    )
                })?;
                ensure_rest_success_within_deadline(
                    success_deadline,
                    deadline_started_at.elapsed(),
                )?;
                let prices = parse_clob_prices_response(&value, token_ids, &side);
                let missing: Vec<String> = token_ids
                    .iter()
                    .filter(|token_id| !prices.contains_key(token_id.as_str()))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    return Err(anyhow!(
                        "CLOB final prices /prices incomplete successful response: requested={} returned={} side={} missing={:?}",
                        token_ids.len(),
                        prices.len(),
                        side,
                        missing
                    ));
                }
                return Ok(prices);
            }
            Ok(resp) => {
                let status = resp.status();
                observe_clob_http_status(config, "POST /prices final-prices", status.as_u16());
                let retry_after_ms = retry_after_header_ms(resp.headers());
                if status.as_u16() == 429 {
                    clob_record_read_rate_limit(
                        config,
                        "POST /prices final-prices",
                        retry_wait_ms(config, attempt, retry_after_ms),
                    );
                }
                if (status.as_u16() == 425 || status.as_u16() == 429 || status.is_server_error())
                    && attempt < max_attempts
                {
                    let wait_ms = retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        retry_after_ms,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "CLOB final prices /prices failed for {} tokens side={} with status {} body={}",
                    token_ids.len(),
                    side,
                    status,
                    body.chars().take(256).collect::<String>()
                ));
            }
            Err(err) => {
                if (err.is_timeout() || err.is_connect()) && attempt < max_attempts {
                    let wait_ms = retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        None,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return Err(anyhow!(
                    "CLOB final prices /prices transport error for {} tokens side={}: {}",
                    token_ids.len(),
                    side,
                    err
                ));
            }
        }
    }

    Err(anyhow!(
        "CLOB final prices /prices exhausted {} attempts for {} tokens side={}",
        max_attempts,
        token_ids.len(),
        side
    ))
}

async fn get_depth_snapshots_with_deadline(
    client: &Client,
    config: &Config,
    token_ids: &[String],
    success_deadline: Option<RestSuccessDeadline>,
) -> Result<HashMap<String, DepthSnapshot>> {
    if token_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let url = format!("{}/books", config.clob_api_url);
    let body: Vec<ClobBookRequest<'_>> = token_ids
        .iter()
        .map(|token_id| ClobBookRequest {
            token_id: token_id.as_str(),
        })
        .collect();
    let max_attempts = config.max_retries.max(1);
    let deadline_started_at = Instant::now();

    for attempt in 1..=max_attempts {
        clob_wait_for_read_rate_limit(
            config,
            "POST /books final-depth",
            success_deadline,
            deadline_started_at.elapsed(),
        )
        .await?;
        let request_timeout = rest_request_timeout_with_deadline(
            config,
            success_deadline,
            deadline_started_at.elapsed(),
        )?;
        match client
            .post(&url)
            .json(&body)
            .timeout(request_timeout)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let books = resp.json::<Vec<ClobBook>>().await.map_err(|err| {
                    anyhow!(
                        "CLOB final depth /books decode error for {} tokens: {}",
                        token_ids.len(),
                        err
                    )
                })?;
                ensure_rest_success_within_deadline(
                    success_deadline,
                    deadline_started_at.elapsed(),
                )?;
                let observed_at = Instant::now();
                let mut out = HashMap::new();
                for (idx, book) in books.into_iter().enumerate() {
                    let fallback = token_ids.get(idx).map(String::as_str).unwrap_or_default();
                    if let Some(snapshot) = depth_snapshot_from_book(fallback, book, observed_at) {
                        out.insert(snapshot.token_id.clone(), snapshot);
                    }
                }
                let mut missing = Vec::new();
                let mut incomplete = Vec::new();
                for token_id in token_ids {
                    match out.get(token_id) {
                        Some(snapshot)
                            if !snapshot.asks.is_empty()
                                && snapshot.tick_size.is_some()
                                && snapshot.min_order_size.is_some()
                                && snapshot.neg_risk.is_some()
                                && snapshot.venue_timestamp_ms.is_some()
                                && snapshot
                                    .book_hash
                                    .as_deref()
                                    .is_some_and(|hash| !hash.trim().is_empty()) => {}
                        Some(_) => incomplete.push(token_id.clone()),
                        None => missing.push(token_id.clone()),
                    }
                }
                if !missing.is_empty() || !incomplete.is_empty() {
                    return Err(anyhow!(
                        "CLOB final depth /books incomplete successful response: requested={} returned={} missing={:?} incomplete={:?}",
                        token_ids.len(),
                        out.len(),
                        missing,
                        incomplete
                    ));
                }
                return Ok(out);
            }
            Ok(resp) => {
                let status = resp.status();
                observe_clob_http_status(config, "POST /books final-depth", status.as_u16());
                let retry_after_ms = retry_after_header_ms(resp.headers());
                if status.as_u16() == 429 {
                    clob_record_read_rate_limit(
                        config,
                        "POST /books final-depth",
                        retry_wait_ms(config, attempt, retry_after_ms),
                    );
                }
                if (status.as_u16() == 425 || status.as_u16() == 429 || status.is_server_error())
                    && attempt < max_attempts
                {
                    let wait_ms = retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        retry_after_ms,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "CLOB final depth /books failed for {} tokens with status {} body={}",
                    token_ids.len(),
                    status,
                    body.chars().take(256).collect::<String>()
                ));
            }
            Err(err) => {
                if (err.is_timeout() || err.is_connect()) && attempt < max_attempts {
                    let wait_ms = retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        None,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }
                return Err(anyhow!(
                    "CLOB final depth /books transport error for {} tokens: {}",
                    token_ids.len(),
                    err
                ));
            }
        }
    }

    Err(anyhow!(
        "CLOB final depth /books exhausted {} attempts for {} tokens",
        max_attempts,
        token_ids.len()
    ))
}

/// Fetch authoritative CLOB V2 `fd.r`/`fd.e` schedules by condition ID.
pub async fn get_live_fee_schedules(
    client: &Client,
    config: &Config,
    condition_ids: &[String],
) -> Result<HashMap<String, LiveClobFeeSchedule>> {
    let mut unique_condition_ids = Vec::new();
    let mut seen = HashSet::new();
    for condition_id in condition_ids {
        let condition_id = condition_id.trim();
        if condition_id.is_empty() {
            return Err(anyhow!(
                "CLOB V2 fee-schedule request received an empty condition id"
            ));
        }
        if seen.insert(condition_id.to_string()) {
            unique_condition_ids.push(condition_id.to_string());
        }
    }
    if unique_condition_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let concurrency = unique_condition_ids
        .len()
        .min(config.max_batchable_legs())
        .max(1);
    let success_deadline = Some(live_rest_success_deadline(config, "market-info"));
    let mut fetches = stream::iter(unique_condition_ids)
        .map(|condition_id| async move {
            let info = get_market_info_strict(
                client,
                config,
                &condition_id,
                success_deadline,
            )
            .await?;
            let rate = info.fee_rate.ok_or_else(|| {
                anyhow!(
                    "CLOB V2 fee metadata missing fd.r condition_id={condition_id}"
                )
            })?;
            let exponent = info.fee_exponent.ok_or_else(|| {
                anyhow!(
                    "CLOB V2 fee metadata missing fd.e condition_id={condition_id}"
                )
            })?;
            if !rate.is_finite()
                || !(0.0..=1.0).contains(&rate)
                || exponent == 0
                || exponent > MAX_SUPPORTED_CLOB_FEE_EXPONENT
            {
                return Err(anyhow!(
                    "CLOB V2 fee metadata invalid condition_id={condition_id} fd.r={rate} fd.e={exponent}"
                ));
            }
            Ok::<_, anyhow::Error>((condition_id, LiveClobFeeSchedule { rate, exponent }))
        })
        .buffer_unordered(concurrency);

    let mut out = HashMap::new();
    while let Some(result) = fetches.next().await {
        let (condition_id, schedule) = result?;
        out.insert(condition_id, schedule);
    }
    Ok(out)
}

async fn fetch_books_best_effort(
    client: &Client,
    config: &Config,
    token_ids: &[String],
    stats: &mut QuoteEnrichmentStats,
    success_deadline: Option<RestSuccessDeadline>,
    deadline_started_at: Instant,
) -> HashMap<String, BookSummary> {
    let mut queue: Vec<Vec<String>> = if token_ids.is_empty() {
        Vec::new()
    } else {
        vec![token_ids.to_vec()]
    };
    let mut out = HashMap::new();

    while let Some(chunk) = queue.pop() {
        if chunk.is_empty() {
            continue;
        }
        stats.rest_batches += 1;
        let (fetched, outcome) = fetch_books_batch_chunk(
            client,
            config,
            &chunk,
            success_deadline,
            deadline_started_at,
        )
        .await;
        match outcome {
            BatchFetchOutcome::Success => {
                out.extend(fetched);
            }
            BatchFetchOutcome::RetryableFailure if chunk.len() > 1 => {
                debug!(
                    "CLOB batch degraded after retryable failure; splitting {} token request into smaller chunks",
                    chunk.len()
                );
                let mid = chunk.len() / 2;
                queue.push(chunk[mid..].to_vec());
                queue.push(chunk[..mid].to_vec());
            }
            BatchFetchOutcome::DeadlineExceeded => {
                debug!(
                    "CLOB scan REST deadline consumed; skipping {} unresolved token(s)",
                    chunk.len()
                );
                break;
            }
            _ => {
                for token_id in chunk {
                    let fallback_timeout = match rest_request_timeout_with_deadline(
                        config,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    ) {
                        Ok(timeout) => timeout,
                        Err(err) => {
                            debug!("{err:#}");
                            break;
                        }
                    };
                    let summary = match tokio::time::timeout(
                        fallback_timeout,
                        get_book_summary(client, config, &token_id),
                    )
                    .await
                    {
                        Ok(summary) => summary,
                        Err(_) => {
                            debug!("CLOB single-book fallback timed out under scan REST deadline");
                            break;
                        }
                    };
                    if summary.best_ask.is_some()
                        || summary.best_bid.is_some()
                        || summary.tick_size.is_some()
                        || summary.min_order_size.is_some()
                    {
                        out.insert(token_id, summary);
                    }
                }
            }
        }
    }

    out
}

async fn fetch_book_summaries_with_cache(
    client: &Client,
    config: &Config,
    token_ids: Vec<String>,
    price_cache: Option<&PriceCache>,
    relaxed_cache: bool,
    rest_token_budget: usize,
) -> (HashMap<String, BookSummary>, QuoteEnrichmentStats) {
    let mut seen = HashSet::new();
    let ordered_ids: Vec<String> = token_ids
        .into_iter()
        .filter(|token_id| !token_id.trim().is_empty())
        .filter(|token_id| seen.insert(token_id.clone()))
        .collect();

    let mut summaries = HashMap::new();
    let mut fresh_summaries = HashMap::new();
    let mut missing = Vec::new();
    let mut stats = QuoteEnrichmentStats {
        total_tokens: ordered_ids.len(),
        ..QuoteEnrichmentStats::default()
    };

    for token_id in &ordered_ids {
        let cached = if let Some(cache) = price_cache {
            if relaxed_cache {
                cached_book_summary_relaxed(cache, config, token_id).await
            } else {
                cached_book_summary(cache, config, token_id).await
            }
        } else {
            None
        };

        if let Some(summary) = cached {
            if summary_has_scan_quote(&summary) {
                stats.cache_hits += 1;
            } else if !relaxed_cache {
                missing.push(token_id.clone());
            } else {
                // scan-time no-ask cache is valid market state; final execution still refreshes depth.
            }
            summaries.insert(token_id.clone(), summary);
        } else {
            missing.push(token_id.clone());
        }
    }

    let rest_budget = rest_token_budget;
    let rest_missing: Vec<String> = missing.iter().take(rest_budget).cloned().collect();
    let deferred: Vec<String> = missing.iter().skip(rest_budget).cloned().collect();
    stats.deferred_tokens = deferred.len();
    stats.rest_requested = rest_missing.len();

    let scan_deadline = Some(scan_rest_success_deadline(config, "scan /books"));
    let scan_deadline_started_at = Instant::now();
    let batch_size = config.clob_book_batch_size.max(1);
    let total_chunks = if rest_missing.is_empty() {
        0
    } else {
        rest_missing.len().div_ceil(batch_size)
    };
    for (chunk_index, chunk) in rest_missing.chunks(batch_size).enumerate() {
        let chunk_ids: Vec<String> = chunk.to_vec();
        let fetched = fetch_books_best_effort(
            client,
            config,
            &chunk_ids,
            &mut stats,
            scan_deadline,
            scan_deadline_started_at,
        )
        .await;
        stats.rest_resolved += fetched
            .values()
            .filter(|summary| summary_has_scan_quote(summary))
            .count();
        for (token_id, summary) in fetched {
            fresh_summaries.insert(token_id.clone(), summary.clone());
            if let Some(existing) = summaries.get_mut(&token_id) {
                merge_book_summary(existing, summary);
            } else {
                summaries.insert(token_id, summary);
            }
        }
        if rest_request_timeout_with_deadline(
            config,
            scan_deadline,
            scan_deadline_started_at.elapsed(),
        )
        .is_err()
        {
            debug!("CLOB scan REST deadline consumed before remaining quote chunks");
            break;
        }
        if config.clob_book_batch_pause_ms > 0 && chunk_index + 1 < total_chunks {
            if rest_request_timeout_with_deadline(
                config,
                scan_deadline,
                scan_deadline_started_at.elapsed(),
            )
            .is_err()
            {
                debug!("CLOB scan REST deadline consumed before inter-batch pause");
                break;
            }
            tokio::time::sleep(Duration::from_millis(config.clob_book_batch_pause_ms)).await;
        }
    }

    stats.hard_unresolved_tokens = rest_missing
        .iter()
        .filter(|token_id| {
            !summaries
                .get(*token_id)
                .map(summary_has_scan_quote)
                .unwrap_or(false)
        })
        .count();
    stats.no_ask_tokens = rest_missing
        .iter()
        .filter(|token_id| matches!(summaries.get(*token_id), Some(summary) if !summary_has_scan_quote(summary)))
        .count();
    stats.missing_book_tokens = stats
        .hard_unresolved_tokens
        .saturating_sub(stats.no_ask_tokens);
    stats.unresolved_tokens = stats.hard_unresolved_tokens + stats.deferred_tokens;
    stats.unresolved_token_samples = ordered_ids
        .iter()
        .filter(|token_id| {
            !summaries
                .get(*token_id)
                .map(summary_has_scan_quote)
                .unwrap_or(false)
        })
        .take(config.quote_shortfall_sample_size.max(1))
        .cloned()
        .collect();

    if let Some(cache) = price_cache {
        update_quote_cache_from_summaries(cache, &fresh_summaries).await;
    }

    (summaries, stats)
}

pub fn tick_decimals(tick_size: f64) -> usize {
    let tick = tick_size.max(0.0001);
    if tick >= 0.1 {
        1
    } else if tick >= 0.01 {
        2
    } else if tick >= 0.001 {
        3
    } else {
        4
    }
}

fn normalize_tick_price(raw: f64, tick_size: f64) -> f64 {
    let decimals = tick_decimals(tick_size);
    let scale = 10_f64.powi(decimals as i32);
    (raw * scale).round() / scale
}

pub fn round_up_to_tick(price: f64, tick_size: f64) -> f64 {
    let tick = tick_size.max(0.0001);
    let rounded = ((price / tick).ceil() * tick).min(0.99);
    normalize_tick_price(rounded, tick)
}

pub fn format_price_for_tick(price: f64, tick_size: f64) -> String {
    let tick = tick_size.max(0.0001);
    let rounded = round_up_to_tick(price, tick);
    format!("{:.*}", tick_decimals(tick), rounded)
}

/// Enrich all markets in an event with CLOB bid/ask prices and market metadata.
pub async fn enrich_event_markets_with_cache(
    client: &Client,
    config: &Config,
    markets: &mut [Market],
    price_cache: Option<&PriceCache>,
) -> bool {
    let max_in_flight = config.clob_max_concurrency.max(1);
    let cache = price_cache.cloned();
    let results: Vec<(usize, BookSummary, BookSummary, MarketMetadataSnapshot)> =
        stream::iter(markets.iter().enumerate())
            .map(|(idx, market)| {
                let yes_id = market.clob_token_id_yes.clone();
                let no_id = market.clob_token_id_no.clone();
                let condition_id = market.condition_id.clone();
                let client = client.clone();
                let config = config.clone();
                let cache = cache.clone();
                async move {
                    let yes_external = is_external_token_id(&yes_id);
                    let no_external = is_external_token_id(&no_id);
                    let condition_external = is_external_token_id(&condition_id);

                    let yes_cached = if yes_external {
                        Some(BookSummary::default())
                    } else if let Some(cache) = cache.as_ref() {
                        cached_book_summary(cache, &config, &yes_id).await
                    } else {
                        None
                    };
                    let no_cached = if no_external {
                        Some(BookSummary::default())
                    } else if let Some(cache) = cache.as_ref() {
                        cached_book_summary(cache, &config, &no_id).await
                    } else {
                        None
                    };

                    let (yes_res, no_res, info_res) = tokio::join!(
                        async {
                            match yes_cached {
                                Some(summary) => summary,
                                None => get_book_summary(&client, &config, &yes_id).await,
                            }
                        },
                        async {
                            match no_cached {
                                Some(summary) => summary,
                                None => get_book_summary(&client, &config, &no_id).await,
                            }
                        },
                        async {
                            if condition_external {
                                MarketMetadataSnapshot::default()
                            } else {
                                get_market_info(&client, &config, &condition_id).await
                            }
                        },
                    );
                    (idx, yes_res, no_res, info_res)
                }
            })
            .buffer_unordered(max_in_flight)
            .collect()
            .await;

    let mut all_available = true;
    for (idx, yes_book, no_book, info) in results {
        let market = &mut markets[idx];
        market.clob_yes_ask = yes_book.best_ask;
        market.clob_yes_bid = yes_book.best_bid;
        market.clob_no_ask = no_book.best_ask;
        market.clob_no_bid = no_book.best_bid;
        if yes_book.best_ask_size.is_some() {
            market.clob_yes_ask_size = yes_book.best_ask_size;
        }
        if yes_book.best_bid_size.is_some() {
            market.clob_yes_bid_size = yes_book.best_bid_size;
        }
        if no_book.best_ask_size.is_some() {
            market.clob_no_ask_size = no_book.best_ask_size;
        }
        if no_book.best_bid_size.is_some() {
            market.clob_no_bid_size = no_book.best_bid_size;
        }

        let tick_size = yes_book.tick_size.or(no_book.tick_size).or(info.tick_size);
        let min_order_size = yes_book
            .min_order_size
            .or(no_book.min_order_size)
            .or(info.min_order_size);
        let neg_risk = yes_book.neg_risk.or(no_book.neg_risk).or(info.neg_risk);

        market.clob_tick_size = tick_size.or(market.clob_tick_size);
        market.clob_fee_rate = info.fee_rate.or(market.clob_fee_rate);
        market.clob_fee_exponent = info.fee_exponent.or(market.clob_fee_exponent);
        market.clob_min_order_size = min_order_size.or(market.clob_min_order_size);
        market.clob_neg_risk = neg_risk.or(market.clob_neg_risk);
        market.clob_rfq_enabled = info.rfq_enabled.or(market.clob_rfq_enabled);
        if matches!(info.live_orderable, Some(false)) {
            market.closed = true;
            all_available = false;
        }

        market.order_price_min_tick_size = tick_size.or(market.order_price_min_tick_size);
        market.order_min_size = min_order_size.or(market.order_min_size);
        market.taker_fee_rate = info.fee_rate.or(market.taker_fee_rate);

        if yes_book.best_ask.is_none() || no_book.best_ask.is_none() {
            all_available = false;
        }
    }

    all_available
}

pub async fn enrich_event_markets(
    client: &Client,
    config: &Config,
    markets: &mut [Market],
) -> bool {
    enrich_event_markets_with_cache(client, config, markets, None).await
}

/// Global scan-time enrichment: hydrate all event markets from the WebSocket cache
/// and batch /books requests. This path intentionally skips per-market metadata
/// fetches to avoid hammering the CLOB API during wide discovery scans.
pub async fn enrich_all_markets_global_with_cache_budgeted(
    client: &Client,
    config: &Config,
    mut events: Vec<&mut crate::models::Event>,
    price_cache: Option<&PriceCache>,
    rest_token_budget: usize,
) -> QuoteEnrichmentStats {
    let mut token_ids = Vec::new();

    for event in &events {
        for market in &event.markets {
            if !market.clob_token_id_yes.is_empty()
                && !is_external_token_id(&market.clob_token_id_yes)
            {
                token_ids.push(market.clob_token_id_yes.clone());
            }
            if !market.clob_token_id_no.is_empty()
                && !is_external_token_id(&market.clob_token_id_no)
            {
                token_ids.push(market.clob_token_id_no.clone());
            }
        }
    }

    let (price_results, stats) = fetch_book_summaries_with_cache(
        client,
        config,
        token_ids,
        price_cache,
        true,
        rest_token_budget,
    )
    .await;

    for event in &mut events {
        for market in &mut event.markets {
            if let Some(book) = price_results.get(&market.clob_token_id_yes) {
                market.clob_yes_ask = book.best_ask;
                market.clob_yes_bid = book.best_bid;
                if book.best_ask_size.is_some() {
                    market.clob_yes_ask_size = book.best_ask_size;
                }
                if book.best_bid_size.is_some() {
                    market.clob_yes_bid_size = book.best_bid_size;
                }
                market.clob_tick_size = book.tick_size.or(market.clob_tick_size);
                market.clob_min_order_size = book.min_order_size.or(market.clob_min_order_size);
                market.clob_neg_risk = book.neg_risk.or(market.clob_neg_risk);
                market.order_price_min_tick_size =
                    book.tick_size.or(market.order_price_min_tick_size);
                market.order_min_size = book.min_order_size.or(market.order_min_size);
            }
            if let Some(book) = price_results.get(&market.clob_token_id_no) {
                market.clob_no_ask = book.best_ask;
                market.clob_no_bid = book.best_bid;
                if book.best_ask_size.is_some() {
                    market.clob_no_ask_size = book.best_ask_size;
                }
                if book.best_bid_size.is_some() {
                    market.clob_no_bid_size = book.best_bid_size;
                }
                market.clob_tick_size = book.tick_size.or(market.clob_tick_size);
                market.clob_min_order_size = book.min_order_size.or(market.clob_min_order_size);
                market.clob_neg_risk = book.neg_risk.or(market.clob_neg_risk);
                market.order_price_min_tick_size =
                    book.tick_size.or(market.order_price_min_tick_size);
                market.order_min_size = book.min_order_size.or(market.order_min_size);
            }
        }
    }

    stats
}

#[cfg(test)]
pub async fn enrich_all_markets_global_with_cache(
    client: &Client,
    config: &Config,
    events: Vec<&mut crate::models::Event>,
    price_cache: Option<&PriceCache>,
) -> QuoteEnrichmentStats {
    enrich_all_markets_global_with_cache_budgeted(
        client,
        config,
        events,
        price_cache,
        config.quote_refresh_token_budget_per_scan,
    )
    .await
}

async fn get_market_info(
    client: &Client,
    config: &Config,
    condition_id: &str,
) -> MarketMetadataSnapshot {
    let condition_id = condition_id.trim();
    if condition_id.is_empty() {
        return MarketMetadataSnapshot::default();
    }

    let url = format!("{}/clob-markets/{}", config.clob_api_url, condition_id);
    let max_attempts = config.max_retries.max(1);

    for attempt in 1..=max_attempts {
        let _ = clob_wait_for_read_rate_limit(
            config,
            "GET /clob-markets/{condition_id}",
            None,
            Duration::from_millis(0),
        )
        .await;
        match client
            .get(&url)
            .timeout(Duration::from_secs(config.api_timeout_secs))
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return match resp.json::<ClobMarketInfo>().await {
                        Ok(info) => market_metadata_from_info(&info),
                        Err(_) => MarketMetadataSnapshot::default(),
                    };
                }

                observe_clob_http_status(
                    config,
                    "GET /clob-markets/{condition_id}",
                    status.as_u16(),
                );
                let retry_after_ms = retry_after_header_ms(resp.headers());
                if status.as_u16() == 429 {
                    clob_record_read_rate_limit(
                        config,
                        "GET /clob-markets/{condition_id}",
                        retry_wait_ms(config, attempt, retry_after_ms),
                    );
                }
                let should_retry =
                    status.as_u16() == 425 || status.as_u16() == 429 || status.is_server_error();
                if should_retry && attempt < max_attempts {
                    let wait_ms = retry_wait_ms(config, attempt, retry_after_ms);
                    warn!(
                        "CLOB market-info retry for condition {}... status {} (attempt {attempt}/{}), waiting {}ms",
                        &condition_id[..16.min(condition_id.len())],
                        status,
                        max_attempts,
                        wait_ms,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }

                return MarketMetadataSnapshot::default();
            }
            Err(err) => {
                let should_retry = err.is_timeout() || err.is_connect();
                if should_retry && attempt < max_attempts {
                    let wait_ms = retry_wait_ms(config, attempt, None);
                    warn!(
                        "CLOB market-info transport retry for condition {}... (attempt {attempt}/{}), waiting {}ms: {}",
                        &condition_id[..16.min(condition_id.len())],
                        max_attempts,
                        wait_ms,
                        err,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }

                return MarketMetadataSnapshot::default();
            }
        }
    }

    MarketMetadataSnapshot::default()
}

pub async fn verify_live_orderable_markets(
    client: &Client,
    config: &Config,
    planned_condition_tokens: &[(String, String)],
) -> Result<()> {
    let mut planned_by_condition: HashMap<String, HashSet<String>> = HashMap::new();
    for (condition_id, token_id) in planned_condition_tokens {
        let condition_id = condition_id.trim();
        let token_id = token_id.trim();
        if condition_id.is_empty() || token_id.is_empty() {
            return Err(anyhow!(
                "CLOB market-info orderability check received an empty condition or token id"
            ));
        }
        planned_by_condition
            .entry(condition_id.to_string())
            .or_default()
            .insert(token_id.to_string());
    }

    for (condition_id, token_ids) in planned_by_condition {
        let info = get_market_info_strict(
            client,
            config,
            &condition_id,
            Some(live_rest_success_deadline(config, "market-info")),
        )
        .await?;
        ensure_market_info_orderable(&info, &condition_id, Some(&token_ids)).map_err(|err| {
            anyhow!("CLOB market-info orderability rejected condition_id={condition_id}: {err}")
        })?;
        if let Some(game_start_time) = info.game_start_time {
            let quarantine = chrono::Duration::seconds(
                config.live_game_start_quarantine_secs.min(i64::MAX as u64) as i64,
            );
            if Utc::now() + quarantine >= game_start_time {
                return Err(anyhow!(
                    "CLOB market-info orderability rejected condition_id={condition_id}: game_start_time={} within_live_quarantine_secs={}",
                    game_start_time.to_rfc3339(),
                    config.live_game_start_quarantine_secs
                ));
            }
        }
    }

    Ok(())
}

pub async fn verify_live_combo_rfq_markets(
    client: &Client,
    config: &Config,
    condition_ids: &[String],
) -> Result<()> {
    let mut seen = HashSet::new();

    for condition_id in condition_ids {
        let condition_id = condition_id.trim();
        if condition_id.is_empty() {
            return Err(anyhow!(
                "CLOB market-info Combo/RFQ check received an empty condition id"
            ));
        }
        if !seen.insert(condition_id.to_string()) {
            continue;
        }

        let info = get_market_info_strict(
            client,
            config,
            condition_id,
            Some(live_rest_success_deadline(config, "market-info")),
        )
        .await?;
        ensure_market_info_orderable(&info, condition_id, None).map_err(|err| {
            anyhow!("CLOB market-info orderability rejected condition_id={condition_id}: {err}")
        })?;
        if let Some(game_start_time) = info.game_start_time {
            let quarantine = chrono::Duration::seconds(
                config.live_game_start_quarantine_secs.min(i64::MAX as u64) as i64,
            );
            if Utc::now() + quarantine >= game_start_time {
                return Err(anyhow!(
                    "CLOB market-info orderability rejected condition_id={condition_id}: game_start_time={} within_live_quarantine_secs={}",
                    game_start_time.to_rfc3339(),
                    config.live_game_start_quarantine_secs
                ));
            }
        }
        match info.rfq_enabled {
            Some(true) => {}
            Some(false) => {
                return Err(anyhow!(
                    "CLOB market-info RFQ-enabled check rejected condition_id={condition_id}: rfqe=false"
                ));
            }
            None => {
                return Err(anyhow!(
                    "CLOB market-info RFQ-enabled check missing rfqe flag condition_id={condition_id}"
                ));
            }
        }
    }

    Ok(())
}

fn clob_market_orderability_detail(info: &MarketMetadataSnapshot) -> String {
    let mut reasons = Vec::new();
    if matches!(info.live_orderable, Some(false)) {
        reasons.push("market is not live-orderable".to_string());
    }
    let mut delayed_matching = Vec::new();
    if let Some(delay) = info.seconds_delay.filter(|delay| *delay > f64::EPSILON) {
        delayed_matching.push(format!("seconds_delay={delay}"));
    }
    if let Some(min_age) = info
        .minimum_order_age_seconds
        .filter(|min_age| *min_age > f64::EPSILON)
    {
        delayed_matching.push(format!("oas={min_age}"));
    }
    if reasons.is_empty() {
        if delayed_matching.is_empty() {
            "market is not live-orderable".into()
        } else {
            format!(
                "market is not live-orderable; delayed_matching={}",
                delayed_matching.join(",")
            )
        }
    } else {
        reasons.extend(delayed_matching);
        reasons.join("; ")
    }
}

async fn get_market_info_strict(
    client: &Client,
    config: &Config,
    condition_id: &str,
    success_deadline: Option<RestSuccessDeadline>,
) -> Result<MarketMetadataSnapshot> {
    let condition_id = condition_id.trim();
    if condition_id.is_empty() {
        return Err(anyhow!(
            "CLOB market-info orderability check received an empty condition id"
        ));
    }

    let url = format!("{}/clob-markets/{}", config.clob_api_url, condition_id);
    let max_attempts = config.max_retries.max(1);
    let deadline_started_at = Instant::now();

    for attempt in 1..=max_attempts {
        clob_wait_for_read_rate_limit(
            config,
            "GET /clob-markets/{condition_id}",
            success_deadline,
            deadline_started_at.elapsed(),
        )
        .await?;
        let request_timeout = rest_request_timeout_with_deadline(
            config,
            success_deadline,
            deadline_started_at.elapsed(),
        )?;
        match client.get(&url).timeout(request_timeout).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    let info = resp.json::<ClobMarketInfo>().await.map_err(|err| {
                        anyhow!(
                            "CLOB market-info orderability decode error condition_id={condition_id}: {err}"
                        )
                    })?;
                    ensure_rest_success_within_deadline(
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    return Ok(market_metadata_from_info(&info));
                }

                observe_clob_http_status(
                    config,
                    "GET /clob-markets/{condition_id}",
                    status.as_u16(),
                );
                let retry_after_ms = retry_after_header_ms(resp.headers());
                if status.as_u16() == 429 {
                    clob_record_read_rate_limit(
                        config,
                        "GET /clob-markets/{condition_id}",
                        retry_wait_ms(config, attempt, retry_after_ms),
                    );
                }
                let should_retry =
                    status.as_u16() == 425 || status.as_u16() == 429 || status.is_server_error();
                if should_retry && attempt < max_attempts {
                    let wait_ms = retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        retry_after_ms,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    warn!(
                        "CLOB market-info orderability retry for condition {}... status {} (attempt {attempt}/{}), waiting {}ms",
                        &condition_id[..16.min(condition_id.len())],
                        status,
                        max_attempts,
                        wait_ms,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }

                let body = resp.text().await.unwrap_or_default();
                let body_excerpt: String = body.chars().take(256).collect();
                return Err(anyhow!(
                    "CLOB market-info orderability failed condition_id={condition_id} status={status} body={body_excerpt}"
                ));
            }
            Err(err) => {
                let should_retry = err.is_timeout() || err.is_connect();
                if should_retry && attempt < max_attempts {
                    let wait_ms = retry_wait_ms_with_deadline(
                        config,
                        attempt,
                        None,
                        success_deadline,
                        deadline_started_at.elapsed(),
                    )?;
                    warn!(
                        "CLOB market-info orderability transport retry for condition {}... (attempt {attempt}/{}), waiting {}ms: {}",
                        &condition_id[..16.min(condition_id.len())],
                        max_attempts,
                        wait_ms,
                        err,
                    );
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    continue;
                }

                return Err(anyhow!(
                    "CLOB market-info orderability transport error condition_id={condition_id}: {err}"
                ));
            }
        }
    }

    Err(anyhow!(
        "CLOB market-info orderability exhausted {} attempts condition_id={condition_id}",
        max_attempts
    ))
}

/// Get best ask/bid summary for a token from the /book endpoint.
async fn get_book_summary(client: &Client, config: &Config, token_id: &str) -> BookSummary {
    let Some(book) = fetch_book(client, config, token_id).await else {
        return BookSummary::default();
    };
    book_summary_from_book(book)
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    fn test_config(base_url: String) -> Config {
        let mut cfg = Config::from_env();
        cfg.clob_api_url = base_url;
        cfg.api_timeout_secs = 2;
        cfg.max_retries = 3;
        cfg.retry_backoff_base_ms = 1;
        cfg.clob_max_concurrency = 4;
        cfg.clob_book_batch_size = 2;
        cfg
    }

    #[test]
    fn retry_wait_uses_retry_after_header_with_cap() {
        let mut cfg = Config::from_env();
        cfg.retry_backoff_base_ms = 1_000;

        let mut headers = header::HeaderMap::new();
        headers.insert(header::RETRY_AFTER, header::HeaderValue::from_static("5"));

        assert_eq!(retry_after_header_ms(&headers), Some(5_000));
        assert_eq!(
            retry_wait_ms(&cfg, 2, retry_after_header_ms(&headers)),
            5_000
        );

        headers.insert(header::RETRY_AFTER, header::HeaderValue::from_static("120"));
        assert_eq!(
            retry_wait_ms(&cfg, 1, retry_after_header_ms(&headers)),
            CLOB_RETRY_WAIT_MAX_MS
        );
        assert_eq!(retry_wait_ms(&cfg, 3, None), 4_000);
    }

    #[test]
    fn live_retry_wait_fails_before_sleeping_past_deadline() {
        let mut cfg = Config::from_env();
        cfg.retry_backoff_base_ms = 200;
        cfg.live_max_refresh_to_submit_ms = 100;

        let err = retry_wait_ms_with_deadline(
            &cfg,
            1,
            None,
            Some(live_rest_success_deadline(&cfg, "fee-rate")),
            Duration::from_millis(10),
        )
        .unwrap_err();

        assert!(err.to_string().contains("retry wait exceeds"));
        assert!(err.to_string().contains("fee-rate"));
    }

    #[test]
    fn clob_read_rate_limit_is_endpoint_scoped() {
        let mut cfg = Config::from_env();
        cfg.clob_api_url = format!(
            "https://endpoint-scoped-cooldown-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );

        clob_record_read_rate_limit(&cfg, "GET /fee-rate", 100);

        assert!(clob_read_rate_limit_remaining(&cfg, "GET /fee-rate").is_some());
        assert!(clob_read_rate_limit_remaining(&cfg, "POST /books final-depth").is_none());
    }

    #[tokio::test]
    async fn clob_read_rate_limit_fails_before_sleeping_past_live_deadline() {
        let mut cfg = Config::from_env();
        cfg.clob_api_url = format!(
            "https://cooldown-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        cfg.live_max_refresh_to_submit_ms = 10;

        clob_record_read_rate_limit(&cfg, "POST /books final-depth", 100);
        let err = clob_wait_for_read_rate_limit(
            &cfg,
            "POST /books final-depth",
            Some(live_rest_success_deadline(&cfg, "final depth /books")),
            Duration::from_millis(0),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("read cooldown exceeds"));
        assert!(err.to_string().contains("LIVE_MAX_REFRESH_TO_SUBMIT_MS"));
    }

    fn market(yes_id: &str, no_id: &str) -> Market {
        Market {
            question: "Q".into(),
            condition_id: "cond".into(),
            market_slug: "q".into(),
            clob_token_id_yes: yes_id.into(),
            clob_token_id_no: no_id.into(),
            gamma_yes_price: 0.4,
            gamma_no_price: 0.6,
            clob_yes_ask: None,
            clob_yes_bid: None,
            clob_no_ask: None,
            clob_no_bid: None,
            clob_yes_ask_size: None,
            clob_yes_bid_size: None,
            clob_no_ask_size: None,
            clob_no_bid_size: None,
            fees_enabled: Some(true),
            taker_fee_rate: None,
            maker_fee_rate: None,
            clob_taker_fee_bps: None,
            clob_fee_rate: None,
            clob_fee_exponent: None,
            order_price_min_tick_size: None,
            order_min_size: None,
            clob_tick_size: None,
            clob_min_order_size: None,
            clob_neg_risk: None,
            clob_rfq_enabled: None,
            liquidity: 10_000.0,
            closed: false,
        }
    }

    #[test]
    fn market_info_parses_boolean_itode_as_delay() {
        let info: ClobMarketInfo = serde_json::from_str(
            r#"{"accepting_orders":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"itode":true}"#,
        )
        .expect("market info parses");

        assert_eq!(market_metadata_from_info(&info).seconds_delay, Some(0.25));
        assert_eq!(clob_market_live_orderable(&info), Some(true));
    }

    #[test]
    fn market_info_parses_false_and_numeric_delay_forms() {
        let no_delay: ClobMarketInfo =
            serde_json::from_str(r#"{"itode":false}"#).expect("false itode parses");
        assert_eq!(
            market_metadata_from_info(&no_delay).seconds_delay,
            Some(0.0)
        );
        assert_eq!(clob_market_live_orderable(&no_delay), None);

        let numeric_delay: ClobMarketInfo =
            serde_json::from_str(r#"{"seconds_delay":"0.5"}"#).expect("string delay parses");
        assert_eq!(
            market_metadata_from_info(&numeric_delay).seconds_delay,
            Some(0.5)
        );
        assert_eq!(clob_market_live_orderable(&numeric_delay), None);

        let compact_delay: ClobMarketInfo =
            serde_json::from_str(r#"{"sd":"2"}"#).expect("compact sd parses");
        assert_eq!(
            market_metadata_from_info(&compact_delay).seconds_delay,
            Some(2.0)
        );
    }

    #[test]
    fn market_info_records_positive_minimum_order_age_for_orderability_check() {
        let info: ClobMarketInfo =
            serde_json::from_str(r#"{"accepting_orders":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"seconds_delay":0,"oas":"1.25"}"#)
                .expect("oas parses");

        let metadata = market_metadata_from_info(&info);

        assert_eq!(metadata.minimum_order_age_seconds, Some(1.25));
        assert_eq!(clob_market_live_orderable(&info), Some(true));
        assert!(ensure_market_info_orderable(&metadata, "cond", None)
            .unwrap_err()
            .to_string()
            .contains("oas/minimum_order_age_seconds=1.25"));
    }

    #[test]
    fn market_info_parses_game_start_time() {
        let info: ClobMarketInfo =
            serde_json::from_str(r#"{"gst":"2026-06-25T12:30:00Z"}"#).expect("compact gst parses");

        let metadata = market_metadata_from_info(&info);

        assert_eq!(
            metadata.game_start_time.map(|dt| dt.to_rfc3339()),
            Some("2026-06-25T12:30:00+00:00".into())
        );
    }

    #[test]
    fn market_info_accepts_null_game_start_and_ignores_rewards_object() {
        let info: ClobMarketInfo = serde_json::from_str(
            r#"{"c":"condition","t":[{"t":"yes"},{"t":"no"}],"mts":0.01,"mos":5,"nr":true,"r":{"moas":4},"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0,"gst":null}"#,
        )
        .expect("official V2 rewards/null-gst shape parses");
        let metadata = market_metadata_from_info(&info);

        assert_eq!(metadata.neg_risk, Some(true));
        assert!(!metadata.game_start_time_present);
        assert_eq!(metadata.game_start_time, None);
        ensure_market_info_orderable(&metadata, "condition", None)
            .expect("null gst means no scheduled game start");
    }

    #[test]
    fn market_info_parses_rfq_enabled_flag() {
        let info: ClobMarketInfo = serde_json::from_str(r#"{"rfqe":true}"#).expect("rfqe parses");
        assert_eq!(market_metadata_from_info(&info).rfq_enabled, Some(true));

        let info: ClobMarketInfo =
            serde_json::from_str(r#"{"rfqEnabled":false}"#).expect("rfqEnabled parses");
        assert_eq!(market_metadata_from_info(&info).rfq_enabled, Some(false));
    }

    #[test]
    fn market_info_parses_current_v2_fee_details() {
        let info: ClobMarketInfo = serde_json::from_str(
            r#"{"c":"condition","t":[{"t":"yes","o":"Yes"},{"t":"no","o":"No"}],"mts":0.01,"mos":5,"fd":{"r":0.02,"e":2,"to":true},"rfqe":true,"ao":true,"sd":2,"gst":"2026-06-25T12:30:00Z"}"#,
        )
        .expect("compact V2 market info parses");
        let metadata = market_metadata_from_info(&info);

        assert_eq!(metadata.condition_id.as_deref(), Some("condition"));
        assert_eq!(metadata.token_ids, vec!["yes", "no"]);
        assert_eq!(metadata.fee_rate, Some(0.02));
        assert_eq!(metadata.fee_exponent, Some(2));
        assert_eq!(metadata.accepting_orders, Some(true));
        assert_eq!(metadata.seconds_delay, Some(2.0));
        assert_eq!(
            metadata.game_start_time.map(|dt| dt.to_rfc3339()),
            Some("2026-06-25T12:30:00+00:00".into())
        );
    }

    #[tokio::test]
    async fn verify_live_orderable_markets_rejects_delayed_matching_metadata() {
        let server = MockServer::start_async().await;
        let seconds_delay = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-sd");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":2,"oas":0}"#);
            })
            .await;
        let minimum_age = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-oas");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0.75}"#);
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;

        let sd_err = verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("cond-sd".into(), "token".into())],
        )
        .await
        .unwrap_err();
        assert!(sd_err.to_string().contains("sd/seconds_delay=2"));

        let oas_err = verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("cond-oas".into(), "token".into())],
        )
        .await
        .unwrap_err();
        assert!(oas_err
            .to_string()
            .contains("oas/minimum_order_age_seconds=0.75"));

        seconds_delay.assert_calls_async(1).await;
        minimum_age.assert_calls_async(1).await;
    }

    #[test]
    fn market_orderability_rejects_invalid_delay_and_game_start_metadata() {
        for (body, expected) in [
            (
                r#"{"ao":true,"sd":"NaN","oas":0}"#,
                "sd/seconds_delay is invalid",
            ),
            (
                r#"{"ao":true,"sd":0,"oas":-1}"#,
                "oas/minimum_order_age_seconds is invalid",
            ),
            (
                r#"{"ao":true,"sd":0,"oas":0,"gst":"not-a-time"}"#,
                "gst/game_start_time is malformed",
            ),
        ] {
            let wire: ClobMarketInfo = serde_json::from_str(body).expect("wire metadata parses");
            let snapshot = market_metadata_from_info(&wire);
            let err = ensure_market_info_orderable(&snapshot, "cond", None).unwrap_err();
            assert!(err.to_string().contains(expected), "{err:#}");
        }
    }

    #[tokio::test]
    async fn verify_live_orderable_markets_accepts_complete_compact_mapping() {
        let server = MockServer::start_async().await;
        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond","t":[{"t":"planned-token","o":"Yes"},{"t":"other-token","o":"No"}],"mts":0.01,"mos":5,"fd":{"r":0.02,"e":2,"to":true},"ao":true,"sd":0,"oas":0}"#);
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;

        verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("cond".into(), "planned-token".into())],
        )
        .await
        .expect("complete compact market mapping is orderable");

        info.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn verify_live_orderable_markets_rejects_sparse_or_mismatched_compact_metadata() {
        let server = MockServer::start_async().await;
        let sparse = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/sparse");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"sparse","fd":{"r":0.02,"e":2},"ao":true}"#);
            })
            .await;
        let mismatch = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/mismatch");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"mismatch","t":[{"t":"yes","o":"Yes"},{"t":"no","o":"No"}],"mts":0.01,"mos":5,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0}"#);
            })
            .await;
        let explicitly_closed = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/closed");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"closed","t":[{"t":"planned","o":"Yes"},{"t":"no","o":"No"}],"mts":0.01,"mos":5,"ao":false}"#);
            })
            .await;
        let missing_accepting_orders = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/missing-ao");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"missing-ao","t":[{"t":"planned","o":"Yes"},{"t":"no","o":"No"}],"mts":0.01,"mos":5}"#);
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;

        let sparse_err = verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("sparse".into(), "planned".into())],
        )
        .await
        .unwrap_err();
        assert!(sparse_err.to_string().contains("valid mts"));

        let mismatch_err = verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("mismatch".into(), "planned".into())],
        )
        .await
        .unwrap_err();
        assert!(mismatch_err.to_string().contains("missing planned token"));

        let closed_err = verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("closed".into(), "planned".into())],
        )
        .await
        .unwrap_err();
        assert!(closed_err.to_string().contains("not live-orderable"));

        let missing_ao_err = verify_live_orderable_markets(
            &Client::new(),
            &cfg,
            &[("missing-ao".into(), "planned".into())],
        )
        .await
        .unwrap_err();
        assert!(missing_ao_err.to_string().contains("ao=true"));

        sparse.assert_calls_async(1).await;
        mismatch.assert_calls_async(1).await;
        explicitly_closed.assert_calls_async(1).await;
        missing_accepting_orders.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn verify_live_orderable_markets_rejects_near_game_start() {
        let server = MockServer::start_async().await;
        let game_start_time = (Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
        let body = serde_json::json!({
            "c": "cond",
            "t": [
                {"t": "token", "o": "Yes"},
                {"t": "other", "o": "No"}
            ],
            "mts": 0.01,
            "mos": 1,
            "ao": true,
            "active": true,
            "archived": false,
            "closed": false,
            "enable_order_book": true,
            "sd": 0,
            "oas": 0,
            "gst": game_start_time,
        })
        .to_string();
        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(body);
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;
        cfg.live_game_start_quarantine_secs = 300;

        let err =
            verify_live_orderable_markets(&Client::new(), &cfg, &[("cond".into(), "token".into())])
                .await
                .unwrap_err();

        assert!(err.to_string().contains("within_live_quarantine_secs=300"));
        info.assert_calls_async(1).await;
    }

    #[test]
    fn depth_snapshot_cutoff_ask_for_shares_returns_worst_required_level() {
        let snapshot = DepthSnapshot {
            token_id: "token".into(),
            asks: vec![(0.40, 10.0), (0.45, 5.0), (0.50, 10.0)],
            tick_size: Some(0.01),
            min_order_size: Some(1.0),
            neg_risk: Some(false),
            observed_at: Some(Instant::now()),
            venue_timestamp_ms: Some(1_000),
            book_hash: Some("hash".into()),
        };

        assert_eq!(snapshot.cutoff_ask_for_shares(10.0), Some(0.40));
        assert_eq!(snapshot.cutoff_ask_for_shares(12.0), Some(0.45));
        assert_eq!(snapshot.cutoff_ask_for_shares(25.0), Some(0.50));
        assert_eq!(snapshot.cutoff_ask_for_shares(25.01), None);
    }

    #[tokio::test]
    async fn depth_snapshots_batch_once_and_compute_local_depth() {
        let server = MockServer::start_async().await;
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "a",
                        "asks": [
                            {"price":"0.40","size":"10"},
                            {"price":"0.45","size":"20"}
                        ],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-a"
                    }
                ]));
            })
            .await;
        let cfg = test_config(server.base_url());
        let snapshots = get_depth_snapshots(&Client::new(), &cfg, &[String::from("a")])
            .await
            .expect("depth snapshots");
        let snapshot = snapshots.get("a").expect("depth snapshot");

        assert_eq!(snapshot.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(snapshot.book_hash.as_deref(), Some("h-a"));
        assert_eq!(snapshot.tick_size, Some(0.01));
        assert_eq!(snapshot.min_order_size, Some(1.0));
        assert_eq!(snapshot.neg_risk, Some(true));
        assert!((snapshot.available_shares_at_price(0.41) - 10.0).abs() < 1e-9);
        assert!(
            (snapshot.average_ask_for_shares(15.0).unwrap() - (4.0 + 2.25) / 15.0).abs() < 1e-9
        );
        books.assert_calls_async(1).await;
    }

    #[test]
    fn clob_prices_parser_accepts_documented_and_wrapped_shapes() {
        let token_ids = vec![String::from("111"), String::from("222")];
        let documented = serde_json::json!({
            "111": {"SELL": "0.375"},
            "222": {"SELL": 0.42},
        });
        let prices = parse_clob_prices_response(&documented, &token_ids, "SELL");
        assert_eq!(prices.get("111"), Some(&0.375));
        assert_eq!(prices.get("222"), Some(&0.42));

        let wrapped = serde_json::json!({
            "data": [
                {"token_id": "111", "side": "SELL", "price": "0.41"},
                {"asset_id": "222", "side": "BUY", "price": "0.58"},
                {"asset_id": "222", "side": "SELL", "price": "0.43"}
            ]
        });
        let prices = parse_clob_prices_response(&wrapped, &token_ids, "SELL");
        assert_eq!(prices.get("111"), Some(&0.41));
        assert_eq!(prices.get("222"), Some(&0.43));
    }

    #[tokio::test]
    async fn live_sell_prices_require_all_requested_tokens() {
        let server = MockServer::start_async().await;
        let prices = server
            .mock_async(|when, then| {
                when.method(POST).path("/prices");
                then.status(200).json_body(serde_json::json!({
                    "a": {"SELL": "0.40"}
                }));
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;

        let err = get_live_sell_prices(
            &Client::new(),
            &cfg,
            &[String::from("a"), String::from("b")],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("incomplete successful response"));
        assert!(err.to_string().contains("\"b\""));
        prices.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn live_depth_snapshots_time_out_slow_successful_response() {
        let server = MockServer::start_async().await;
        let _books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200)
                    .delay(Duration::from_millis(25))
                    .json_body(serde_json::json!([
                        {
                            "asset_id": "a",
                            "asks": [{"price":"0.40","size":"10"}],
                            "tick_size": "0.01",
                            "min_order_size": "1",
                            "neg_risk": true,
                            "timestamp": "1700000002000",
                            "hash": "h-a"
                        }
                    ]));
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;
        cfg.live_max_refresh_to_submit_ms = 1;

        let err = get_live_depth_snapshots(&Client::new(), &cfg, &[String::from("a")])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("transport error"));
        assert!(err.to_string().contains("final depth /books"));
    }

    #[tokio::test]
    async fn depth_snapshots_return_status_errors_for_live_breaker() {
        let server = MockServer::start_async().await;
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(429).body("rate limited");
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;

        let err = get_depth_snapshots(&Client::new(), &cfg, &[String::from("a")])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("status 429"));
        assert!(err.to_string().contains("rate limited"));
        books.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn depth_snapshots_require_all_requested_tokens_and_metadata() {
        let server = MockServer::start_async().await;
        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200).json_body(serde_json::json!([
                    {
                        "asset_id": "a",
                        "asks": [{"price":"0.40","size":"10"}],
                        "tick_size": "0.01",
                        "min_order_size": "1",
                        "neg_risk": true,
                        "timestamp": "1700000002000",
                        "hash": "h-a"
                    },
                    {
                        "asset_id": "b",
                        "asks": [{"price":"0.50","size":"10"}]
                    }
                ]));
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;

        let err = get_depth_snapshots(
            &Client::new(),
            &cfg,
            &[String::from("a"), String::from("b"), String::from("c")],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("incomplete successful response"));
        assert!(err.to_string().contains("\"b\""));
        assert!(err.to_string().contains("\"c\""));
        books.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn live_fee_schedules_use_compact_v2_fd_without_legacy_endpoint() {
        let server = MockServer::start_async().await;
        let market = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/condition-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"condition-a","t":[{"t":"yes","o":"Yes"},{"t":"no","o":"No"}],"mts":0.01,"mos":5,"fd":{"r":0.02,"e":2,"to":true}}"#);
            })
            .await;
        let legacy = server
            .mock_async(|when, then| {
                when.method(GET).path("/fee-rate");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"base_fee":1000}"#);
            })
            .await;
        let cfg = test_config(server.base_url());

        let schedules = get_live_fee_schedules(
            &Client::new(),
            &cfg,
            &["condition-a".into(), "condition-a".into()],
        )
        .await
        .expect("V2 fee schedule");

        assert_eq!(
            schedules.get("condition-a"),
            Some(&LiveClobFeeSchedule {
                rate: 0.02,
                exponent: 2,
            })
        );
        market.assert_calls_async(1).await;
        legacy.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn live_fee_schedules_fail_closed_without_fd_exponent() {
        let server = MockServer::start_async().await;
        let market = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/condition-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"condition-a","fd":{"r":0.02}}"#);
            })
            .await;
        let cfg = test_config(server.base_url());

        let err = get_live_fee_schedules(&Client::new(), &cfg, &["condition-a".into()])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("missing fd.e"));
        market.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn enrich_event_markets_populates_prices_and_metadata() {
        let server = MockServer::start_async().await;

        let yes = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/book")
                    .query_param("token_id", "yes-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.33","size":"1000"}],"bids":[{"price":"0.30","size":"1000"}]}"#);
            })
            .await;

        let no = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/book")
                    .query_param("token_id", "no-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.67","size":"1000"}],"bids":[{"price":"0.64","size":"1000"}]}"#);
            })
            .await;

        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond","t":[{"t":"yes-a","o":"Yes"},{"t":"no-a","o":"No"}],"mos":5,"mts":0.01,"tbf":1000,"fd":{"r":0.05,"e":1,"to":true},"nr":true,"rfqe":true}"#);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();
        let mut markets = vec![market("yes-a", "no-a")];
        markets[0].clob_taker_fee_bps = Some(1_000);

        let ok = enrich_event_markets(&client, &cfg, &mut markets).await;
        assert!(ok);
        assert_eq!(markets[0].clob_yes_ask, Some(0.33));
        assert_eq!(markets[0].clob_no_ask, Some(0.67));
        assert_eq!(markets[0].clob_yes_ask_size, Some(1000.0));
        assert_eq!(markets[0].clob_yes_bid_size, Some(1000.0));
        assert_eq!(markets[0].clob_no_bid_size, Some(1000.0));
        assert_eq!(markets[0].clob_tick_size, Some(0.01));
        assert_eq!(markets[0].clob_taker_fee_bps, Some(1_000));
        assert_eq!(markets[0].clob_fee_rate, Some(0.05));
        assert_eq!(markets[0].clob_fee_exponent, Some(1));
        assert_eq!(markets[0].clob_rfq_enabled, Some(true));
        assert!(!markets[0].closed);
        yes.assert_calls_async(1).await;
        no.assert_calls_async(1).await;
        info.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn verify_live_combo_rfq_markets_requires_true_rfq_flag() {
        let server = MockServer::start_async().await;
        let disabled = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-disabled");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond-disabled","t":[{"t":"yes"},{"t":"no"}],"mts":0.01,"mos":1,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0,"rfqe":false}"#);
            })
            .await;
        let missing = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-missing");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond-missing","t":[{"t":"yes"},{"t":"no"}],"mts":0.01,"mos":1,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0}"#);
            })
            .await;
        let enabled = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-enabled");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"c":"cond-enabled","t":[{"t":"yes"},{"t":"no"}],"mts":0.01,"mos":1,"ao":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"sd":0,"oas":0,"rfqe":true}"#);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();

        let err = verify_live_combo_rfq_markets(&client, &cfg, &["cond-disabled".into()])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rfqe=false"));

        let err = verify_live_combo_rfq_markets(&client, &cfg, &["cond-missing".into()])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing rfqe flag"));

        verify_live_combo_rfq_markets(&client, &cfg, &["cond-enabled".into()])
            .await
            .unwrap();

        disabled.assert_calls_async(1).await;
        missing.assert_calls_async(1).await;
        enabled.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn verify_live_combo_rfq_markets_times_out_slow_market_info_response() {
        let server = MockServer::start_async().await;
        let _info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond-slow");
                then.status(200)
                    .delay(Duration::from_millis(25))
                    .header("content-type", "application/json")
                    .body(r#"{"accepting_orders":true,"active":true,"archived":false,"closed":false,"enable_order_book":true,"seconds_delay":0,"oas":0,"rfqe":true}"#);
            })
            .await;
        let mut cfg = test_config(server.base_url());
        cfg.max_retries = 1;
        cfg.live_max_refresh_to_submit_ms = 1;

        let err = verify_live_combo_rfq_markets(&Client::new(), &cfg, &["cond-slow".into()])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("transport error"));
        assert!(err.to_string().contains("market-info"));
    }

    #[tokio::test]
    async fn enrich_event_markets_marks_non_orderable_clob_market_closed() {
        let server = MockServer::start_async().await;

        let yes = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/book")
                    .query_param("token_id", "yes-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.33","size":"1000"}],"bids":[{"price":"0.30","size":"1000"}]}"#);
            })
            .await;

        let no = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/book")
                    .query_param("token_id", "no-a");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.67","size":"1000"}],"bids":[{"price":"0.64","size":"1000"}]}"#);
            })
            .await;

        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/cond");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"minimum_order_size":5,"minimum_tick_size":0.01,"neg_risk":true,"accepting_orders":false,"active":true,"archived":false,"closed":false,"enable_order_book":true,"seconds_delay":0}"#);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();
        let mut markets = vec![market("yes-a", "no-a")];

        let ok = enrich_event_markets(&client, &cfg, &mut markets).await;
        assert!(!ok);
        assert!(markets[0].closed);
        yes.assert_calls_async(1).await;
        no.assert_calls_async(1).await;
        info.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn enrich_event_markets_skips_external_tokens() {
        let server = MockServer::start_async().await;

        let book = server
            .mock_async(|when, then| {
                when.method(GET).path("/book");
                then.status(500);
            })
            .await;
        let info = server
            .mock_async(|when, then| {
                when.method(GET).path("/clob-markets/external:kalshi:cond");
                then.status(500);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();
        let mut external = market("external:kalshi:yes", "external:kalshi:no");
        external.condition_id = "external:kalshi:cond".into();
        let mut markets = vec![external];

        let ok = enrich_event_markets(&client, &cfg, &mut markets).await;

        assert!(!ok);
        assert_eq!(markets[0].clob_yes_ask, None);
        assert_eq!(markets[0].clob_no_ask, None);
        book.assert_calls_async(0).await;
        info.assert_calls_async(0).await;
    }

    #[test]
    fn best_ask_summary_aggregates_equal_price_levels() {
        let levels = vec![
            ClobBookLevel {
                price: Some("0.40".into()),
                size: Some("10".into()),
            },
            ClobBookLevel {
                price: Some("0.40".into()),
                size: Some("15".into()),
            },
            ClobBookLevel {
                price: Some("0.41".into()),
                size: Some("50".into()),
            },
        ];
        let (price, size) = best_ask_summary(levels);
        assert_eq!(price, Some(0.40));
        assert_eq!(size, Some(25.0));
    }

    #[test]
    fn merge_book_summary_clears_authoritative_no_ask_or_bid() {
        let mut target = BookSummary {
            best_ask: Some(0.40),
            best_bid: Some(0.39),
            best_ask_size: Some(100.0),
            best_bid_size: Some(90.0),
            ask_depth: vec![(0.40, 100.0)],
            bid_depth: vec![(0.39, 90.0)],
            tick_size: Some(0.01),
            min_order_size: Some(5.0),
            neg_risk: Some(true),
            venue_timestamp_ms: Some(1_700_000_001_000),
            book_hash: Some("old".into()),
        };
        let update = BookSummary {
            best_ask: None,
            best_bid: None,
            best_ask_size: None,
            best_bid_size: None,
            ask_depth: Vec::new(),
            bid_depth: Vec::new(),
            tick_size: None,
            min_order_size: None,
            neg_risk: None,
            venue_timestamp_ms: Some(1_700_000_002_000),
            book_hash: Some("new".into()),
        };

        merge_book_summary(&mut target, update);

        assert_eq!(target.best_ask, None);
        assert_eq!(target.best_bid, None);
        assert_eq!(target.best_ask_size, None);
        assert_eq!(target.best_bid_size, None);
        assert!(target.ask_depth.is_empty());
        assert!(target.bid_depth.is_empty());
        assert_eq!(target.tick_size, Some(0.01));
        assert_eq!(target.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(target.book_hash.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn book_summary_reads_book_level_metadata() {
        let server = MockServer::start_async().await;

        let book = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/book")
                    .query_param("token_id", "yes-meta");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"asks":[{"price":"0.45","size":"10"}],"bids":[{"price":"0.44","size":"9"}],"tick_size":"0.001","min_order_size":"2","neg_risk":true,"timestamp":"1700000002000","hash":"rest-hash"}"#);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();
        let summary = get_book_summary(&client, &cfg, "yes-meta").await;
        assert_eq!(summary.tick_size, Some(0.001));
        assert_eq!(summary.min_order_size, Some(2.0));
        assert_eq!(summary.neg_risk, Some(true));
        assert_eq!(summary.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(summary.book_hash.as_deref(), Some("rest-hash"));
        book.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn global_enrichment_uses_batch_books_and_ws_cache() {
        let server = MockServer::start_async().await;

        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"[
                        {"asset_id":"yes-b","asks":[{"price":"0.31","size":"50"}],"bids":[{"price":"0.30","size":"25"}],"tick_size":"0.01","timestamp":"1700000002000","hash":"yes-b-hash"},
                        {"asset_id":"no-b","asks":[{"price":"0.69","size":"50"}],"bids":[{"price":"0.68","size":"25"}],"tick_size":"0.01","timestamp":"1700000002001","hash":"no-b-hash"}
                    ]"#);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let cached_yes_at = std::time::Instant::now();
        let cached_no_at = std::time::Instant::now();
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-a".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.33),
                    best_bid: Some(0.32),
                    best_ask_size: Some(100.0),
                    best_bid_size: Some(100.0),
                    ask_depth: vec![(0.33, 100.0)],
                    bid_depth: vec![(0.32, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: None,
                    book_hash: None,
                    snapshot_ready: true,
                    last_updated: cached_yes_at,
                },
            );
            guard.insert(
                "no-a".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.67),
                    best_bid: Some(0.66),
                    best_ask_size: Some(100.0),
                    best_bid_size: Some(100.0),
                    ask_depth: vec![(0.67, 100.0)],
                    bid_depth: vec![(0.66, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: None,
                    book_hash: None,
                    snapshot_ready: true,
                    last_updated: cached_no_at,
                },
            );
        }

        let mut events = [
            crate::models::Event {
                event_id: "1".into(),
                title: "A".into(),
                slug: "a".into(),
                category: "politics".into(),
                enable_neg_risk: true,
                neg_risk: true,
                neg_risk_augmented: false,
                lifecycle: Default::default(),
                markets: vec![market("yes-a", "no-a")],
            },
            crate::models::Event {
                event_id: "2".into(),
                title: "B".into(),
                slug: "b".into(),
                category: "politics".into(),
                enable_neg_risk: true,
                neg_risk: true,
                neg_risk_augmented: false,
                lifecycle: Default::default(),
                markets: vec![market("yes-b", "no-b")],
            },
        ];

        let refs: Vec<&mut crate::models::Event> = events.iter_mut().collect();
        let stats = enrich_all_markets_global_with_cache(&client, &cfg, refs, Some(&cache)).await;

        assert_eq!(stats.total_tokens, 4);
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.rest_requested, 2);
        assert_eq!(stats.rest_resolved, 2);
        assert_eq!(stats.unresolved_tokens, 0);

        assert_eq!(events[0].markets[0].clob_yes_ask, Some(0.33));
        assert_eq!(events[0].markets[0].clob_no_ask, Some(0.67));
        assert_eq!(events[1].markets[0].clob_yes_ask, Some(0.31));
        assert_eq!(events[1].markets[0].clob_no_ask, Some(0.69));

        let guard = cache.read().await;
        assert_eq!(guard.get("yes-b").and_then(|p| p.best_ask), Some(0.31));
        assert_eq!(guard.get("no-b").and_then(|p| p.best_ask_size), Some(50.0));
        assert_eq!(
            guard.get("yes-b").and_then(|p| p.venue_timestamp_ms),
            Some(1_700_000_002_000)
        );
        assert_eq!(
            guard.get("yes-b").and_then(|p| p.book_hash.as_deref()),
            Some("yes-b-hash")
        );
        assert_eq!(
            guard.get("yes-a").map(|p| p.last_updated),
            Some(cached_yes_at)
        );
        assert_eq!(
            guard.get("no-a").map(|p| p.last_updated),
            Some(cached_no_at)
        );
        books.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn global_enrichment_uses_fresh_cached_no_ask_without_rest_retry() {
        let server = MockServer::start_async().await;

        let books = server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(500);
            })
            .await;

        let cfg = test_config(server.base_url());
        let client = Client::new();
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "yes-noask".into(),
                crate::ws_client::Price {
                    best_ask: None,
                    best_bid: Some(0.32),
                    best_ask_size: None,
                    best_bid_size: Some(100.0),
                    ask_depth: Vec::new(),
                    bid_depth: vec![(0.32, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: None,
                    book_hash: Some("noask-hash".into()),
                    snapshot_ready: true,
                    last_updated: std::time::Instant::now(),
                },
            );
        }

        let (summaries, stats) = fetch_book_summaries_with_cache(
            &client,
            &cfg,
            vec!["yes-noask".into()],
            Some(&cache),
            true,
            10,
        )
        .await;

        assert_eq!(stats.rest_requested, 0);
        assert_eq!(stats.hard_unresolved_tokens, 0);
        assert_eq!(
            summaries
                .get("yes-noask")
                .and_then(|summary| summary.best_ask),
            None
        );
        books.assert_calls_async(0).await;
    }

    #[tokio::test]
    async fn global_enrichment_caps_slow_scan_books_by_deadline() {
        let server = MockServer::start_async().await;

        server
            .mock_async(|when, then| {
                when.method(POST).path("/books");
                then.status(200)
                    .delay(Duration::from_millis(50))
                    .header("content-type", "application/json")
                    .body(r#"[{"asset_id":"slow-token","asks":[{"price":"0.41","size":"10"}]}]"#);
            })
            .await;

        let mut cfg = test_config(server.base_url());
        cfg.clob_book_batch_size = 1;
        cfg.live_max_refresh_to_submit_ms = 5;
        cfg.max_retries = 2;
        let client = Client::new();

        let started_at = Instant::now();
        let (summaries, stats) = fetch_book_summaries_with_cache(
            &client,
            &cfg,
            vec!["slow-token".into()],
            None,
            true,
            10,
        )
        .await;

        assert!(
            started_at.elapsed() < Duration::from_millis(250),
            "scan REST deadline should cap slow /books hydration"
        );
        assert!(summaries
            .get("slow-token")
            .and_then(|summary| summary.best_ask)
            .is_none());
        assert_eq!(stats.rest_requested, 1);
        assert_eq!(stats.rest_batches, 1);
        assert_eq!(stats.rest_resolved, 0);
        assert_eq!(stats.hard_unresolved_tokens, 1);
        assert_eq!(stats.missing_book_tokens, 1);
    }

    #[tokio::test]
    async fn rest_cache_update_skips_regressive_book_timestamp() {
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "token-regressive".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.40),
                    best_bid: Some(0.39),
                    best_ask_size: Some(100.0),
                    best_bid_size: Some(100.0),
                    ask_depth: vec![(0.40, 100.0)],
                    bid_depth: vec![(0.39, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: Some(1_700_000_002_000),
                    book_hash: Some("newer".into()),
                    snapshot_ready: true,
                    last_updated: std::time::Instant::now(),
                },
            );
        }

        let mut summaries = HashMap::new();
        summaries.insert(
            "token-regressive".into(),
            BookSummary {
                best_ask: Some(0.55),
                best_bid: Some(0.54),
                best_ask_size: Some(10.0),
                best_bid_size: Some(10.0),
                ask_depth: vec![(0.55, 10.0)],
                bid_depth: vec![(0.54, 10.0)],
                tick_size: Some(0.01),
                min_order_size: None,
                neg_risk: None,
                venue_timestamp_ms: Some(1_700_000_001_000),
                book_hash: Some("older".into()),
            },
        );

        update_quote_cache_from_summaries(&cache, &summaries).await;

        let guard = cache.read().await;
        let price = guard.get("token-regressive").expect("price retained");
        assert_eq!(price.best_ask, Some(0.40));
        assert_eq!(price.best_bid, Some(0.39));
        assert_eq!(price.best_ask_size, Some(100.0));
        assert_eq!(price.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(price.book_hash.as_deref(), Some("newer"));
    }

    #[tokio::test]
    async fn rest_cache_update_clears_stale_ask_when_authoritative_book_has_no_ask() {
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "token-noask".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.40),
                    best_bid: Some(0.39),
                    best_ask_size: Some(100.0),
                    best_bid_size: Some(100.0),
                    ask_depth: vec![(0.40, 100.0)],
                    bid_depth: vec![(0.39, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: Some(1_700_000_001_000),
                    book_hash: Some("old".into()),
                    snapshot_ready: true,
                    last_updated: std::time::Instant::now(),
                },
            );
        }

        let mut summaries = HashMap::new();
        summaries.insert(
            "token-noask".into(),
            BookSummary {
                best_ask: None,
                best_bid: Some(0.38),
                best_ask_size: None,
                best_bid_size: Some(25.0),
                ask_depth: Vec::new(),
                bid_depth: vec![(0.38, 25.0)],
                tick_size: Some(0.01),
                min_order_size: None,
                neg_risk: None,
                venue_timestamp_ms: Some(1_700_000_002_000),
                book_hash: Some("new".into()),
            },
        );

        update_quote_cache_from_summaries(&cache, &summaries).await;

        let guard = cache.read().await;
        let price = guard.get("token-noask").expect("price retained");
        assert_eq!(price.best_ask, None);
        assert_eq!(price.best_ask_size, None);
        assert!(price.ask_depth.is_empty());
        assert_eq!(price.best_bid, Some(0.38));
        assert_eq!(price.best_bid_size, Some(25.0));
        assert_eq!(price.bid_depth, vec![(0.38, 25.0)]);
        assert_eq!(price.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(price.book_hash.as_deref(), Some("new"));
        assert!(price.snapshot_ready);
    }

    #[tokio::test]
    async fn cached_book_summary_rejects_stale_venue_timestamp() {
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            guard.insert(
                "token-stale-venue".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.40),
                    best_bid: Some(0.39),
                    best_ask_size: Some(100.0),
                    best_bid_size: Some(100.0),
                    ask_depth: vec![(0.40, 100.0)],
                    bid_depth: vec![(0.39, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: Some(1),
                    book_hash: Some("stale".into()),
                    snapshot_ready: true,
                    last_updated: std::time::Instant::now(),
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = 1_000;

        assert!(cached_book_summary(&cache, &cfg, "token-stale-venue")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn cached_book_summary_rejects_future_venue_timestamp() {
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        {
            let mut guard = cache.write().await;
            guard.insert(
                "token-future-venue".into(),
                crate::ws_client::Price {
                    best_ask: Some(0.40),
                    best_bid: Some(0.39),
                    best_ask_size: Some(100.0),
                    best_bid_size: Some(100.0),
                    ask_depth: vec![(0.40, 100.0)],
                    bid_depth: vec![(0.39, 100.0)],
                    recent_trades: Default::default(),
                    recent_depth_changes: Default::default(),
                    tick_size: Some(0.01),
                    venue_timestamp_ms: Some(now_ms + 10_000),
                    book_hash: Some("future".into()),
                    snapshot_ready: true,
                    last_updated: std::time::Instant::now(),
                },
            );
        }
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = 1_000;

        assert!(cached_book_summary(&cache, &cfg, "token-future-venue")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn cached_depth_snapshots_read_multi_token_cache() {
        let cache: PriceCache = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().await;
            for (token_id, ask, hash) in [("token-a", 0.40, "hash-a"), ("token-b", 0.42, "hash-b")]
            {
                guard.insert(
                    token_id.into(),
                    Price {
                        best_ask: Some(ask),
                        best_bid: Some(ask - 0.01),
                        best_ask_size: Some(100.0),
                        best_bid_size: Some(100.0),
                        ask_depth: vec![(ask, 100.0), (ask + 0.01, 50.0)],
                        bid_depth: vec![(ask - 0.01, 100.0)],
                        recent_trades: Default::default(),
                        recent_depth_changes: Default::default(),
                        tick_size: Some(0.01),
                        venue_timestamp_ms: None,
                        book_hash: Some(hash.into()),
                        snapshot_ready: true,
                        last_updated: std::time::Instant::now(),
                    },
                );
            }
        }
        let mut cfg = Config::from_env();
        cfg.ws_quote_max_age_ms = 1_000;
        let token_ids = vec!["token-a".to_string(), "token-b".to_string()];

        let snapshots = get_cached_depth_snapshots(&cache, &cfg, &token_ids)
            .await
            .expect("fresh cached depth snapshots");

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots["token-a"].asks[0], (0.40, 100.0));
        assert_eq!(snapshots["token-b"].book_hash.as_deref(), Some("hash-b"));
    }

    #[test]
    fn round_to_tick_respects_increment() {
        assert!((round_up_to_tick(0.333, 0.01) - 0.34).abs() < 1e-10);
        assert_eq!(format_price_for_tick(0.333, 0.01), "0.34");
        assert_eq!(format_price_for_tick(0.3332, 0.001), "0.334");
    }
}
