//! Gamma API client for Polymarket event/market discovery.
//!
//! Handles pagination, retries with exponential backoff, and data parsing
//! into typed model objects.

use reqwest::Client;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, error, warn};

use crate::config::Config;
use crate::fees;
use crate::models::{Event, EventLifecycle, GammaEvent, GammaMarket, Market};

const MAX_RETRY_BACKOFF_MS: u64 = 60_000;

fn retry_backoff_ms(base_ms: u64, attempt: u32) -> u64 {
    let exp = 2u64.saturating_pow(attempt.saturating_sub(1));
    base_ms.saturating_mul(exp).min(MAX_RETRY_BACKOFF_MS)
}

fn parse_jsonish_array(value: Option<&Value>) -> Option<Vec<Value>> {
    match value {
        Some(Value::Array(arr)) => Some(arr.clone()),
        Some(Value::String(s)) => serde_json::from_str::<Vec<Value>>(s).ok(),
        _ => None,
    }
}

fn value_to_nonempty_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => {
            let value = s.trim();
            (!value.is_empty()).then(|| value.to_string())
        }
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn value_to_datetime(value: Option<&Value>) -> Option<chrono::DateTime<chrono::Utc>> {
    let value = value?;
    match value {
        Value::String(raw) => parse_datetime_string(raw),
        Value::Number(number) => {
            let raw = number.as_f64()?;
            if !raw.is_finite() || raw < 0.0 {
                return None;
            }
            let millis = if raw < 10_000_000_000.0 {
                raw * 1000.0
            } else {
                raw
            };
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis.round() as i64)
        }
        _ => None,
    }
}

fn parse_datetime_string(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return date.and_hms_opt(0, 0, 0).map(|dt| dt.and_utc());
    }
    if let Ok(raw_number) = trimmed.parse::<f64>() {
        return value_to_datetime(Some(&Value::from(raw_number)));
    }
    None
}

fn clean_text(value: Option<&String>) -> Option<String> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn event_lifecycle_from_gamma_event(raw: &GammaEvent) -> EventLifecycle {
    EventLifecycle {
        end_date: value_to_datetime(raw.end_date.as_ref()),
        game_start_time: value_to_datetime(raw.game_start_time.as_ref()),
        resolution_source: clean_text(raw.resolution_source.as_ref()),
        description: clean_text(raw.description.as_ref()),
        rules: clean_text(raw.rules.as_ref()),
        uma_resolution_status: clean_text(raw.uma_resolution_status.as_ref()),
    }
}

fn event_lifecycle_from_gamma_market(raw: &GammaMarket) -> EventLifecycle {
    EventLifecycle {
        end_date: value_to_datetime(raw.end_date.as_ref()),
        game_start_time: value_to_datetime(raw.game_start_time.as_ref()),
        resolution_source: clean_text(raw.resolution_source.as_ref()),
        description: clean_text(raw.description.as_ref()),
        rules: clean_text(raw.rules.as_ref()),
        uma_resolution_status: clean_text(raw.uma_resolution_status.as_ref()),
    }
}

/// Make a GET request with exponential backoff retries.
async fn request_with_retry(
    client: &Client,
    url: &str,
    params: &[(&str, &str)],
    config: &Config,
) -> Option<serde_json::Value> {
    for attempt in 1..=config.max_retries {
        match client
            .get(url)
            .query(params)
            .timeout(Duration::from_secs(config.api_timeout_secs))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(data) => return Some(data),
                        Err(e) => {
                            warn!("Failed to parse JSON from Gamma API: {e}");
                            return None;
                        }
                    }
                } else if status.as_u16() == 429 {
                    let wait = retry_backoff_ms(config.retry_backoff_base_ms, attempt);
                    warn!("Rate limited (429). Waiting {wait}ms before retry {attempt}...");
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                } else if status.is_server_error() {
                    let wait = retry_backoff_ms(config.retry_backoff_base_ms, attempt);
                    warn!(
                        "Server error ({status}). Retry {attempt}/{}...",
                        config.max_retries
                    );
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                } else {
                    error!("HTTP error {status} (non-retryable) for {url}");
                    return None;
                }
            }
            Err(e) => {
                if e.is_timeout() {
                    let wait = retry_backoff_ms(config.retry_backoff_base_ms, attempt);
                    warn!("Request timeout. Retry {attempt}/{}...", config.max_retries);
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                } else if e.is_connect() {
                    let wait = retry_backoff_ms(config.retry_backoff_base_ms, attempt);
                    warn!(
                        "Connection error. Retry {attempt}/{}...",
                        config.max_retries
                    );
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                } else {
                    error!("Unexpected request error: {e}");
                    return None;
                }
            }
        }
    }

    error!("All {} retries exhausted for {url}", config.max_retries);
    None
}

/// Parse a raw Gamma market into our Market type.
fn normalize_fee_rate(value: Option<&Value>) -> Option<f64> {
    let raw = value.and_then(value_to_f64)?;
    if !raw.is_finite() || raw < 0.0 {
        return None;
    }
    if raw <= 1.0 {
        Some(raw)
    } else {
        Some(raw / 10_000.0)
    }
}

fn gamma_market_closed(raw: &GammaMarket) -> bool {
    raw.closed.unwrap_or(false)
        || matches!(raw.active, Some(false))
        || matches!(raw.archived, Some(true))
        || matches!(raw.accepting_orders, Some(false))
        || matches!(raw.enable_order_book, Some(false))
}

fn parse_market(raw: &GammaMarket) -> Option<Market> {
    // Only accept binary YES/NO markets, regardless of order in payload.
    let outcomes_raw = parse_jsonish_array(raw.outcomes.as_ref())?;
    if outcomes_raw.len() != 2 {
        return None;
    }
    let outcomes: Vec<String> = outcomes_raw
        .iter()
        .map(value_to_nonempty_string)
        .collect::<Option<_>>()?;
    let normalized: Vec<String> = outcomes
        .iter()
        .map(|o| o.trim().to_ascii_lowercase())
        .collect();
    let yes_idx = normalized.iter().position(|o| o == "yes")?;
    let no_idx = normalized.iter().position(|o| o == "no")?;

    let prices_raw = parse_jsonish_array(raw.outcome_prices.as_ref())?;
    if prices_raw.len() != 2 {
        return None;
    }
    let prices: Vec<f64> = prices_raw.iter().map(value_to_f64).collect::<Option<_>>()?;
    let yes_price = prices[yes_idx];
    let no_price = prices[no_idx];
    if !(0.0..=1.0).contains(&yes_price) || !(0.0..=1.0).contains(&no_price) {
        return None;
    }

    let token_ids_raw = parse_jsonish_array(raw.clob_token_ids.as_ref())?;
    if token_ids_raw.len() != 2 {
        return None;
    }
    let token_ids: Vec<String> = token_ids_raw
        .iter()
        .map(value_to_nonempty_string)
        .collect::<Option<_>>()?;
    let clob_token_id_yes = token_ids[yes_idx].clone();
    let clob_token_id_no = token_ids[no_idx].clone();
    if clob_token_id_yes == clob_token_id_no {
        return None;
    }

    let liquidity = raw.liquidity.as_ref().and_then(value_to_f64).unwrap_or(0.0);
    let taker_fee_rate = normalize_fee_rate(raw.taker_base_fee.as_ref());
    let maker_fee_rate = normalize_fee_rate(raw.maker_base_fee.as_ref());

    Some(Market {
        question: raw.question.clone().unwrap_or_else(|| "Unknown".into()),
        condition_id: raw.condition_id.clone().unwrap_or_default(),
        market_slug: raw.slug.clone().unwrap_or_default(),
        clob_token_id_yes,
        clob_token_id_no,
        gamma_yes_price: yes_price,
        gamma_no_price: no_price,
        clob_yes_ask: None,
        clob_yes_bid: None,
        clob_no_ask: None,
        clob_no_bid: None,
        clob_yes_ask_size: None,
        clob_yes_bid_size: None,
        clob_no_ask_size: None,
        clob_no_bid_size: None,
        fees_enabled: raw.fees_enabled,
        taker_fee_rate,
        maker_fee_rate,
        order_price_min_tick_size: raw
            .order_price_min_tick_size
            .as_ref()
            .and_then(value_to_f64),
        order_min_size: raw.order_min_size.as_ref().and_then(value_to_f64),
        clob_tick_size: None,
        clob_min_order_size: None,
        clob_taker_fee_bps: raw.taker_base_fee.as_ref().and_then(value_to_f64).map(|v| {
            if v <= 1.0 {
                (v * 10_000.0).round() as u32
            } else {
                v.round() as u32
            }
        }),
        clob_fee_rate: None,
        clob_fee_exponent: None,
        clob_neg_risk: None,
        clob_rfq_enabled: None,
        liquidity,
        closed: gamma_market_closed(raw),
    })
}

/// Parse a raw Gamma event into our Event type.
fn parse_event(raw: &GammaEvent) -> Option<Event> {
    let event_id = match &raw.id {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => return None,
    };

    let mut event = Event {
        event_id,
        title: raw.title.clone().unwrap_or_else(|| "Unknown".into()),
        slug: raw.slug.clone().unwrap_or_default(),
        category: raw.category.clone().unwrap_or_default(),
        enable_neg_risk: raw.enable_neg_risk.unwrap_or(false) || raw.neg_risk.unwrap_or(false),
        neg_risk: raw.neg_risk.unwrap_or(false),
        neg_risk_augmented: raw.neg_risk_augmented.unwrap_or(false),
        lifecycle: event_lifecycle_from_gamma_event(raw),
        markets: Vec::new(),
    };

    if let Some(raw_markets) = &raw.markets {
        let mut seen_market_keys = HashSet::new();
        for raw_market in raw_markets {
            event
                .lifecycle
                .merge_missing_or_earlier(&event_lifecycle_from_gamma_market(raw_market));
            let mut market = parse_market(raw_market)?;
            if market.clob_neg_risk.is_none() && (event.enable_neg_risk || event.neg_risk) {
                market.clob_neg_risk = Some(true);
            }
            let dedupe_key = market_dedupe_key(&market);
            // Duplicates make family cardinality ambiguous. Reject the event,
            // including byte-for-byte duplicate members, instead of guessing.
            if !seen_market_keys.insert(dedupe_key) {
                return None;
            }
            event.markets.push(market);
        }
    }

    Some(event)
}

fn market_dedupe_key(market: &Market) -> String {
    if !market.condition_id.is_empty() {
        format!("cond:{}", market.condition_id)
    } else if !market.market_slug.is_empty() {
        format!("slug:{}", market.market_slug)
    } else {
        format!("question:{}", market.question)
    }
}

fn markets_semantically_equal(left: &Market, right: &Market) -> bool {
    match (serde_json::to_value(left), serde_json::to_value(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn merge_events_in_place(target: &mut Event, incoming: Event) -> bool {
    let existing_markets: HashMap<String, &Market> = target
        .markets
        .iter()
        .map(|market| (market_dedupe_key(market), market))
        .collect();
    for market in &incoming.markets {
        if let Some(existing) = existing_markets.get(&market_dedupe_key(market)) {
            if !markets_semantically_equal(existing, market) {
                return false;
            }
        }
    }

    target.enable_neg_risk |= incoming.enable_neg_risk;
    target.neg_risk |= incoming.neg_risk;
    target.neg_risk_augmented |= incoming.neg_risk_augmented;
    target
        .lifecycle
        .merge_missing_or_earlier(&incoming.lifecycle);
    if target.title.trim().is_empty() && !incoming.title.trim().is_empty() {
        target.title = incoming.title.clone();
    }
    if target.slug.trim().is_empty() && !incoming.slug.trim().is_empty() {
        target.slug = incoming.slug.clone();
    }
    if target.category.trim().is_empty() && !incoming.category.trim().is_empty() {
        target.category = incoming.category.clone();
    }

    let mut seen_market_keys: HashSet<String> =
        target.markets.iter().map(market_dedupe_key).collect();

    for market in incoming.markets {
        let key = market_dedupe_key(&market);
        if seen_market_keys.insert(key) {
            target.markets.push(market);
        }
    }
    true
}

fn dedupe_events(events: Vec<Event>) -> Vec<Event> {
    let mut ordered_ids = Vec::new();
    let mut by_id = HashMap::new();
    let mut rejected_ids = HashSet::new();

    for event in events {
        let event_id = event.event_id.clone();
        if rejected_ids.contains(&event_id) {
            continue;
        }
        if let Some(existing) = by_id.get_mut(&event_id) {
            if !merge_events_in_place(existing, event) {
                by_id.remove(&event_id);
                rejected_ids.insert(event_id);
            }
        } else {
            ordered_ids.push(event_id.clone());
            by_id.insert(event_id, event);
        }
    }

    ordered_ids
        .into_iter()
        .filter_map(|event_id| by_id.remove(&event_id))
        .collect()
}

async fn fetch_active_events_offset(client: &Client, config: &Config) -> Vec<Event> {
    use futures::stream::{self, StreamExt};

    let limit = 100u64;
    let max_to_fetch = config.max_events_to_fetch;
    let num_pages = max_to_fetch.div_ceil(limit);
    let pages: Vec<u64> = (0..num_pages).map(|p| p * limit).collect();

    let all_data: Vec<Vec<Event>> = stream::iter(pages)
        .map(|offset| {
            let client = client.clone();
            let config = config.clone();
            let limit_str = limit.to_string();
            let offset_str = offset.to_string();
            async move {
                let params = [
                    ("closed", "false"),
                    ("active", "true"),
                    ("limit", &limit_str),
                    ("offset", &offset_str),
                ];
                let url = format!("{}/events", config.gamma_api_url);
                match request_with_retry(&client, &url, &params, &config).await {
                    Some(data) => {
                        let raw_events: Vec<GammaEvent> =
                            serde_json::from_value(data).unwrap_or_default();
                        raw_events
                            .into_iter()
                            .filter_map(|raw| parse_event(&raw))
                            .collect()
                    }
                    None => Vec::new(),
                }
            }
        })
        .buffer_unordered(4)
        .collect()
        .await;

    let events: Vec<Event> = dedupe_events(all_data.into_iter().flatten().collect());
    debug!(
        "Fetched {} active events from Gamma API (offset pagination fallback)",
        events.len()
    );
    events
}

/// Fetch all active events from the Polymarket Gamma API using keyset pagination.
pub async fn fetch_active_events(client: &Client, config: &Config) -> Vec<Event> {
    let mut events = Vec::new();
    let mut next_cursor: Option<String> = None;
    let limit = 100usize.min(config.max_events_to_fetch.max(1) as usize);

    loop {
        let limit_str = limit.to_string();
        let mut params = vec![
            ("closed".to_string(), "false".to_string()),
            ("active".to_string(), "true".to_string()),
            ("limit".to_string(), limit_str),
        ];
        if let Some(cursor) = &next_cursor {
            params.push(("after_cursor".to_string(), cursor.clone()));
        }
        let params_refs: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let url = format!("{}/events/keyset", config.gamma_api_url);

        let Some(data) = request_with_retry(client, &url, &params_refs, config).await else {
            if events.is_empty() {
                debug!("Keyset discovery failed before any events were fetched; falling back to offset pagination");
                return fetch_active_events_offset(client, config).await;
            }
            break;
        };

        let page_events = data
            .get("events")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let raw_events: Vec<GammaEvent> = serde_json::from_value(page_events).unwrap_or_default();

        if raw_events.is_empty() {
            break;
        }

        for raw in raw_events {
            if let Some(event) = parse_event(&raw) {
                events.push(event);
                if events.len() >= config.max_events_to_fetch as usize {
                    let events = dedupe_events(events);
                    debug!(
                        "Fetched {} active events from Gamma API (hit MAX_EVENTS_TO_FETCH)",
                        events.len()
                    );
                    return events;
                }
            }
        }

        next_cursor = data
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        if next_cursor.is_none() {
            break;
        }
    }

    if events.is_empty() {
        debug!("Keyset pagination returned no events; falling back to offset pagination");
        return fetch_active_events_offset(client, config).await;
    }

    let events = dedupe_events(events);
    debug!(
        "Fetched {} active events from Gamma API (keyset pagination)",
        events.len()
    );
    events
}

/// Fetch all active events and split them into neg-risk and general categories.
pub async fn fetch_discovery_data(client: &Client, config: &Config) -> DiscoveryData {
    let all = fetch_active_events(client, config).await;
    let neg_risk = filter_neg_risk(all.clone(), config);
    DiscoveryData { neg_risk, all }
}

#[derive(Debug, Clone)]
pub struct DiscoveryData {
    pub neg_risk: Vec<Event>,
    pub all: Vec<Event>,
}

pub fn filter_neg_risk(all_events: Vec<Event>, config: &Config) -> Vec<Event> {
    let total = all_events.len();
    let mut candidates = Vec::new();
    let mut skipped_non_neg_risk = 0usize;
    let mut skipped_augmented = 0usize;
    let mut skipped_too_few_markets = 0usize;
    let mut skipped_too_many_legs = 0usize;
    let mut skipped_incomplete_tradability = 0usize;

    for event in all_events {
        let clob_confirmed_neg_risk = !event.markets.is_empty()
            && event
                .markets
                .iter()
                .all(|market| market.clob_neg_risk == Some(true));
        let clob_verifiable_neg_risk = !event.markets.is_empty()
            && event.markets.iter().all(|market| {
                !market.clob_token_id_yes.trim().is_empty()
                    && !market.clob_token_id_no.trim().is_empty()
                    && market.clob_neg_risk != Some(false)
            });
        if !(event.enable_neg_risk
            || event.neg_risk
            || clob_confirmed_neg_risk
            || clob_verifiable_neg_risk)
        {
            skipped_non_neg_risk += 1;
            continue;
        }

        if event.neg_risk_augmented && !config.allow_augmented_neg_risk {
            skipped_augmented += 1;
            continue;
        }

        if event.markets.len() < 2 {
            skipped_too_few_markets += 1;
            continue;
        }

        if event.markets.len() > config.max_batchable_legs() {
            skipped_too_many_legs += 1;
            continue;
        }

        let fully_basket_tradable = event.markets.iter().all(|market| {
            !market.closed
                && market.liquidity >= config.min_liquidity_usd
                && fees::market_fee_curve_supported(market)
        });
        if !fully_basket_tradable {
            skipped_incomplete_tradability += 1;
            continue;
        }

        candidates.push(event);
    }

    debug!(
        "Neg-risk filtering breakdown: non-neg-risk={skipped_non_neg_risk}, augmented-blocked={skipped_augmented}, too-few-markets={skipped_too_few_markets}, too-many-legs={skipped_too_many_legs}, incomplete-tradability={skipped_incomplete_tradability}"
    );

    debug!(
        "Found {} fully basket-tradable neg-risk events (from {total} total)",
        candidates.len(),
    );
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market_with_payload(outcomes: Value, prices: Value, token_ids: Value) -> GammaMarket {
        GammaMarket {
            question: Some("Who wins?".into()),
            slug: Some("who-wins".into()),
            end_date: None,
            game_start_time: None,
            resolution_source: None,
            description: None,
            rules: None,
            uma_resolution_status: None,
            condition_id: Some("cond-1".into()),
            clob_token_ids: Some(token_ids),
            outcomes: Some(outcomes),
            outcome_prices: Some(prices),
            liquidity: Some(Value::String("2500.0".into())),
            closed: Some(false),
            active: Some(true),
            archived: Some(false),
            accepting_orders: Some(true),
            enable_order_book: Some(true),
            order_price_min_tick_size: Some(Value::from(0.01)),
            order_min_size: Some(Value::from(5.0)),
            taker_base_fee: Some(Value::from(0)),
            maker_base_fee: None,
            fees_enabled: Some(true),
        }
    }

    #[test]
    fn parse_market_handles_outcome_reordering() {
        let raw = market_with_payload(
            Value::String(r#"["No", "Yes"]"#.into()),
            Value::String(r#"["0.71", "0.29"]"#.into()),
            Value::String(r#"["no-token", "yes-token"]"#.into()),
        );

        let market = parse_market(&raw).expect("expected valid market");
        assert!((market.gamma_yes_price - 0.29).abs() < 1e-10);
        assert!((market.gamma_no_price - 0.71).abs() < 1e-10);
        assert_eq!(market.clob_token_id_yes, "yes-token");
        assert_eq!(market.clob_token_id_no, "no-token");
    }

    #[test]
    fn parse_market_accepts_array_payloads() {
        let raw = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.44), Value::from(0.56)]),
            Value::Array(vec![
                Value::String("yes-token".into()),
                Value::String("no-token".into()),
            ]),
        );

        let market = parse_market(&raw).expect("expected valid market");
        assert!((market.gamma_yes_price - 0.44).abs() < 1e-10);
        assert!((market.gamma_no_price - 0.56).abs() < 1e-10);
    }

    #[test]
    fn parse_market_marks_non_orderable_metadata_closed() {
        let mut raw = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.44), Value::from(0.56)]),
            Value::Array(vec![
                Value::String("yes-token".into()),
                Value::String("no-token".into()),
            ]),
        );
        raw.accepting_orders = Some(false);

        let market = parse_market(&raw).expect("expected valid market");
        assert!(market.closed);
    }

    #[tokio::test]
    async fn fetch_active_events_falls_back_to_offset_when_keyset_unavailable() {
        use httpmock::prelude::*;

        let server = MockServer::start_async().await;

        let _keyset = server
            .mock_async(|when, then| {
                when.method(GET).path("/events/keyset");
                then.status(500);
            })
            .await;

        let _offset = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/events")
                    .query_param("closed", "false")
                    .query_param("active", "true")
                    .query_param("limit", "100")
                    .query_param("offset", "0");
                then.status(200)
                    .header("content-type", "application/json")
                    .body(r#"[{"id":"1","title":"Event","slug":"event","category":"sports","enableNegRisk":true,"negRisk":true,"negRiskAugmented":false,"markets":[{"question":"A?","slug":"a","conditionId":"cond","clobTokenIds":["yes","no"],"outcomes":["Yes","No"],"outcomePrices":[0.4,0.6],"liquidity":"2500","closed":false}]}]"#);
            })
            .await;

        let client = Client::new();
        let mut cfg = Config::from_env();
        cfg.gamma_api_url = server.base_url();
        cfg.max_events_to_fetch = 1;
        let events = fetch_active_events(&client, &cfg).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "1");
    }

    #[test]
    fn parse_event_propagates_neg_risk_to_markets() {
        let raw = GammaEvent {
            id: Some(Value::String("evt-1".into())),
            title: Some("Event".into()),
            slug: Some("event".into()),
            category: Some("sports".into()),
            end_date: None,
            game_start_time: None,
            resolution_source: None,
            description: None,
            rules: None,
            uma_resolution_status: None,
            enable_neg_risk: Some(true),
            neg_risk: Some(true),
            neg_risk_augmented: Some(false),
            markets: Some(vec![market_with_payload(
                Value::Array(vec![
                    Value::String("Yes".into()),
                    Value::String("No".into()),
                ]),
                Value::Array(vec![Value::from(0.4), Value::from(0.6)]),
                Value::Array(vec![
                    Value::String("yes-token".into()),
                    Value::String("no-token".into()),
                ]),
            )]),
        };

        let event = parse_event(&raw).expect("event parses");
        assert_eq!(event.markets.len(), 1);
        assert_eq!(event.markets[0].clob_neg_risk, Some(true));
    }

    #[test]
    fn parse_event_leaves_false_gamma_neg_risk_open_for_clob_confirmation() {
        let raw = GammaEvent {
            id: Some(Value::String("evt-1".into())),
            title: Some("Event".into()),
            slug: Some("event".into()),
            category: Some("sports".into()),
            end_date: None,
            game_start_time: None,
            resolution_source: None,
            description: None,
            rules: None,
            uma_resolution_status: None,
            enable_neg_risk: Some(false),
            neg_risk: Some(false),
            neg_risk_augmented: Some(false),
            markets: Some(vec![market_with_payload(
                Value::Array(vec![
                    Value::String("Yes".into()),
                    Value::String("No".into()),
                ]),
                Value::Array(vec![Value::from(0.4), Value::from(0.6)]),
                Value::Array(vec![
                    Value::String("yes-token".into()),
                    Value::String("no-token".into()),
                ]),
            )]),
        };

        let event = parse_event(&raw).expect("event parses");

        assert!(!event.enable_neg_risk);
        assert!(!event.neg_risk);
        assert_eq!(event.markets[0].clob_neg_risk, None);
    }

    #[test]
    fn filter_neg_risk_keeps_clob_verifiable_family_when_gamma_flags_false() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        let mut market_a = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.4), Value::from(0.6)]),
            Value::Array(vec![
                Value::String("yes-token-a".into()),
                Value::String("no-token-a".into()),
            ]),
        );
        market_a.condition_id = Some("cond-a".into());
        let mut market_b = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.3), Value::from(0.7)]),
            Value::Array(vec![
                Value::String("yes-token-b".into()),
                Value::String("no-token-b".into()),
            ]),
        );
        market_b.condition_id = Some("cond-b".into());
        let raw = GammaEvent {
            id: Some(Value::String("evt-1".into())),
            title: Some("Event".into()),
            slug: Some("event".into()),
            category: Some("sports".into()),
            end_date: None,
            game_start_time: None,
            resolution_source: None,
            description: None,
            rules: None,
            uma_resolution_status: None,
            enable_neg_risk: Some(false),
            neg_risk: Some(false),
            neg_risk_augmented: Some(false),
            markets: Some(vec![market_a, market_b]),
        };
        let event = parse_event(&raw).expect("event parses");

        let filtered = filter_neg_risk(vec![event], &cfg);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].event_id, "evt-1");
    }

    #[test]
    fn parse_event_captures_lifecycle_metadata() {
        let mut market = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.4), Value::from(0.6)]),
            Value::Array(vec![
                Value::String("yes-token".into()),
                Value::String("no-token".into()),
            ]),
        );
        market.end_date = Some(Value::String("2026-06-25T12:00:00Z".into()));

        let raw = GammaEvent {
            id: Some(Value::String("evt-life".into())),
            title: Some("Lifecycle Event".into()),
            slug: Some("lifecycle-event".into()),
            category: Some("sports".into()),
            end_date: Some(Value::String("2026-06-26T12:00:00Z".into())),
            game_start_time: Some(Value::String("2026-06-24T18:30:00Z".into())),
            resolution_source: Some("official league stats".into()),
            description: Some("settles from official source".into()),
            rules: Some("primary source controls".into()),
            uma_resolution_status: Some("unresolved".into()),
            enable_neg_risk: Some(true),
            neg_risk: Some(true),
            neg_risk_augmented: Some(false),
            markets: Some(vec![market]),
        };

        let event = parse_event(&raw).expect("event parses");

        assert_eq!(
            event
                .lifecycle
                .end_date
                .expect("market fallback can tighten event end")
                .to_rfc3339(),
            "2026-06-25T12:00:00+00:00"
        );
        assert_eq!(
            event
                .lifecycle
                .game_start_time
                .expect("game start parsed")
                .to_rfc3339(),
            "2026-06-24T18:30:00+00:00"
        );
        assert_eq!(
            event.lifecycle.resolution_source.as_deref(),
            Some("official league stats")
        );
        assert_eq!(
            event.lifecycle.rules.as_deref(),
            Some("primary source controls")
        );
        assert_eq!(
            event.lifecycle.uma_resolution_status.as_deref(),
            Some("unresolved")
        );
    }

    #[test]
    fn parse_event_rejects_duplicate_markets() {
        let raw = GammaEvent {
            id: Some(Value::String("evt-dup".into())),
            title: Some("Event".into()),
            slug: Some("event".into()),
            category: Some("sports".into()),
            end_date: None,
            game_start_time: None,
            resolution_source: None,
            description: None,
            rules: None,
            uma_resolution_status: None,
            enable_neg_risk: Some(true),
            neg_risk: Some(true),
            neg_risk_augmented: Some(false),
            markets: Some(vec![
                market_with_payload(
                    Value::Array(vec![
                        Value::String("Yes".into()),
                        Value::String("No".into()),
                    ]),
                    Value::Array(vec![Value::from(0.4), Value::from(0.6)]),
                    Value::Array(vec![
                        Value::String("yes-token".into()),
                        Value::String("no-token".into()),
                    ]),
                ),
                market_with_payload(
                    Value::Array(vec![
                        Value::String("Yes".into()),
                        Value::String("No".into()),
                    ]),
                    Value::Array(vec![Value::from(0.4), Value::from(0.6)]),
                    Value::Array(vec![
                        Value::String("yes-token".into()),
                        Value::String("no-token".into()),
                    ]),
                ),
            ]),
        };
        assert!(parse_event(&raw).is_none());
    }

    #[test]
    fn dedupe_events_merges_consistent_pages_and_rejects_conflicts() {
        let first = Event {
            event_id: "evt-1".into(),
            title: "Event".into(),
            slug: "event".into(),
            category: "sports".into(),
            enable_neg_risk: true,
            neg_risk: true,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets: vec![Market {
                question: "Will Alice win?".into(),
                condition_id: "cond-a".into(),
                market_slug: "alice-win".into(),
                clob_token_id_yes: "a-yes".into(),
                clob_token_id_no: "a-no".into(),
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
                clob_neg_risk: Some(true),
                clob_rfq_enabled: None,
                liquidity: 1_000.0,
                closed: false,
            }],
        };
        let conflict_base = first.clone();
        let exact_duplicate = dedupe_events(vec![first.clone(), first.clone()]);
        assert_eq!(exact_duplicate.len(), 1);
        assert_eq!(exact_duplicate[0].markets.len(), 1);

        let mut second = first.clone();
        second.markets = vec![Market {
            question: "Will Bob win?".into(),
            condition_id: "cond-b".into(),
            market_slug: "bob-win".into(),
            clob_token_id_yes: "b-yes".into(),
            clob_token_id_no: "b-no".into(),
            gamma_yes_price: 0.3,
            gamma_no_price: 0.7,
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
            clob_neg_risk: Some(true),
            clob_rfq_enabled: None,
            liquidity: 1_000.0,
            closed: false,
        }];

        let deduped = dedupe_events(vec![first, second]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].markets.len(), 2);

        let mut conflicting = conflict_base.clone();
        conflicting.markets[0].clob_token_id_yes = "conflicting-token".into();
        assert!(dedupe_events(vec![conflict_base, conflicting]).is_empty());
    }

    #[test]
    fn parse_market_rejects_non_binary_outcomes() {
        let raw = market_with_payload(
            Value::String(r#"["Yes", "No", "Other"]"#.into()),
            Value::String(r#"["0.4", "0.4", "0.2"]"#.into()),
            Value::String(r#"["a", "b", "c"]"#.into()),
        );
        assert!(parse_market(&raw).is_none());
    }

    #[test]
    fn parse_market_rejects_null_without_compacting_positions() {
        let null_token = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.2), Value::from(0.8)]),
            Value::Array(vec![Value::Null, Value::String("no-token".into())]),
        );
        assert!(parse_market(&null_token).is_none());

        let null_price = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::Null, Value::from(0.8)]),
            Value::Array(vec![
                Value::String("yes-token".into()),
                Value::String("no-token".into()),
            ]),
        );
        assert!(parse_market(&null_price).is_none());
    }

    #[test]
    fn parse_market_requires_exact_binary_array_lengths_and_distinct_tokens() {
        let extra_price = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.2), Value::from(0.8), Value::from(0.0)]),
            Value::Array(vec![
                Value::String("yes-token".into()),
                Value::String("no-token".into()),
            ]),
        );
        assert!(parse_market(&extra_price).is_none());

        let equal_tokens = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.2), Value::from(0.8)]),
            Value::Array(vec![
                Value::String("same-token".into()),
                Value::String("same-token".into()),
            ]),
        );
        assert!(parse_market(&equal_tokens).is_none());

        let empty_token = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.2), Value::from(0.8)]),
            Value::Array(vec![
                Value::String("yes-token".into()),
                Value::String("  ".into()),
            ]),
        );
        assert!(parse_market(&empty_token).is_none());
    }

    #[test]
    fn parse_event_rejects_incomplete_neg_risk_family() {
        let mut valid_a = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.2), Value::from(0.8)]),
            Value::Array(vec![
                Value::String("a-yes".into()),
                Value::String("a-no".into()),
            ]),
        );
        valid_a.condition_id = Some("cond-a".into());
        let mut valid_b = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::from(0.3), Value::from(0.7)]),
            Value::Array(vec![
                Value::String("b-yes".into()),
                Value::String("b-no".into()),
            ]),
        );
        valid_b.condition_id = Some("cond-b".into());
        let mut malformed = market_with_payload(
            Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ]),
            Value::Array(vec![Value::Null, Value::from(0.4)]),
            Value::Array(vec![
                Value::String("c-yes".into()),
                Value::String("c-no".into()),
            ]),
        );
        malformed.condition_id = Some("cond-c".into());
        let raw = GammaEvent {
            id: Some(Value::String("evt-incomplete".into())),
            title: Some("Incomplete family".into()),
            slug: Some("incomplete-family".into()),
            category: Some("sports".into()),
            end_date: None,
            game_start_time: None,
            resolution_source: None,
            description: None,
            rules: None,
            uma_resolution_status: None,
            enable_neg_risk: Some(true),
            neg_risk: Some(true),
            neg_risk_augmented: Some(false),
            markets: Some(vec![valid_a, valid_b, malformed]),
        };

        assert!(parse_event(&raw).is_none());
    }
}

use crate::models::{RankedFamily, RankedMarketInstance};
use regex::Regex;

fn normalized_tokens(text: &str) -> Vec<String> {
    text.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .map(|token| token.to_string())
        .collect()
}

fn dedupe_tokens_preserve_order(tokens: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for token in tokens {
        if seen.insert(token.clone()) {
            out.push(token);
        }
    }
    out
}

fn ordinal_word_value(token: &str) -> Option<u32> {
    match token {
        "winner" | "win" | "wins" | "winning" | "champion" | "gold" => Some(1),
        "first" | "1st" => Some(1),
        "second" | "2nd" | "silver" | "runnerup" | "vicechampion" => Some(2),
        "third" | "3rd" | "bronze" => Some(3),
        "fourth" | "4th" => Some(4),
        "fifth" | "5th" => Some(5),
        "sixth" | "6th" => Some(6),
        "seventh" | "7th" => Some(7),
        "eighth" | "8th" => Some(8),
        "ninth" | "9th" => Some(9),
        "tenth" | "10th" => Some(10),
        "eleventh" | "11th" => Some(11),
        "twelfth" | "12th" => Some(12),
        "thirteenth" | "13th" => Some(13),
        "fourteenth" | "14th" => Some(14),
        "fifteenth" | "15th" => Some(15),
        "sixteenth" | "16th" => Some(16),
        "seventeenth" | "17th" => Some(17),
        "eighteenth" | "18th" => Some(18),
        "nineteenth" | "19th" => Some(19),
        "twentieth" | "20th" => Some(20),
        _ => None,
    }
}

fn parse_ordinal_token(token: &str) -> Option<u32> {
    if let Some(rank) = ordinal_word_value(token) {
        return Some(rank);
    }

    for suffix in ["st", "nd", "rd", "th"] {
        if let Some(num) = token.strip_suffix(suffix) {
            if let Ok(rank) = num.parse::<u32>() {
                if rank >= 1 {
                    return Some(rank);
                }
            }
        }
    }

    for prefix in ["p", "pos", "position", "place", "no", "num"] {
        if let Some(num) = token.strip_prefix(prefix) {
            if let Ok(rank) = num.parse::<u32>() {
                if rank >= 1 {
                    return Some(rank);
                }
            }
        }
    }

    None
}

fn is_generic_rank_one_token(token: &str) -> bool {
    matches!(token, "win" | "wins" | "winning" | "winner" | "champion")
}

fn is_rank_noun(token: &str) -> bool {
    matches!(
        token,
        "place" | "position" | "spot" | "rank" | "ranking" | "placing"
    )
}

fn parse_rank_from_tokens(tokens: &[String]) -> Option<u32> {
    let mut idx = 0;
    let mut fallback_rank = None;
    while idx < tokens.len() {
        let token = tokens[idx].as_str();
        if token == "top"
            && tokens
                .get(idx + 1)
                .map(String::as_str)
                .is_some_and(is_rank_noun)
        {
            return Some(1);
        }
        if token == "vice" && tokens.get(idx + 1).map(String::as_str) == Some("champion") {
            return Some(2);
        }
        if token == "pole" && tokens.get(idx + 1).map(String::as_str) == Some("position") {
            return Some(1);
        }
        if token == "runner" && tokens.get(idx + 1).map(String::as_str) == Some("up") {
            return Some(2);
        }
        if matches!(token, "number" | "no" | "num") {
            if let Some(next_rank) = tokens
                .get(idx + 1)
                .and_then(|next| parse_ordinal_token(next))
            {
                return Some(next_rank);
            }
        }
        if let Some(rank) = parse_ordinal_token(token) {
            if rank == 1 && is_generic_rank_one_token(token) {
                fallback_rank = Some(rank);
            } else {
                return Some(rank);
            }
        }
        if is_rank_noun(token) {
            if let Some(next_rank) = tokens
                .get(idx + 1)
                .and_then(|next| parse_ordinal_token(next))
            {
                return Some(next_rank);
            }
        }
        idx += 1;
    }
    fallback_rank
}

fn replace_rank_tokens_with_placeholder(tokens: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut idx = 0;
    while idx < tokens.len() {
        let token = tokens[idx].as_str();
        if token == "top"
            && tokens
                .get(idx + 1)
                .map(String::as_str)
                .is_some_and(is_rank_noun)
        {
            out.push("<rank>".to_string());
            idx += 2;
            continue;
        }
        if token == "vice" && tokens.get(idx + 1).map(String::as_str) == Some("champion") {
            out.push("<rank>".to_string());
            idx += 2;
            continue;
        }
        if token == "pole" && tokens.get(idx + 1).map(String::as_str) == Some("position") {
            out.push("<rank>".to_string());
            idx += 2;
            continue;
        }
        if token == "runner" && tokens.get(idx + 1).map(String::as_str) == Some("up") {
            out.push("<rank>".to_string());
            idx += 2;
            continue;
        }
        if matches!(token, "number" | "no" | "num")
            && idx + 1 < tokens.len()
            && parse_ordinal_token(&tokens[idx + 1]).is_some()
        {
            out.push("<rank>".to_string());
            idx += 2;
            continue;
        }
        if parse_ordinal_token(token).is_some() {
            out.push("<rank>".to_string());
            idx += 1;
            if idx < tokens.len() && is_rank_noun(&tokens[idx]) {
                idx += 1;
            }
            continue;
        }
        if is_rank_noun(token)
            && idx + 1 < tokens.len()
            && parse_ordinal_token(&tokens[idx + 1]).is_some()
        {
            out.push("<rank>".to_string());
            idx += 2;
            continue;
        }
        out.push(tokens[idx].clone());
        idx += 1;
    }
    out
}

fn filter_family_tokens(tokens: Vec<String>) -> Vec<String> {
    tokens
        .into_iter()
        .filter(|token| {
            !matches!(
                token.as_str(),
                "who"
                    | "will"
                    | "which"
                    | "what"
                    | "finish"
                    | "finishes"
                    | "finished"
                    | "come"
                    | "comes"
                    | "coming"
                    | "take"
                    | "takes"
                    | "get"
                    | "gets"
                    | "be"
                    | "to"
                    | "the"
                    | "of"
                    | "for"
                    | "in"
                    | "on"
                    | "at"
                    | "a"
                    | "an"
                    | "place"
                    | "position"
                    | "spot"
                    | "rank"
                    | "ranking"
                    | "placing"
                    | "medal"
                    | "overall"
                    | "result"
                    | "results"
                    | "standing"
                    | "standings"
                    | "table"
                    | "classification"
                    | "championship"
                    | "championships"
                    | "qualifying"
                    | "qualifier"
                    | "qualifiers"
                    | "medalist"
                    | "podium"
                    | "podiums"
                    | "pole"
                    | "vice"
            )
        })
        .collect()
}

fn family_context_tokens(event: &Event) -> Option<Vec<String>> {
    let title_tokens = normalized_tokens(&event.title);
    let slug_tokens = normalized_tokens(&event.slug);
    let title_rank = parse_rank_from_tokens(&title_tokens);
    let slug_rank = parse_rank_from_tokens(&slug_tokens);
    if title_rank.is_none() && slug_rank.is_none() {
        return None;
    }

    let mut combined = Vec::new();
    combined.extend(title_tokens);
    combined.extend(slug_tokens);
    let mut normalized = filter_family_tokens(replace_rank_tokens_with_placeholder(&combined));
    normalized = dedupe_tokens_preserve_order(normalized);
    if normalized.is_empty() {
        normalized = vec!["<rank>".to_string()];
    }
    Some(normalized)
}

fn normalize_ranked_family_key(event: &Event) -> Option<String> {
    family_context_tokens(event).map(|mut tokens| {
        tokens.sort_unstable();
        tokens.dedup();
        tokens.join(" ")
    })
}

fn display_family_title(title: &str) -> String {
    let rank_regex = Regex::new(
        r"(?i)\b(?:\d{1,2}(?:st|nd|rd|th)|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|eighteenth|nineteenth|twentieth|winner|runner[- ]?up|vice\s+champion|top\s+(?:spot|position)|pole\s+position|gold|silver|bronze|p\d{1,2}|pos\d{1,2}|position\d{1,2}|(?:number|no|num)\s+\d{1,2})\b(?:\s+(?:place|position|spot|rank|placing))?",
    )
    .expect("static rank regex should compile");
    rank_regex.replace_all(title, "<RANK>").to_string()
}

fn is_contestant_stopword(token: &str) -> bool {
    matches!(
        token,
        "will"
            | "who"
            | "which"
            | "what"
            | "finish"
            | "finishes"
            | "finished"
            | "come"
            | "comes"
            | "coming"
            | "take"
            | "takes"
            | "get"
            | "gets"
            | "win"
            | "wins"
            | "winning"
            | "be"
            | "to"
            | "the"
            | "of"
            | "for"
            | "in"
            | "on"
            | "at"
            | "a"
            | "an"
            | "place"
            | "position"
            | "spot"
            | "rank"
            | "ranking"
            | "placing"
            | "end"
            | "up"
            | "runner"
            | "candidate"
            | "contestant"
            | "driver"
            | "team"
            | "medal"
            | "overall"
            | "result"
            | "results"
            | "standing"
            | "standings"
            | "table"
            | "classification"
            | "championship"
            | "championships"
            | "qualifying"
            | "qualifier"
            | "qualifiers"
            | "medalist"
            | "podium"
            | "podiums"
            | "pole"
            | "vice"
    )
}

fn looks_generic_contestant_name(tokens: &[String]) -> bool {
    if tokens.is_empty() {
        return true;
    }
    matches!(
        tokens.join(" ").as_str(),
        "other" | "someone else" | "somebody else" | "none" | "none of the above" | "unlisted"
    )
}

fn display_name_from_tokens(tokens: &[String]) -> String {
    tokens
        .iter()
        .map(|token| {
            let mut chars = token.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_candidate_tokens(tokens: &[String], family_context: &HashSet<String>) -> Vec<String> {
    let mut filtered = Vec::new();
    let mut idx = 0;
    while idx < tokens.len() {
        let token = tokens[idx].as_str();
        if token == "top"
            && tokens
                .get(idx + 1)
                .map(String::as_str)
                .is_some_and(is_rank_noun)
        {
            idx += 2;
            continue;
        }
        if token == "vice" && tokens.get(idx + 1).map(String::as_str) == Some("champion") {
            idx += 2;
            continue;
        }
        if token == "pole" && tokens.get(idx + 1).map(String::as_str) == Some("position") {
            idx += 2;
            continue;
        }
        if token == "runner" && tokens.get(idx + 1).map(String::as_str) == Some("up") {
            idx += 2;
            continue;
        }
        if matches!(token, "number" | "no" | "num")
            && idx + 1 < tokens.len()
            && parse_ordinal_token(&tokens[idx + 1]).is_some()
        {
            idx += 2;
            continue;
        }
        if parse_ordinal_token(token).is_some() {
            idx += 1;
            if idx < tokens.len() && is_rank_noun(&tokens[idx]) {
                idx += 1;
            }
            continue;
        }
        if is_rank_noun(token)
            && idx + 1 < tokens.len()
            && parse_ordinal_token(&tokens[idx + 1]).is_some()
        {
            idx += 2;
            continue;
        }
        if token.chars().all(|ch| ch.is_ascii_digit())
            || is_contestant_stopword(token)
            || family_context.contains(token)
        {
            idx += 1;
            continue;
        }
        filtered.push(tokens[idx].clone());
        idx += 1;
    }
    filtered
}

fn normalize_ranked_contestant_name(
    question: &str,
    market_slug: &str,
    family_context_tokens: &[String],
) -> Option<(String, String)> {
    let family_context: HashSet<String> = family_context_tokens.iter().cloned().collect();
    let mut sources = vec![question.to_string()];
    if !market_slug.trim().is_empty() {
        sources.push(market_slug.to_string());
    }

    let mut fallback_candidate: Option<Vec<String>> = None;

    for source in sources {
        let tokens = normalized_tokens(&source);
        if tokens.is_empty() {
            continue;
        }

        let filtered = extract_candidate_tokens(&tokens, &family_context);
        if !looks_generic_contestant_name(&filtered) && !filtered.is_empty() {
            let key = filtered.join(" ");
            let display = display_name_from_tokens(&filtered);
            return Some((key, display));
        }

        if fallback_candidate.is_none() {
            let unscoped = extract_candidate_tokens(&tokens, &HashSet::new());
            if !looks_generic_contestant_name(&unscoped) && !unscoped.is_empty() {
                fallback_candidate = Some(unscoped);
            }
        }
    }

    fallback_candidate.map(|tokens| {
        let key = tokens.join(" ");
        let display = display_name_from_tokens(&tokens);
        (key, display)
    })
}

/// Group events into cohesive ranked families, associating contestants across mutually exclusive ranks.
pub fn group_into_ranked_families(events: &[Event]) -> Vec<RankedFamily> {
    let mut families: HashMap<String, Vec<(&Event, u32)>> = HashMap::new();

    for event in events {
        let title_tokens = normalized_tokens(&event.title);
        let slug_tokens = normalized_tokens(&event.slug);
        let rank_val =
            parse_rank_from_tokens(&title_tokens).or_else(|| parse_rank_from_tokens(&slug_tokens));
        let Some(rank_val) = rank_val else {
            continue;
        };
        let Some(family_key) = normalize_ranked_family_key(event) else {
            continue;
        };
        families
            .entry(family_key)
            .or_default()
            .push((event, rank_val));
    }

    let mut ranked_families = Vec::new();

    for (family_key, event_group) in families {
        if event_group.len() < 2 {
            continue;
        }

        let mut temp_instances = Vec::new();
        let mut contestant_map: HashMap<String, usize> = HashMap::new();
        let mut contestant_display = Vec::new();
        let mut rank_set: HashSet<u32> = HashSet::new();
        let mut seen_markets: HashSet<(String, u32)> = HashSet::new();

        for (event, rank_val) in &event_group {
            let family_context = family_context_tokens(event).unwrap_or_default();
            rank_set.insert(*rank_val);

            for market in &event.markets {
                let Some((contestant_key, contestant_name)) = normalize_ranked_contestant_name(
                    &market.question,
                    &market.market_slug,
                    &family_context,
                ) else {
                    continue;
                };

                let contestant_id = if let Some(&idx) = contestant_map.get(&contestant_key) {
                    idx
                } else {
                    let idx = contestant_map.len();
                    contestant_map.insert(contestant_key.clone(), idx);
                    contestant_display.push(contestant_name);
                    idx
                };

                let market_key = if !market.condition_id.is_empty() {
                    market.condition_id.clone()
                } else if !market.market_slug.is_empty() {
                    market.market_slug.clone()
                } else {
                    market.question.clone()
                };
                if !seen_markets.insert((market_key, *rank_val)) {
                    continue;
                }

                temp_instances.push((*rank_val, contestant_id, market.clone()));
            }
        }

        let mut ranks: Vec<u32> = rank_set.into_iter().collect();
        ranks.sort_unstable();
        if ranks.len() < 2 || contestant_display.len() < 2 {
            continue;
        }

        let mut instance_list: Vec<RankedMarketInstance> = temp_instances
            .into_iter()
            .filter_map(|(rank_val, contestant_id, market)| {
                let rank_idx = ranks.iter().position(|candidate| *candidate == rank_val)?;
                Some(RankedMarketInstance {
                    contestant_id,
                    rank_idx,
                    market,
                })
            })
            .collect();
        instance_list.sort_by_key(|instance| (instance.rank_idx, instance.contestant_id));

        let mut cat_counts = HashMap::new();
        for (event, _) in &event_group {
            *cat_counts.entry(event.category.clone()).or_insert(0usize) += 1;
        }
        let category = cat_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(cat, _)| cat)
            .unwrap_or_default();
        let title = display_family_title(event_group[0].0.title.as_str());

        ranked_families.push(RankedFamily {
            id: family_key.clone(),
            title,
            category,
            markets: instance_list,
            contestants: contestant_display,
            ranks,
        });
    }

    ranked_families.sort_by(|left, right| left.id.cmp(&right.id));
    ranked_families
}

#[cfg(test)]
mod ranked_family_tests {
    use super::*;
    use crate::models::Market;

    fn market(question: &str) -> Market {
        Market {
            question: question.into(),
            condition_id: question.to_ascii_lowercase().replace(' ', "-"),
            market_slug: question.to_ascii_lowercase().replace(' ', "-"),
            clob_token_id_yes: "y".into(),
            clob_token_id_no: "n".into(),
            gamma_yes_price: 0.4,
            gamma_no_price: 0.6,
            clob_yes_ask: Some(0.4),
            clob_yes_bid: Some(0.39),
            clob_no_ask: Some(0.6),
            clob_no_bid: Some(0.59),
            clob_yes_ask_size: Some(100.0),
            clob_yes_bid_size: None,
            clob_no_ask_size: Some(100.0),
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
            clob_neg_risk: Some(true),
            clob_rfq_enabled: None,
            liquidity: 1000.0,
            closed: false,
        }
    }

    fn event(event_id: &str, title: &str, slug: &str, markets: Vec<Market>) -> Event {
        Event {
            event_id: event_id.into(),
            title: title.into(),
            slug: slug.into(),
            category: "sports".into(),
            enable_neg_risk: true,
            neg_risk: true,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets,
        }
    }

    #[test]
    fn normalize_ranked_name_rejects_empty_names() {
        assert!(
            normalize_ranked_contestant_name("Will 1st place?", "will-1st-place", &[]).is_none()
        );
        assert_eq!(
            normalize_ranked_contestant_name("Will Alice finish 1st?", "alice-finish-1st", &[])
                .map(|(_, display)| display),
            Some("Alice".to_string())
        );
    }

    #[test]
    fn ranked_family_grouping_skips_empty_contestants() {
        let events = vec![
            event(
                "1",
                "Who finishes 1st place?",
                "who-finishes-1st-place",
                vec![market("Will Alice finish 1st?"), market("Will 1st place?")],
            ),
            event(
                "2",
                "Who finishes 2nd place?",
                "who-finishes-2nd-place",
                vec![market("Will Alice finish 2nd?"), market("Will 2nd place?")],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 0);
    }

    #[test]
    fn ranked_family_grouping_supports_ordinals_beyond_five() {
        let events = vec![
            event(
                "1",
                "Who finishes 6th place in F1?",
                "who-finishes-6th-place-in-f1",
                vec![
                    market("Will Lewis Hamilton finish 6th?"),
                    market("Will Charles Leclerc finish 6th?"),
                ],
            ),
            event(
                "2",
                "Who finishes 7th place in F1?",
                "who-finishes-7th-place-in-f1",
                vec![
                    market("Will Lewis Hamilton finish 7th?"),
                    market("Will Charles Leclerc finish 7th?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![6, 7]);
        assert_eq!(families[0].contestants.len(), 2);
    }

    #[test]
    fn ranked_family_grouping_supports_podium_aliases() {
        let events = vec![
            event(
                "1",
                "Who wins gold medal in sprint?",
                "who-wins-gold-medal-in-sprint",
                vec![
                    market("Will Noah Lyles win gold?"),
                    market("Will Fred Kerley win gold?"),
                ],
            ),
            event(
                "2",
                "Who wins silver medal in sprint?",
                "who-wins-silver-medal-in-sprint",
                vec![
                    market("Will Noah Lyles win silver?"),
                    market("Will Fred Kerley win silver?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![1, 2]);
    }

    #[test]
    fn ranked_family_grouping_removes_event_context_from_contestant_names() {
        let events = vec![
            event(
                "1",
                "Who finishes 1st in Premier League?",
                "who-finishes-1st-in-premier-league",
                vec![
                    market("Will Arsenal finish 1st in Premier League?"),
                    market("Will Liverpool finish 1st in Premier League?"),
                ],
            ),
            event(
                "2",
                "Who finishes 2nd in Premier League?",
                "who-finishes-2nd-in-premier-league",
                vec![
                    market("Will Arsenal finish 2nd in Premier League?"),
                    market("Will Liverpool finish 2nd in Premier League?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(
            families[0].contestants,
            vec!["Arsenal".to_string(), "Liverpool".to_string()]
        );
    }

    #[test]
    fn ranked_family_grouping_supports_motorsport_p_notation() {
        let events = vec![
            event(
                "1",
                "Who finishes P1 in qualifying?",
                "who-finishes-p1-in-qualifying",
                vec![
                    market("Will Max Verstappen finish P1?"),
                    market("Will Lando Norris finish P1?"),
                ],
            ),
            event(
                "2",
                "Who finishes P2 in qualifying?",
                "who-finishes-p2-in-qualifying",
                vec![
                    market("Will Max Verstappen finish P2?"),
                    market("Will Lando Norris finish P2?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![1, 2]);
    }

    #[test]
    fn ranked_family_grouping_supports_eleventh_place_wording() {
        let events = vec![
            event(
                "1",
                "Who finishes 11th place in the race?",
                "who-finishes-11th-place-in-the-race",
                vec![
                    market("Will Alice finish 11th?"),
                    market("Will Bob finish 11th?"),
                ],
            ),
            event(
                "2",
                "Who finishes 12th place in the race?",
                "who-finishes-12th-place-in-the-race",
                vec![
                    market("Will Alice finish 12th?"),
                    market("Will Bob finish 12th?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![11, 12]);
    }

    #[test]
    fn ranked_family_grouping_uses_slug_when_title_wording_differs() {
        let events = vec![
            event(
                "1",
                "Who comes in 1st in the race?",
                "race-finish-1st",
                vec![market("Will Alice come 1st?"), market("Will Bob come 1st?")],
            ),
            event(
                "2",
                "Race runner-up",
                "race-finish-2nd",
                vec![
                    market("Will Alice be runner-up?"),
                    market("Will Bob be runner-up?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(
            families[0].contestants,
            vec!["Alice".to_string(), "Bob".to_string()]
        );
    }

    #[test]
    fn ranked_family_grouping_supports_vice_champion_wording() {
        let events = vec![
            event(
                "1",
                "Who will be champion?",
                "who-will-be-champion",
                vec![
                    market("Will Alice be champion?"),
                    market("Will Bob be champion?"),
                ],
            ),
            event(
                "2",
                "Who will be vice champion?",
                "who-will-be-vice-champion",
                vec![
                    market("Will Alice be vice champion?"),
                    market("Will Bob be vice champion?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![1, 2]);
    }

    #[test]
    fn ranked_family_grouping_supports_top_spot_wording() {
        let events = vec![
            event(
                "1",
                "Who takes the top spot in qualifying?",
                "who-takes-the-top-spot-in-qualifying",
                vec![
                    market("Will Alice take the top spot?"),
                    market("Will Bob take the top spot?"),
                ],
            ),
            event(
                "2",
                "Who finishes 2nd in qualifying?",
                "who-finishes-2nd-in-qualifying",
                vec![
                    market("Will Alice finish 2nd?"),
                    market("Will Bob finish 2nd?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![1, 2]);
    }

    #[test]
    fn ranked_family_grouping_supports_pole_position_wording() {
        let events = vec![
            event(
                "1",
                "Who gets pole position?",
                "who-gets-pole-position",
                vec![
                    market("Will Alice get pole position?"),
                    market("Will Bob get pole position?"),
                ],
            ),
            event(
                "2",
                "Who finishes P2?",
                "who-finishes-p2",
                vec![
                    market("Will Alice finish P2?"),
                    market("Will Bob finish P2?"),
                ],
            ),
        ];
        let families = group_into_ranked_families(&events);
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].ranks, vec![1, 2]);
    }
}
