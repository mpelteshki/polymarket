//! Public prediction-market discovery adapters.
//!
//! External venues are scan-only. They normalize public prices into the existing
//! Market model, but execution remains Polymarket-only.

use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, warn};

use crate::config::Config;
use crate::gamma_client;
use crate::models::{Event, Market, EXTERNAL_TOKEN_PREFIX};

const SOURCES: [&str; 7] = [
    "kalshi",
    "manifold",
    "predictit",
    "limitless",
    "seer",
    "sxbet",
    "betdex",
];
const MAX_SOURCE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub async fn fetch_discovery_data(client: &Client, config: &Config) -> gamma_client::DiscoveryData {
    let mut data = if config.market_source_enabled("polymarket") {
        gamma_client::fetch_discovery_data(client, config).await
    } else {
        gamma_client::DiscoveryData {
            neg_risk: Vec::new(),
            all: Vec::new(),
        }
    };

    let external = fetch_external_events(client, config).await;
    if !external.is_empty() {
        data.all.extend(external);
        data.neg_risk = gamma_client::filter_neg_risk(data.all.clone(), config);
    }

    data
}

async fn fetch_external_events(client: &Client, config: &Config) -> Vec<Event> {
    let (kalshi, manifold, predictit, limitless, seer, sxbet, betdex) = tokio::join!(
        async {
            if config.market_source_enabled("kalshi") {
                fetch_kalshi_events(client, config).await
            } else {
                Vec::new()
            }
        },
        async {
            if config.market_source_enabled("manifold") {
                fetch_manifold_events(client, config).await
            } else {
                Vec::new()
            }
        },
        async {
            if config.market_source_enabled("predictit") {
                fetch_predictit_events(client, config).await
            } else {
                Vec::new()
            }
        },
        async {
            if config.market_source_enabled("limitless") {
                fetch_limitless_events(client, config).await
            } else {
                Vec::new()
            }
        },
        async {
            if config.market_source_enabled("seer") {
                fetch_seer_events(client, config).await
            } else {
                Vec::new()
            }
        },
        async {
            if config.market_source_enabled("sxbet") {
                fetch_sxbet_events(client, config).await
            } else {
                Vec::new()
            }
        },
        async {
            if config.market_source_enabled("betdex") {
                fetch_betdex_events(client, config).await
            } else {
                Vec::new()
            }
        }
    );

    let events = interleave_source_events(
        vec![kalshi, manifold, predictit, limitless, seer, sxbet, betdex],
        config.max_events_to_fetch as usize,
    );

    if !events.is_empty() {
        debug!(
            "Fetched {} scan-only markets from external sources [{}]",
            events.len(),
            SOURCES
                .iter()
                .filter(|source| config.market_source_enabled(source))
                .copied()
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    events
}

fn interleave_source_events(sources: Vec<Vec<Event>>, limit: usize) -> Vec<Event> {
    let mut events = Vec::new();
    if limit == 0 {
        return events;
    }
    let mut index = 0;
    loop {
        let mut pushed = false;
        for source in &sources {
            if let Some(event) = source.get(index) {
                events.push(event.clone());
                pushed = true;
                if events.len() == limit {
                    return events;
                }
            }
        }
        if !pushed {
            return events;
        }
        index += 1;
    }
}

async fn request_json(
    client: &Client,
    url: &str,
    params: &[(&str, String)],
    config: &Config,
) -> Option<Value> {
    request_json_with_auth(client, url, params, config, None).await
}

async fn request_json_with_auth(
    client: &Client,
    url: &str,
    params: &[(&str, String)],
    config: &Config,
    auth: Option<&str>,
) -> Option<Value> {
    let attempts = config.max_retries.max(1);
    for attempt in 1..=attempts {
        let mut request = client
            .get(url)
            .query(params)
            .timeout(Duration::from_secs(config.api_timeout_secs));
        if let Some(auth) = auth.filter(|value| !value.trim().is_empty()) {
            request = request.header("authorization", auth);
        }

        match request.send().await {
            Ok(resp) if resp.status().is_success() => {
                return limited_json_response(resp, url).await;
            }
            Ok(resp) => {
                if attempt == attempts
                    || !(resp.status().as_u16() == 429 || resp.status().is_server_error())
                {
                    warn!(
                        "Prediction-market source request failed: {} {}",
                        resp.status(),
                        url
                    );
                    return None;
                }
            }
            Err(err) => {
                if attempt == attempts || !(err.is_timeout() || err.is_connect()) {
                    warn!(
                        "Prediction-market source request error for {}: {}",
                        url, err
                    );
                    return None;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(config.retry_backoff_base_ms)).await;
    }
    None
}

async fn request_json_post(
    client: &Client,
    url: &str,
    body: Value,
    config: &Config,
) -> Option<Value> {
    let attempts = config.max_retries.max(1);
    for attempt in 1..=attempts {
        let request = client
            .post(url)
            .json(&body)
            .timeout(Duration::from_secs(config.api_timeout_secs));

        match request.send().await {
            Ok(resp) if resp.status().is_success() => {
                return limited_json_response(resp, url).await;
            }
            Ok(resp) => {
                if attempt == attempts
                    || !(resp.status().as_u16() == 429 || resp.status().is_server_error())
                {
                    warn!(
                        "Prediction-market source request failed: {} {}",
                        resp.status(),
                        url
                    );
                    return None;
                }
            }
            Err(err) => {
                if attempt == attempts || !(err.is_timeout() || err.is_connect()) {
                    warn!(
                        "Prediction-market source request error for {}: {}",
                        url, err
                    );
                    return None;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(config.retry_backoff_base_ms)).await;
    }
    None
}

async fn limited_json_response(resp: reqwest::Response, url: &str) -> Option<Value> {
    if resp
        .content_length()
        .is_some_and(|length| length > MAX_SOURCE_RESPONSE_BYTES as u64)
    {
        warn!(
            "Prediction-market source response exceeded {} bytes: {}",
            MAX_SOURCE_RESPONSE_BYTES, url
        );
        return None;
    }
    let mut body = Vec::new();
    let mut chunks = resp.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(err) => {
                warn!("Prediction-market source body failed for {}: {}", url, err);
                return None;
            }
        };
        if !append_limited_chunk(&mut body, &chunk, MAX_SOURCE_RESPONSE_BYTES) {
            warn!(
                "Prediction-market source response exceeded {} bytes: {}",
                MAX_SOURCE_RESPONSE_BYTES, url
            );
            return None;
        }
    }
    match serde_json::from_slice(&body) {
        Ok(value) => Some(value),
        Err(err) => {
            warn!("Prediction-market source JSON failed for {}: {}", url, err);
            None
        }
    }
}

fn append_limited_chunk(body: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
    let Some(next_len) = body.len().checked_add(chunk.len()) else {
        return false;
    };
    if next_len > limit {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn field_f64(raw: &Value, key: &str) -> Option<f64> {
    raw.get(key).and_then(as_f64)
}

fn field_str(raw: &Value, key: &str) -> Option<String> {
    raw.get(key).and_then(Value::as_str).map(str::to_string)
}

fn field_bool(raw: &Value, key: &str) -> Option<bool> {
    raw.get(key).and_then(Value::as_bool)
}

fn price(raw: Option<f64>) -> Option<f64> {
    let value = raw?;
    let normalized = if value > 1.0 { value / 100.0 } else { value };
    if normalized.is_finite() && (0.0..=1.0).contains(&normalized) {
        Some(normalized)
    } else {
        None
    }
}

fn scan_liquidity(config: &Config, reported: Option<f64>) -> Option<f64> {
    reported.filter(|v| v.is_finite() && *v >= config.min_liquidity_usd)
}

fn decimal_scaled(raw: &Value, scale: f64) -> Option<f64> {
    as_f64(raw).map(|value| value / scale)
}

fn update_best_ask(best_price: &mut Option<f64>, best_size: &mut f64, price: f64, size: f64) {
    if !(0.0..=1.0).contains(&price) || price <= 0.0 || size <= 0.0 {
        return;
    }
    match *best_price {
        Some(current) if (price - current).abs() <= 1e-9 => *best_size += size,
        Some(current) if price < current => {
            *best_price = Some(price);
            *best_size = size;
        }
        None => {
            *best_price = Some(price);
            *best_size = size;
        }
        _ => {}
    }
}

struct QuoteMarket<'a> {
    venue: &'a str,
    id: String,
    question: String,
    yes_ask: f64,
    no_ask: f64,
    yes_bid: Option<f64>,
    no_bid: Option<f64>,
    yes_size: Option<f64>,
    no_size: Option<f64>,
    liquidity: f64,
    closed: bool,
}

fn quoted_market(input: QuoteMarket<'_>) -> Market {
    Market {
        question: input.question,
        condition_id: format!("{}{}:{}", EXTERNAL_TOKEN_PREFIX, input.venue, input.id),
        market_slug: input.id.clone(),
        clob_token_id_yes: format!("{}{}:{}:yes", EXTERNAL_TOKEN_PREFIX, input.venue, input.id),
        clob_token_id_no: format!("{}{}:{}:no", EXTERNAL_TOKEN_PREFIX, input.venue, input.id),
        gamma_yes_price: input.yes_ask,
        gamma_no_price: input.no_ask,
        clob_yes_ask: Some(input.yes_ask),
        clob_yes_bid: input.yes_bid,
        clob_no_ask: Some(input.no_ask),
        clob_no_bid: input.no_bid,
        clob_yes_ask_size: input.yes_size,
        clob_yes_bid_size: None,
        clob_no_ask_size: input.no_size,
        clob_no_bid_size: None,
        fees_enabled: None,
        taker_fee_rate: None,
        maker_fee_rate: None,
        clob_taker_fee_bps: None,
        clob_fee_rate: None,
        clob_fee_exponent: None,
        order_price_min_tick_size: Some(0.01),
        order_min_size: Some(1.0),
        clob_tick_size: Some(0.01),
        clob_min_order_size: Some(1.0),
        clob_neg_risk: Some(false),
        clob_rfq_enabled: None,
        liquidity: input.liquidity,
        closed: input.closed,
    }
}

fn event_for_market(
    venue: &str,
    id: &str,
    title: String,
    category: String,
    market: Market,
) -> Event {
    Event {
        event_id: format!("{}{}:{}", EXTERNAL_TOKEN_PREFIX, venue, id),
        title,
        slug: id.to_string(),
        category,
        enable_neg_risk: false,
        neg_risk: false,
        neg_risk_augmented: false,
        lifecycle: Default::default(),
        markets: vec![market],
    }
}

async fn fetch_kalshi_events(client: &Client, config: &Config) -> Vec<Event> {
    let params = [
        ("status", "open".to_string()),
        ("limit", config.max_events_to_fetch.min(1000).to_string()),
        ("mve_filter", "exclude".to_string()),
    ];
    let Some(data) = request_json(
        client,
        &format!("{}/markets", config.kalshi_api_url),
        &params,
        config,
    )
    .await
    else {
        return Vec::new();
    };
    parse_kalshi_events(&data, config)
}

fn parse_kalshi_events(data: &Value, config: &Config) -> Vec<Event> {
    data.get("markets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|raw| {
            let id = field_str(raw, "ticker")?;
            let yes = price(field_f64(raw, "yes_ask_dollars"))?;
            let no = price(field_f64(raw, "no_ask_dollars"))?;
            let title = field_str(raw, "title").unwrap_or_else(|| id.clone());
            let liquidity = scan_liquidity(
                config,
                field_f64(raw, "liquidity_dollars").or_else(|| field_f64(raw, "open_interest_fp")),
            )?;
            let market = quoted_market(QuoteMarket {
                venue: "kalshi",
                id: id.clone(),
                question: field_str(raw, "yes_sub_title")
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| title.clone()),
                yes_ask: yes,
                no_ask: no,
                yes_bid: price(field_f64(raw, "yes_bid_dollars")),
                no_bid: price(field_f64(raw, "no_bid_dollars")),
                yes_size: field_f64(raw, "yes_ask_size_fp"),
                no_size: None,
                liquidity,
                closed: !matches!(field_str(raw, "status").as_deref(), Some("active" | "open")),
            });
            Some(event_for_market(
                "kalshi",
                &id,
                title,
                "kalshi".into(),
                market,
            ))
        })
        .collect()
}

async fn fetch_manifold_events(client: &Client, config: &Config) -> Vec<Event> {
    let params = [
        ("limit", config.max_events_to_fetch.min(1000).to_string()),
        ("sort", "updated-time".to_string()),
    ];
    let Some(data) = request_json(
        client,
        &format!("{}/markets", config.manifold_api_url),
        &params,
        config,
    )
    .await
    else {
        return Vec::new();
    };
    parse_manifold_events(&data, config)
}

fn parse_manifold_events(data: &Value, config: &Config) -> Vec<Event> {
    data.as_array()
        .into_iter()
        .flatten()
        .filter_map(|raw| {
            if raw
                .get("isResolved")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return None;
            }
            if raw.get("outcomeType").and_then(Value::as_str) != Some("BINARY") {
                return None;
            }
            let id = field_str(raw, "id")?;
            let prob = price(field_f64(raw, "probability"))?;
            let title = field_str(raw, "question").unwrap_or_else(|| id.clone());
            let liquidity = scan_liquidity(config, field_f64(raw, "totalLiquidity"))?;
            let market = quoted_market(QuoteMarket {
                venue: "manifold",
                id: id.clone(),
                question: title.clone(),
                yes_ask: prob,
                no_ask: 1.0 - prob,
                yes_bid: None,
                no_bid: None,
                yes_size: None,
                no_size: None,
                liquidity,
                closed: false,
            });
            Some(event_for_market(
                "manifold",
                &id,
                title,
                "manifold".into(),
                market,
            ))
        })
        .collect()
}

async fn fetch_predictit_events(client: &Client, config: &Config) -> Vec<Event> {
    let Some(data) = request_json(client, &config.predictit_api_url, &[], config).await else {
        return Vec::new();
    };
    parse_predictit_events(&data, config)
}

fn parse_predictit_events(data: &Value, config: &Config) -> Vec<Event> {
    data.get("markets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(config.max_events_to_fetch as usize)
        .filter_map(|raw| {
            let id = field_str(raw, "id").or_else(|| raw.get("id").map(Value::to_string))?;
            let title = field_str(raw, "name").unwrap_or_else(|| id.clone());
            let mut markets = Vec::new();
            for contract in raw
                .get("contracts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let contract_id = field_str(contract, "id")
                    .or_else(|| contract.get("id").map(Value::to_string))?;
                let yes = price(field_f64(contract, "bestBuyYesCost"))?;
                let no = price(field_f64(contract, "bestBuyNoCost"))?;
                let liquidity = scan_liquidity(config, None)?;
                markets.push(quoted_market(QuoteMarket {
                    venue: "predictit",
                    id: contract_id,
                    question: field_str(contract, "name").unwrap_or_else(|| title.clone()),
                    yes_ask: yes,
                    no_ask: no,
                    yes_bid: price(field_f64(contract, "bestSellYesCost")),
                    no_bid: price(field_f64(contract, "bestSellNoCost")),
                    yes_size: None,
                    no_size: None,
                    liquidity,
                    closed: contract.get("status").and_then(Value::as_str) != Some("Open"),
                }));
            }
            if markets.is_empty() {
                return None;
            }
            Some(Event {
                event_id: format!("{}predictit:{}", EXTERNAL_TOKEN_PREFIX, id),
                title,
                slug: id,
                category: "predictit".into(),
                enable_neg_risk: false,
                neg_risk: false,
                neg_risk_augmented: false,
                lifecycle: Default::default(),
                markets,
            })
        })
        .collect()
}

async fn fetch_limitless_events(client: &Client, config: &Config) -> Vec<Event> {
    if config.max_events_to_fetch == 0 {
        return Vec::new();
    }

    // Limitless caps active-market pages at 25. Fetch the CLOB index first,
    // then probe executable books only for explicit Polymarket mirrors.
    const PAGE_SIZE: usize = 25;
    const MAX_PAGES: usize = 40;
    let first_params = [
        ("limit", PAGE_SIZE.to_string()),
        ("page", "1".to_string()),
        ("sortBy", "high_value".to_string()),
        ("tradeType", "clob".to_string()),
    ];
    let Some(first_page) = request_json(
        client,
        &format!("{}/markets/active", config.limitless_api_url),
        &first_params,
        config,
    )
    .await
    else {
        return Vec::new();
    };

    let Some(total) = first_page
        .get("totalMarketsCount")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
    else {
        warn!("Limitless active-market response missing totalMarketsCount; refusing partial index");
        return Vec::new();
    };
    let page_count = total.div_ceil(PAGE_SIZE).max(1);
    if page_count > MAX_PAGES {
        warn!(
            "Limitless active-market index requires {} pages, above safety cap {}; refusing partial index",
            page_count, MAX_PAGES
        );
        return Vec::new();
    }
    let mut pages = vec![(1_usize, first_page)];
    let remaining: Vec<Option<(usize, Value)>> = stream::iter(2..=page_count)
        .map(|page| async move {
            let params = [
                ("limit", PAGE_SIZE.to_string()),
                ("page", page.to_string()),
                ("sortBy", "high_value".to_string()),
                ("tradeType", "clob".to_string()),
            ];
            request_json(
                client,
                &format!("{}/markets/active", config.limitless_api_url),
                &params,
                config,
            )
            .await
            .map(|value| (page, value))
        })
        .buffer_unordered(4)
        .collect()
        .await;
    if remaining.iter().any(Option::is_none) {
        warn!("Limitless active-market pagination failed; refusing partial index");
        return Vec::new();
    }
    let mut remaining: Vec<(usize, Value)> = remaining.into_iter().flatten().collect();
    pages.append(&mut remaining);
    pages.sort_by_key(|(page, _)| *page);

    let candidates = collect_limitless_candidates(
        pages.iter().map(|(_, page)| page),
        config.max_events_to_fetch as usize,
    );
    let concurrency = config.clob_max_concurrency.clamp(1, 8);
    stream::iter(candidates)
        .map(|market| async move {
            let slug = field_str(&market, "slug")?;
            let orderbook = request_json(
                client,
                &format!("{}/markets/{slug}/orderbook", config.limitless_api_url),
                &[],
                config,
            )
            .await?;
            parse_limitless_market(&market, &orderbook, config)
        })
        .buffer_unordered(concurrency)
        .filter_map(|event| async move { event })
        .collect()
        .await
}

fn collect_limitless_candidates<'a>(
    pages: impl IntoIterator<Item = &'a Value>,
    limit: usize,
) -> Vec<Value> {
    if limit == 0 {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for page in pages {
        for raw in page
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(slug) = field_str(raw, "slug") else {
                continue;
            };
            if limitless_candidate(raw) && seen.insert(slug) {
                candidates.push(raw.clone());
                if candidates.len() == limit {
                    return candidates;
                }
            }
        }
    }
    candidates
}

fn limitless_candidate(raw: &Value) -> bool {
    field_str(raw, "status").as_deref() == Some("FUNDED")
        && !field_bool(raw, "expired").unwrap_or(true)
        && field_str(raw, "marketType").as_deref() == Some("single")
        && field_str(raw, "tradeType").as_deref() == Some("clob")
        && raw
            .get("metadata")
            .and_then(|metadata| metadata.get("isPolyArbitrage"))
            .and_then(Value::as_bool)
            == Some(true)
        && field_str(raw, "slug").is_some()
        && field_str(raw, "conditionId").is_some()
        && raw
            .get("tokens")
            .and_then(|tokens| tokens.get("yes"))
            .and_then(Value::as_str)
            .is_some_and(|token| !token.trim().is_empty())
        && raw
            .get("tokens")
            .and_then(|tokens| tokens.get("no"))
            .and_then(Value::as_str)
            .is_some_and(|token| !token.trim().is_empty())
}

fn limitless_best_level(rows: &Value, best_ask: bool) -> Option<(f64, f64)> {
    let mut best_price: Option<f64> = None;
    let mut shares = 0.0;
    for row in rows.as_array()? {
        let level_price = field_f64(row, "price")?;
        let level_shares = field_f64(row, "size")? / 1e6;
        if !level_price.is_finite()
            || !(0.0..1.0).contains(&level_price)
            || !level_shares.is_finite()
            || level_shares <= 0.0
        {
            continue;
        }
        match best_price {
            Some(current) if (level_price - current).abs() <= 1e-12 => {
                shares += level_shares;
            }
            Some(current)
                if (best_ask && level_price < current) || (!best_ask && level_price > current) =>
            {
                best_price = Some(level_price);
                shares = level_shares;
            }
            None => {
                best_price = Some(level_price);
                shares = level_shares;
            }
            _ => {}
        }
    }
    Some((best_price?, shares))
}

fn parse_limitless_market(raw: &Value, orderbook: &Value, config: &Config) -> Option<Event> {
    if !limitless_candidate(raw) {
        return None;
    }
    let id = field_str(raw, "slug")?;
    let condition_id = field_str(raw, "conditionId")?;
    let yes_token = raw.get("tokens")?.get("yes")?.as_str()?.to_string();
    let no_token = raw.get("tokens")?.get("no")?.as_str()?.to_string();
    if field_str(orderbook, "tokenId").as_deref() != Some(yes_token.as_str()) {
        return None;
    }
    let (yes_ask, yes_ask_size) = limitless_best_level(orderbook.get("asks")?, true)?;
    let (yes_bid, yes_bid_size) = limitless_best_level(orderbook.get("bids")?, false)?;
    if yes_bid >= yes_ask {
        return None;
    }
    let no_ask = 1.0 - yes_bid;
    let no_bid = 1.0 - yes_ask;
    let no_ask_size = yes_bid_size;
    let liquidity = scan_liquidity(
        config,
        Some((yes_ask * yes_ask_size).min(no_ask * no_ask_size)),
    )?;
    let title = field_str(raw, "title").unwrap_or_else(|| id.clone());
    let mut market = quoted_market(QuoteMarket {
        venue: "limitless",
        id: id.clone(),
        question: title.clone(),
        yes_ask,
        no_ask,
        yes_bid: Some(yes_bid),
        no_bid: Some(no_bid),
        yes_size: Some(yes_ask_size),
        no_size: Some(no_ask_size),
        liquidity,
        closed: false,
    });
    market.condition_id = format!("{}limitless:{condition_id}", EXTERNAL_TOKEN_PREFIX);
    market.clob_token_id_yes = format!("{}limitless:{yes_token}", EXTERNAL_TOKEN_PREFIX);
    market.clob_token_id_no = format!("{}limitless:{no_token}", EXTERNAL_TOKEN_PREFIX);
    market.order_price_min_tick_size = None;
    market.clob_tick_size = None;
    let min_size = raw
        .get("settings")
        .and_then(|settings| settings.get("minSize"))
        .and_then(as_f64)
        .map(|size| size / 1e6)
        .filter(|size| size.is_finite() && *size > 0.0);
    market.order_min_size = min_size;
    market.clob_min_order_size = min_size;
    Some(event_for_market(
        "limitless",
        &id,
        title,
        "limitless-poly-mirror".into(),
        market,
    ))
}

async fn fetch_seer_events(client: &Client, config: &Config) -> Vec<Event> {
    // ponytail: one public Seer page keeps discovery bounded; add paging if it becomes primary.
    let body = serde_json::json!({
        "chainsList": ["100"],
        "limit": config.max_events_to_fetch.min(1000),
        "page": 1
    });
    let Some(data) = request_json_post(
        client,
        &format!("{}/markets-search", config.seer_api_url),
        body,
        config,
    )
    .await
    else {
        return Vec::new();
    };
    parse_seer_events(&data, config)
}

fn parse_seer_events(data: &Value, config: &Config) -> Vec<Event> {
    data.get("markets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|raw| {
            if field_bool(raw, "payoutReported").unwrap_or(false)
                || field_bool(raw, "hasAnswers").unwrap_or(false)
                || !field_bool(raw, "hasLiquidity").unwrap_or(false)
            {
                return None;
            }
            let outcomes = raw.get("outcomes").and_then(Value::as_array)?;
            if outcomes.len() != 2
                || outcomes.first().and_then(Value::as_str) != Some("Yes")
                || outcomes.get(1).and_then(Value::as_str) != Some("No")
            {
                return None;
            }
            let odds = raw.get("odds").and_then(Value::as_array)?;
            let yes = price(odds.first().and_then(as_f64))?;
            let no = price(odds.get(1).and_then(as_f64))?;
            let id = field_str(raw, "id")?;
            let title = field_str(raw, "marketName").unwrap_or_else(|| id.clone());
            let category = raw
                .get("categories")
                .and_then(Value::as_array)
                .and_then(|arr| arr.first())
                .and_then(Value::as_str)
                .unwrap_or("seer")
                .to_ascii_lowercase();
            let liquidity = scan_liquidity(
                config,
                field_f64(raw, "liquidityUSD").or_else(|| field_f64(raw, "openInterestUSD")),
            )?;
            let market = quoted_market(QuoteMarket {
                venue: "seer",
                id: id.clone(),
                question: title.clone(),
                yes_ask: yes,
                no_ask: no,
                yes_bid: None,
                no_bid: None,
                yes_size: None,
                no_size: None,
                liquidity,
                closed: false,
            });
            Some(event_for_market("seer", &id, title, category, market))
        })
        .collect()
}

async fn fetch_sxbet_events(client: &Client, config: &Config) -> Vec<Event> {
    // ponytail: cap per-market order probes; add SX batching/paging if it becomes primary.
    let limit = config.max_events_to_fetch.min(50);
    let params = [
        ("pageSize", limit.to_string()),
        ("onlyMainLine", "true".to_string()),
    ];
    let Some(data) = request_json(
        client,
        &format!("{}/markets/active", config.sxbet_api_url),
        &params,
        config,
    )
    .await
    else {
        return Vec::new();
    };

    let markets: Vec<Value> = data
        .get("data")
        .and_then(|data| data.get("markets"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    let concurrency = config.clob_max_concurrency.clamp(1, 8);
    stream::iter(markets)
        .map(|market| async move {
            let market_hash = field_str(&market, "marketHash")?;
            let order_params = [
                ("marketHashes", market_hash),
                ("baseToken", config.sxbet_base_token.clone()),
            ];
            let orders = request_json(
                client,
                &format!("{}/orders", config.sxbet_api_url),
                &order_params,
                config,
            )
            .await?;
            parse_sxbet_market(&market, &orders, config)
        })
        .buffer_unordered(concurrency)
        .filter_map(|event| async move { event })
        .collect()
        .await
}

fn parse_sxbet_market(market: &Value, orders: &Value, config: &Config) -> Option<Event> {
    if field_str(market, "status").as_deref() != Some("ACTIVE") {
        return None;
    }
    let id = field_str(market, "marketHash")?;
    let outcome_one = field_str(market, "outcomeOneName").unwrap_or_else(|| "Outcome one".into());
    let outcome_two = field_str(market, "outcomeTwoName").unwrap_or_else(|| "Outcome two".into());
    let league = field_str(market, "leagueLabel")
        .or_else(|| field_str(market, "sportLabel"))
        .unwrap_or_else(|| "SX Bet".into());
    let title = format!("{league}: {outcome_one} vs {outcome_two}");
    let mut yes_ask = None;
    let mut no_ask = None;
    let mut yes_size = 0.0;
    let mut no_size = 0.0;

    for order in orders.get("data").and_then(Value::as_array)?.iter() {
        if field_str(order, "orderStatus").as_deref() != Some("ACTIVE") {
            continue;
        }
        let Some(maker_prob) = order
            .get("percentageOdds")
            .and_then(|value| decimal_scaled(value, 1e20))
        else {
            continue;
        };
        if !(0.0..1.0).contains(&maker_prob) {
            continue;
        }
        let maker_remaining = order
            .get("totalBetSize")
            .and_then(|value| decimal_scaled(value, 1e6))
            .unwrap_or(0.0)
            - order
                .get("fillAmount")
                .and_then(|value| decimal_scaled(value, 1e6))
                .unwrap_or(0.0)
            - order
                .get("pendingFillAmount")
                .and_then(|value| decimal_scaled(value, 1e6))
                .unwrap_or(0.0);
        let taker_price = 1.0 - maker_prob;
        let taker_usd = maker_remaining * taker_price / maker_prob;
        let taker_shares = taker_usd / taker_price;
        if field_bool(order, "isMakerBettingOutcomeOne").unwrap_or(false) {
            update_best_ask(&mut no_ask, &mut no_size, taker_price, taker_shares);
        } else {
            update_best_ask(&mut yes_ask, &mut yes_size, taker_price, taker_shares);
        }
    }

    let yes = yes_ask?;
    let no = no_ask?;
    let liquidity = scan_liquidity(config, Some((yes * yes_size).min(no * no_size)))?;
    let market = quoted_market(QuoteMarket {
        venue: "sxbet",
        id: id.clone(),
        question: title.clone(),
        yes_ask: yes,
        no_ask: no,
        yes_bid: None,
        no_bid: None,
        yes_size: Some(yes_size),
        no_size: Some(no_size),
        liquidity,
        closed: false,
    });
    Some(event_for_market(
        "sxbet",
        &id,
        title,
        "sxbet".into(),
        market,
    ))
}

async fn fetch_betdex_events(client: &Client, config: &Config) -> Vec<Event> {
    let auth = config.betdex_auth_token.trim();
    if auth.is_empty() {
        debug!("BetDEX source enabled but BETDEX_AUTH_TOKEN is empty; skipping");
        return Vec::new();
    }
    let params = [
        ("statuses", "Open".to_string()),
        ("published", "true".to_string()),
        ("size", config.max_events_to_fetch.min(100).to_string()),
    ];
    let Some(data) = request_json_with_auth(
        client,
        &format!("{}/markets", config.betdex_api_url),
        &params,
        config,
        Some(auth),
    )
    .await
    else {
        return Vec::new();
    };

    let markets: Vec<Value> = data
        .get("markets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .cloned()
        .collect();
    let concurrency = config.clob_max_concurrency.clamp(1, 8);
    stream::iter(markets)
        .map(|market| {
            let discovery = data.clone();
            async move {
                let id = field_str(&market, "id")?;
                let prices = request_json_with_auth(
                    client,
                    &format!("{}/markets/{}/prices-v2", config.betdex_api_url, id),
                    &[],
                    config,
                    Some(auth),
                )
                .await?;
                parse_betdex_market(&market, &discovery, &prices, config)
            }
        })
        .buffer_unordered(concurrency)
        .filter_map(|event| async move { event })
        .collect()
        .await
}

fn ref_ids(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(|item| item.get("_ids"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn title_for_id(data: &Value, collection: &str, id: &str) -> Option<String> {
    data.get(collection)
        .and_then(Value::as_array)?
        .iter()
        .find(|item| field_str(item, "id").as_deref() == Some(id))
        .and_then(|item| field_str(item, "title").or_else(|| field_str(item, "name")))
}

fn parse_betdex_market(
    market: &Value,
    discovery: &Value,
    prices: &Value,
    config: &Config,
) -> Option<Event> {
    if !field_bool(market, "published").unwrap_or(false)
        || field_bool(market, "suspended").unwrap_or(false)
        || market
            .get("settledAt")
            .is_some_and(|value| !value.is_null())
    {
        return None;
    }
    let id = field_str(market, "id")?;
    let outcome_ids = ref_ids(market, "marketOutcomes");
    if outcome_ids.len() != 2 {
        return None;
    }
    let first = &outcome_ids[0];
    let second = &outcome_ids[1];
    let title = field_str(market, "name").unwrap_or_else(|| id.clone());
    let first_title =
        title_for_id(discovery, "marketOutcomes", first).unwrap_or_else(|| "Outcome one".into());
    let second_title =
        title_for_id(discovery, "marketOutcomes", second).unwrap_or_else(|| "Outcome two".into());
    let price_row = prices
        .get("prices")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|row| field_str(row, "marketId").as_deref() == Some(id.as_str()))?;
    let prices = price_row.get("prices").and_then(Value::as_array)?;
    let best_price_for = |outcome_id: &str| {
        prices
            .iter()
            .filter(|row| field_str(row, "outcomeId").as_deref() == Some(outcome_id))
            .filter_map(|row| price(field_f64(row, "price")))
            .min_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal))
    };
    let yes = best_price_for(first)?;
    let no = best_price_for(second)?;
    let liquidity = scan_liquidity(config, field_f64(price_row, "liquidity"))?;
    let market = quoted_market(QuoteMarket {
        venue: "betdex",
        id: id.clone(),
        question: format!("{title}: {first_title} / {second_title}"),
        yes_ask: yes,
        no_ask: no,
        yes_bid: None,
        no_bid: None,
        yes_size: None,
        no_size: None,
        liquidity,
        closed: false,
    });
    Some(event_for_market(
        "betdex",
        &id,
        title,
        "betdex".into(),
        market,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(id: &str) -> Event {
        Event {
            event_id: id.to_string(),
            title: id.to_string(),
            slug: id.to_string(),
            category: "test".into(),
            enable_neg_risk: false,
            neg_risk: false,
            neg_risk_augmented: false,
            lifecycle: Default::default(),
            markets: Vec::new(),
        }
    }

    fn limitless_market_fixture() -> Value {
        serde_json::json!({
            "id": 230815,
            "slug": "world-cup-goalkeeper-to-score",
            "title": "World Cup: Goalkeeper to Score?",
            "status": "FUNDED",
            "expired": false,
            "marketType": "single",
            "tradeType": "clob",
            "conditionId": "0xcondition",
            "tokens": {"yes": "111", "no": "222"},
            "settings": {"minSize": "300000000"},
            "metadata": {"isPolyArbitrage": true}
        })
    }

    fn limitless_book_fixture() -> Value {
        serde_json::json!({
            "tokenId": "111",
            "bids": [
                {"price": 0.44, "size": 20_000_000},
                {"price": 0.44, "size": 5_000_000},
                {"price": 0.40, "size": 100_000_000}
            ],
            "asks": [
                {"price": 0.47, "size": 10_000_000},
                {"price": 0.47, "size": 2_000_000},
                {"price": 0.50, "size": 100_000_000}
            ]
        })
    }

    #[test]
    fn interleaves_external_sources_before_limit() {
        let events = interleave_source_events(
            vec![
                vec![test_event("a1"), test_event("a2"), test_event("a3")],
                vec![test_event("b1")],
                vec![test_event("c1"), test_event("c2")],
            ],
            5,
        );
        let ids: Vec<_> = events.iter().map(|event| event.event_id.as_str()).collect();
        assert_eq!(ids, vec!["a1", "b1", "c1", "a2", "c2"]);
    }

    #[test]
    fn source_response_chunk_limit_rejects_chunked_oversize_without_appending() {
        let mut body = Vec::new();
        assert!(append_limited_chunk(&mut body, b"abc", 5));
        assert!(append_limited_chunk(&mut body, b"de", 5));
        assert_eq!(body, b"abcde");
        assert!(!append_limited_chunk(&mut body, b"f", 5));
        assert_eq!(body, b"abcde");
    }

    #[test]
    fn parser_rejects_zero_missing_and_tiny_liquidity() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 10.0;

        let kalshi_zero = serde_json::json!({"markets":[{
            "ticker":"KXTEST-YES","title":"Will test happen?","yes_sub_title":"Test happens",
            "status":"active","yes_ask_dollars":"0.4200","no_ask_dollars":"0.5900",
            "liquidity_dollars":"0.00"
        }]});
        let manifold_missing = serde_json::json!([{
            "id":"abc","question":"Will it rain?","outcomeType":"BINARY",
            "probability":0.25,"isResolved":false
        }]);
        let limitless_tiny = limitless_market_fixture();
        let limitless_tiny_book = serde_json::json!({
            "tokenId":"111",
            "bids":[{"price":0.44,"size":1_000_000}],
            "asks":[{"price":0.47,"size":1_000_000}]
        });
        let predictit_missing = serde_json::json!({"markets":[{
            "id":1,"name":"Election winner","contracts":[{
                "id":11,"name":"Alice","status":"Open","bestBuyYesCost":0.33,"bestBuyNoCost":0.68
            }]
        }]});

        assert!(parse_kalshi_events(&kalshi_zero, &cfg).is_empty());
        assert!(parse_manifold_events(&manifold_missing, &cfg).is_empty());
        assert!(parse_limitless_market(&limitless_tiny, &limitless_tiny_book, &cfg).is_none());
        assert!(parse_predictit_events(&predictit_missing, &cfg).is_empty());
        assert_eq!(scan_liquidity(&cfg, None), None);
        assert_eq!(scan_liquidity(&cfg, Some(0.0)), None);
        assert_eq!(scan_liquidity(&cfg, Some(9.99)), None);
    }

    #[test]
    fn parses_public_source_payloads_as_scan_only_events_with_liquidity() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;

        let kalshi = serde_json::json!({"markets":[{
            "ticker":"KXTEST-YES","title":"Will test happen?","yes_sub_title":"Test happens",
            "status":"active","yes_ask_dollars":"0.4200","no_ask_dollars":"0.5900",
            "yes_bid_dollars":"0.4100","no_bid_dollars":"0.5800","liquidity_dollars":"12.00"
        }]});
        let manifold = serde_json::json!([{
            "id":"abc","question":"Will it rain?","outcomeType":"BINARY",
            "probability":0.25,"totalLiquidity":100,"isResolved":false
        }]);
        let limitless = limitless_market_fixture();
        let limitless_book = limitless_book_fixture();

        let mut events = Vec::new();
        events.extend(parse_kalshi_events(&kalshi, &cfg));
        events.extend(parse_manifold_events(&manifold, &cfg));
        events.extend(parse_limitless_market(&limitless, &limitless_book, &cfg));

        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|event| !event.enable_neg_risk && !event.neg_risk));
        assert!(events
            .iter()
            .flat_map(|event| &event.markets)
            .all(
                |market| market.clob_token_id_yes.starts_with(EXTERNAL_TOKEN_PREFIX)
                    && market.clob_yes_ask.is_some()
                    && market.clob_no_ask.is_some()
                    && market.liquidity >= cfg.min_liquidity_usd
            ));
    }

    #[test]
    fn limitless_uses_executable_depth_complements_and_real_venue_ids() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        let event =
            parse_limitless_market(&limitless_market_fixture(), &limitless_book_fixture(), &cfg)
                .expect("depth-backed Limitless mirror");
        let market = &event.markets[0];

        assert_eq!(market.clob_yes_ask, Some(0.47));
        assert_eq!(market.clob_yes_bid, Some(0.44));
        assert_eq!(market.clob_no_ask, Some(0.56));
        assert_eq!(market.clob_no_bid, Some(0.53));
        assert_eq!(market.clob_yes_ask_size, Some(12.0));
        assert_eq!(market.clob_no_ask_size, Some(25.0));
        assert!((market.liquidity - 5.64).abs() < 1e-9);
        assert_eq!(market.order_min_size, Some(300.0));
        assert_eq!(market.clob_min_order_size, Some(300.0));
        assert_eq!(market.order_price_min_tick_size, None);
        assert_eq!(market.clob_tick_size, None);
        assert_eq!(
            market.condition_id,
            format!("{}limitless:0xcondition", EXTERNAL_TOKEN_PREFIX)
        );
        assert_eq!(
            market.clob_token_id_yes,
            format!("{}limitless:111", EXTERNAL_TOKEN_PREFIX)
        );
        assert_eq!(
            market.clob_token_id_no,
            format!("{}limitless:222", EXTERNAL_TOKEN_PREFIX)
        );
        assert_eq!(event.category, "limitless-poly-mirror");
    }

    #[test]
    fn limitless_candidates_paginate_deduplicate_and_fail_closed() {
        let valid = limitless_market_fixture();
        let mut duplicate = valid.clone();
        duplicate["id"] = serde_json::json!(999);
        let mut expired = valid.clone();
        expired["slug"] = serde_json::json!("expired");
        expired["expired"] = serde_json::json!(true);
        let mut not_mirror = valid.clone();
        not_mirror["slug"] = serde_json::json!("not-mirror");
        not_mirror["metadata"]["isPolyArbitrage"] = serde_json::json!(false);
        let mut grouped = valid.clone();
        grouped["slug"] = serde_json::json!("grouped");
        grouped["marketType"] = serde_json::json!("group");
        let mut missing_token = valid.clone();
        missing_token["slug"] = serde_json::json!("missing-token");
        missing_token["tokens"]["no"] = Value::Null;
        let page_one = serde_json::json!({"data":[valid, expired, not_mirror]});
        let page_two = serde_json::json!({"data":[duplicate, grouped, missing_token]});

        let candidates = collect_limitless_candidates([&page_one, &page_two], 10);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            field_str(&candidates[0], "slug").as_deref(),
            Some("world-cup-goalkeeper-to-score")
        );
        assert!(collect_limitless_candidates([&page_one], 0).is_empty());

        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        let one_sided = serde_json::json!({
            "tokenId":"111",
            "bids":[{"price":0.44,"size":20_000_000}],
            "asks":[]
        });
        assert!(parse_limitless_market(&limitless_market_fixture(), &one_sided, &cfg).is_none());
        let crossed = serde_json::json!({
            "tokenId":"111",
            "bids":[{"price":0.48,"size":20_000_000}],
            "asks":[{"price":0.47,"size":20_000_000}]
        });
        assert!(parse_limitless_market(&limitless_market_fixture(), &crossed, &cfg).is_none());
        let invalid_size = serde_json::json!({
            "tokenId":"111",
            "bids":[{"price":0.44,"size":0}],
            "asks":[{"price":0.47,"size":20_000_000}]
        });
        assert!(parse_limitless_market(&limitless_market_fixture(), &invalid_size, &cfg).is_none());
        let mut wrong_token = limitless_book_fixture();
        wrong_token["tokenId"] = serde_json::json!("222");
        assert!(parse_limitless_market(&limitless_market_fixture(), &wrong_token, &cfg).is_none());
    }

    #[test]
    fn parses_sxbet_only_when_both_sides_have_depth() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        let market = serde_json::json!({
            "status":"ACTIVE",
            "marketHash":"0xabc",
            "outcomeOneName":"Team A",
            "outcomeTwoName":"Team B",
            "leagueLabel":"Test League"
        });
        let one_sided = serde_json::json!({"data":[{
            "orderStatus":"ACTIVE",
            "totalBetSize":"10000000",
            "fillAmount":"0",
            "pendingFillAmount":"0",
            "percentageOdds":"40000000000000000000",
            "isMakerBettingOutcomeOne":true
        }]});
        assert!(parse_sxbet_market(&market, &one_sided, &cfg).is_none());

        let both_sides = serde_json::json!({"data":[
            {
                "orderStatus":"ACTIVE",
                "totalBetSize":"10000000",
                "fillAmount":"0",
                "pendingFillAmount":"0",
                "percentageOdds":"40000000000000000000",
                "isMakerBettingOutcomeOne":true
            },
            {
                "orderStatus":"ACTIVE",
                "totalBetSize":"20000000",
                "fillAmount":"0",
                "pendingFillAmount":"0",
                "percentageOdds":"70000000000000000000",
                "isMakerBettingOutcomeOne":false
            }
        ]});
        let event = parse_sxbet_market(&market, &both_sides, &cfg).expect("sxbet event");
        let parsed = &event.markets[0];
        assert!((parsed.clob_yes_ask.unwrap() - 0.3).abs() < 1e-9);
        assert!((parsed.clob_no_ask.unwrap() - 0.6).abs() < 1e-9);
        assert!(parsed.clob_yes_ask_size.is_some());
        assert!(parsed.clob_no_ask_size.is_some());
        assert!(parsed.liquidity >= cfg.min_liquidity_usd);
    }

    #[test]
    fn parses_seer_binary_markets_only() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        let data = serde_json::json!({"markets":[
            {
                "id":"0xseer",
                "marketName":"Will the referendum pass?",
                "outcomes":["Yes","No"],
                "odds":[52.5,47.5],
                "payoutReported":false,
                "hasAnswers":false,
                "hasLiquidity":true,
                "liquidityUSD":1250,
                "categories":["politics"]
            },
            {
                "id":"0xinvalid",
                "marketName":"Will the referendum pass with invalid risk?",
                "outcomes":["Yes","No","Invalid"],
                "odds":[52.5,47.5,null],
                "payoutReported":false,
                "hasAnswers":false,
                "hasLiquidity":true,
                "liquidityUSD":1250,
                "categories":["politics"]
            },
            {
                "id":"0xmulti",
                "marketName":"Who wins?",
                "outcomes":["Alice","Bob","Invalid"],
                "odds":[60,40,null],
                "payoutReported":false,
                "hasAnswers":false,
                "hasLiquidity":true,
                "liquidityUSD":1250
            }
        ]});

        let events = parse_seer_events(&data, &cfg);
        assert_eq!(events.len(), 1);
        let parsed = &events[0].markets[0];
        assert_eq!(parsed.clob_yes_ask, Some(0.525));
        assert_eq!(parsed.clob_no_ask, Some(0.475));
        assert!(parsed.clob_token_id_yes.starts_with(EXTERNAL_TOKEN_PREFIX));
    }

    #[test]
    fn parses_betdex_market_with_two_priced_outcomes() {
        let mut cfg = Config::from_env();
        cfg.min_liquidity_usd = 1.0;
        let discovery = serde_json::json!({
            "marketOutcomes":[
                {"id":"yes","title":"Yes"},
                {"id":"no","title":"No"}
            ]
        });
        let market = serde_json::json!({
            "id":"m1",
            "name":"Will the home team win?",
            "published":true,
            "suspended":false,
            "settledAt":null,
            "marketOutcomes":{"_ids":["yes","no"]}
        });
        let prices = serde_json::json!({
            "prices":[{
                "marketId":"m1",
                "liquidity":250,
                "prices":[
                    {"outcomeId":"yes","price":49,"amount":10},
                    {"outcomeId":"yes","price":45,"amount":100},
                    {"outcomeId":"no","price":58,"amount":10},
                    {"outcomeId":"no","price":55,"amount":120}
                ]
            }]
        });
        let event = parse_betdex_market(&market, &discovery, &prices, &cfg).expect("betdex event");
        let parsed = &event.markets[0];
        assert_eq!(parsed.clob_yes_ask, Some(0.45));
        assert_eq!(parsed.clob_no_ask, Some(0.55));
        assert_eq!(parsed.liquidity, 250.0);
        assert_eq!(parsed.clob_yes_ask_size, None);
        assert_eq!(parsed.clob_no_ask_size, None);
        assert!(parsed.clob_token_id_yes.starts_with(EXTERNAL_TOKEN_PREFIX));
    }
}
