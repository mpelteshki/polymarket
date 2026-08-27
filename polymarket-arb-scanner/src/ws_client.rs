use futures::{Sink, SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, Duration, Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::Config;

const WS_ASSET_CHUNK_SIZE: usize = 200;
const WS_DEPTH_LEVEL_LIMIT: usize = 25;
const WS_DEPTH_CHANGE_RING_LIMIT: usize = 128;
const WS_TRADE_RING_LIMIT: usize = 64;
const WS_MARKET_DATA_SILENCE_CHECK_MIN_MS: u64 = 50;

fn runtime_info(config: &Config, message: impl AsRef<str>) {
    if config.verbose_scan_logs {
        info!("{}", message.as_ref());
    } else {
        debug!("{}", message.as_ref());
    }
}

fn runtime_warn(config: &Config, message: impl AsRef<str>) {
    if config.verbose_scan_logs {
        warn!("{}", message.as_ref());
    } else {
        debug!("{}", message.as_ref());
    }
}

fn market_data_silence_timeout(config: &Config) -> Option<Duration> {
    (config.ws_market_data_silence_timeout_ms > 0).then_some(Duration::from_millis(
        config.ws_market_data_silence_timeout_ms,
    ))
}

fn market_data_silence_check_interval(timeout: Duration) -> Duration {
    let timeout_ms = timeout.as_millis().min(u64::MAX as u128) as u64;
    Duration::from_millis(
        timeout_ms
            .saturating_div(2)
            .max(WS_MARKET_DATA_SILENCE_CHECK_MIN_MS),
    )
}

#[derive(Debug, Clone, Serialize)]
struct WsInitialSubscription {
    #[serde(rename = "type")]
    msg_type: String,
    assets_ids: Vec<String>,
    custom_feature_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
struct WsOperationMessage {
    operation: String,
    assets_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom_feature_enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Price {
    pub best_ask: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask_size: Option<f64>,
    pub best_bid_size: Option<f64>,
    pub ask_depth: Vec<(f64, f64)>,
    pub bid_depth: Vec<(f64, f64)>,
    pub recent_trades: VecDeque<TradePrint>,
    pub recent_depth_changes: VecDeque<DepthChangePrint>,
    pub tick_size: Option<f64>,
    pub venue_timestamp_ms: Option<u64>,
    pub book_hash: Option<String>,
    pub snapshot_ready: bool,
    pub last_updated: std::time::Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradePrint {
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub venue_timestamp_ms: Option<u64>,
    pub observed_at: std::time::Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DepthChangePrint {
    pub side: String,
    pub price: f64,
    pub old_size: f64,
    pub new_size: f64,
    pub level_index: Option<usize>,
    pub venue_timestamp_ms: Option<u64>,
    pub observed_at: std::time::Instant,
}

impl Default for Price {
    fn default() -> Self {
        Self {
            best_ask: None,
            best_bid: None,
            best_ask_size: None,
            best_bid_size: None,
            ask_depth: Vec::new(),
            bid_depth: Vec::new(),
            recent_trades: VecDeque::new(),
            recent_depth_changes: VecDeque::new(),
            tick_size: None,
            venue_timestamp_ms: None,
            book_hash: None,
            snapshot_ready: false,
            last_updated: std::time::Instant::now(),
        }
    }
}

pub type PriceCache = Arc<RwLock<HashMap<String, Price>>>;
pub type DirtyTokenReceiver = mpsc::Receiver<WsWake>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WsWake {
    Token(String),
    Discovery,
}

pub struct WsClient {
    config: Config,
    price_cache: PriceCache,
    cmd_rx: mpsc::Receiver<WsCommand>,
    dirty_tx: Option<mpsc::Sender<WsWake>>,
    subscribed_assets: HashSet<String>,
    shard_id: usize,
}

pub struct WsSupervisor {
    config: Config,
    price_cache: PriceCache,
    cmd_rx: mpsc::Receiver<WsCommand>,
    shard_size: usize,
    next_shard_id: usize,
    shard_txs: HashMap<usize, mpsc::Sender<WsCommand>>,
    shard_assets: HashMap<usize, HashSet<String>>,
    asset_to_shard: HashMap<String, usize>,
    dirty_tx: Option<mpsc::Sender<WsWake>>,
}

#[derive(Debug, Clone)]
pub enum WsCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn level_price(level: &Value) -> Option<f64> {
    level.get("price").and_then(value_to_f64)
}

fn level_size(level: &Value) -> Option<f64> {
    level.get("size").and_then(value_to_f64)
}

fn normalized_depth_levels(value: Option<&Value>, ascending: bool) -> Vec<(f64, f64)> {
    let Some(arr) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut levels = arr
        .iter()
        .filter_map(|level| Some((level_price(level)?, level_size(level)?)))
        .filter(|(price, size)| {
            price.is_finite() && *price > 0.0 && size.is_finite() && *size > 0.0
        })
        .collect::<Vec<_>>();
    sort_depth_levels(&mut levels, ascending);
    aggregate_and_truncate_depth(levels)
}

fn sort_depth_levels(levels: &mut [(f64, f64)], ascending: bool) {
    if ascending {
        levels.sort_by(|a, b| a.0.total_cmp(&b.0));
    } else {
        levels.sort_by(|a, b| b.0.total_cmp(&a.0));
    }
}

fn aggregate_and_truncate_depth(levels: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    let mut out: Vec<(f64, f64)> = Vec::new();
    for (price, size) in levels {
        if let Some((last_price, last_size)) = out.last_mut() {
            if (*last_price - price).abs() < 1e-12 {
                *last_size += size;
                continue;
            }
        }
        out.push((price, size));
        if out.len() >= WS_DEPTH_LEVEL_LIMIT {
            break;
        }
    }
    out
}

fn apply_depth_delta(levels: &mut Vec<(f64, f64)>, price: f64, size: f64, ascending: bool) {
    if !price.is_finite() || price <= 0.0 {
        return;
    }
    levels.retain(|(existing, _)| (*existing - price).abs() >= 1e-12);
    if size.is_finite() && size > 0.0 {
        levels.push((price, size));
    }
    sort_depth_levels(levels, ascending);
    if levels.len() > WS_DEPTH_LEVEL_LIMIT {
        levels.truncate(WS_DEPTH_LEVEL_LIMIT);
    }
}

fn record_depth_change(
    entry: &mut Price,
    delta: &DepthDelta,
    venue_timestamp_ms: Option<u64>,
    observed_at: std::time::Instant,
) {
    let (side, levels) = match delta.side {
        DepthSide::Ask => ("ASK", &entry.ask_depth),
        DepthSide::Bid => ("BID", &entry.bid_depth),
    };
    let level_index = levels
        .iter()
        .position(|(price, _)| (*price - delta.price).abs() < 1e-12);
    let old_size = level_index.map(|idx| levels[idx].1).unwrap_or(0.0);
    let new_size = delta.size.max(0.0);
    if (old_size - new_size).abs() < 1e-12 {
        return;
    }

    entry.recent_depth_changes.push_back(DepthChangePrint {
        side: side.to_string(),
        price: delta.price,
        old_size,
        new_size,
        level_index,
        venue_timestamp_ms,
        observed_at,
    });
    while entry.recent_depth_changes.len() > WS_DEPTH_CHANGE_RING_LIMIT {
        entry.recent_depth_changes.pop_front();
    }
}

fn best_ask_from_levels(value: Option<&Value>) -> (Option<f64>, Option<f64>) {
    let Some(arr) = value.and_then(Value::as_array) else {
        return (None, None);
    };
    let Some(best_price) = arr
        .iter()
        .filter_map(level_price)
        .filter(|price| *price > 0.0)
        .min_by(|a, b| a.total_cmp(b))
    else {
        return (None, None);
    };
    let best_size = arr
        .iter()
        .filter_map(|level| {
            let price = level_price(level)?;
            let size = level_size(level)?;
            ((price - best_price).abs() < 1e-12 && size > 0.0).then_some(size)
        })
        .sum::<f64>();
    (Some(best_price), (best_size > 0.0).then_some(best_size))
}

fn best_bid_from_levels(value: Option<&Value>) -> (Option<f64>, Option<f64>) {
    let Some(arr) = value.and_then(Value::as_array) else {
        return (None, None);
    };
    let Some(best_price) = arr
        .iter()
        .filter_map(level_price)
        .filter(|price| *price > 0.0)
        .max_by(|a, b| a.total_cmp(b))
    else {
        return (None, None);
    };
    let best_size = arr
        .iter()
        .filter_map(|level| {
            let price = level_price(level)?;
            let size = level_size(level)?;
            ((price - best_price).abs() < 1e-12 && size > 0.0).then_some(size)
        })
        .sum::<f64>();
    (Some(best_price), (best_size > 0.0).then_some(best_size))
}

fn best_from_depth(levels: &[(f64, f64)]) -> (Option<f64>, Option<f64>) {
    levels
        .iter()
        .find(|(price, size)| price.is_finite() && *price > 0.0 && size.is_finite() && *size > 0.0)
        .map(|(price, size)| (Some(*price), Some(*size)))
        .unwrap_or((None, None))
}

fn field_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(value_to_f64) {
            return Some(v);
        }
    }
    None
}

fn field_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(Value::as_str) {
            return Some(v.to_string());
        }
    }
    None
}

fn value_to_timestamp_ms(value: &Value) -> Option<u64> {
    let raw = match value {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.parse::<f64>().ok()?,
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

fn field_timestamp_ms(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(v) = value.get(*key).and_then(value_to_timestamp_ms) {
            return Some(v);
        }
    }
    None
}

fn event_timestamp_ms(value: &Value) -> Option<u64> {
    field_timestamp_ms(
        value,
        &["timestamp", "time", "ts", "created_at", "createdAt"],
    )
}

fn event_book_hash(value: &Value) -> Option<String> {
    field_string(value, &["hash", "book_hash", "bookHash"])
}

fn quote_is_crossed(best_ask: Option<f64>, best_bid: Option<f64>, tick_size: Option<f64>) -> bool {
    let (Some(ask), Some(bid)) = (best_ask, best_bid) else {
        return false;
    };
    if ask <= 0.0 || bid <= 0.0 {
        return false;
    }
    let tick = tick_size.unwrap_or(0.0001).max(0.0001);
    bid > ask + tick + 1e-12
}

fn invalidate_cached_quote_for_integrity(asset_id: &str, entry: &mut Price, reason: &str) {
    debug!("WS: Invalidating cached quote for {asset_id}: {reason}");
    entry.best_ask = None;
    entry.best_bid = None;
    entry.best_ask_size = None;
    entry.best_bid_size = None;
    entry.ask_depth.clear();
    entry.bid_depth.clear();
    entry.snapshot_ready = false;
    entry.last_updated = std::time::Instant::now();
}

fn invalidate_on_ws_integrity_gap(
    asset_id: &str,
    entry: &mut Price,
    venue_timestamp_ms: Option<u64>,
    book_hash: Option<&str>,
    phase: &str,
) -> bool {
    if let (Some(current), Some(incoming)) = (entry.venue_timestamp_ms, venue_timestamp_ms) {
        if incoming < current {
            invalidate_cached_quote_for_integrity(
                asset_id,
                entry,
                &format!(
                    "regressive {phase} timestamp incoming_ts={incoming} current_ts={current}"
                ),
            );
            return true;
        }
        if incoming == current {
            if let (Some(current_hash), Some(incoming_hash)) =
                (entry.book_hash.as_deref(), book_hash)
            {
                if current_hash != incoming_hash {
                    debug!(
                        "WS: accepting same-ms {phase} hash advance for {asset_id}: current_hash={current_hash} incoming_hash={incoming_hash} timestamp={incoming}"
                    );
                }
            }
        }
    }
    false
}

fn clear_crossed_quote(asset_id: &str, entry: &mut Price) -> bool {
    if quote_is_crossed(entry.best_ask, entry.best_bid, entry.tick_size) {
        invalidate_cached_quote_for_integrity(
            asset_id,
            entry,
            &format!(
                "crossed quote bid={:?} ask={:?} tick={:?}",
                entry.best_bid, entry.best_ask, entry.tick_size
            ),
        );
        return true;
    }
    false
}

#[derive(Debug, Clone, Copy)]
struct DepthDelta {
    side: DepthSide,
    price: f64,
    size: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DepthSide {
    Ask,
    Bid,
}

fn normalize_trade_side(value: Option<String>) -> String {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "BUY" | "BID" | "B" | "1" => "BUY".to_string(),
        "SELL" | "ASK" | "OFFER" | "S" | "0" => "SELL".to_string(),
        other => other.to_string(),
    }
}

fn asset_ids_from_event(value: &Value) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut asset_ids = Vec::new();
    for key in [
        "assets_ids",
        "asset_ids",
        "assetIds",
        "clob_token_ids",
        "clobTokenIds",
        "token_ids",
        "tokenIds",
        "asset_id",
        "assetId",
        "winning_asset_id",
        "winningAssetId",
    ] {
        let Some(field) = value.get(key) else {
            continue;
        };
        match field {
            Value::String(asset_id) => {
                if !asset_id.trim().is_empty() && seen.insert(asset_id.clone()) {
                    asset_ids.push(asset_id.clone());
                }
            }
            Value::Array(values) => {
                for value in values {
                    let Some(asset_id) = value.as_str() else {
                        continue;
                    };
                    if !asset_id.trim().is_empty() && seen.insert(asset_id.to_string()) {
                        asset_ids.push(asset_id.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    asset_ids
}

impl WsClient {
    #[cfg(test)]
    pub fn new(config: Config, price_cache: PriceCache) -> (Self, mpsc::Sender<WsCommand>) {
        Self::new_with_dirty_tokens(config, price_cache, None)
    }

    #[cfg(test)]
    pub fn new_with_dirty_tokens(
        config: Config,
        price_cache: PriceCache,
        dirty_tx: Option<mpsc::Sender<WsWake>>,
    ) -> (Self, mpsc::Sender<WsCommand>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self::with_receiver(config, price_cache, rx, 0, dirty_tx),
            tx,
        )
    }

    fn with_receiver(
        config: Config,
        price_cache: PriceCache,
        cmd_rx: mpsc::Receiver<WsCommand>,
        shard_id: usize,
        dirty_tx: Option<mpsc::Sender<WsWake>>,
    ) -> Self {
        Self {
            config,
            price_cache,
            cmd_rx,
            dirty_tx,
            subscribed_assets: HashSet::new(),
            shard_id,
        }
    }
}

impl WsSupervisor {
    #[cfg(test)]
    pub fn new(config: Config, price_cache: PriceCache) -> (Self, mpsc::Sender<WsCommand>) {
        Self::new_with_dirty_tokens(config, price_cache, None)
    }

    pub fn new_with_dirty_tokens(
        config: Config,
        price_cache: PriceCache,
        dirty_tx: Option<mpsc::Sender<WsWake>>,
    ) -> (Self, mpsc::Sender<WsCommand>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                config,
                price_cache,
                cmd_rx: rx,
                shard_size: 1,
                next_shard_id: 0,
                shard_txs: HashMap::new(),
                shard_assets: HashMap::new(),
                asset_to_shard: HashMap::new(),
                dirty_tx,
            },
            tx,
        )
    }

    fn initialize(mut self) -> Self {
        self.shard_size = self.config.ws_shard_size.max(1);
        self
    }

    fn choose_shard(&mut self) -> usize {
        let mut candidates: Vec<(usize, usize)> = self
            .shard_assets
            .iter()
            .map(|(id, assets)| (*id, assets.len()))
            .filter(|(_, len)| *len < self.shard_size)
            .collect();
        candidates.sort_by_key(|(id, len)| (*len, *id));
        if let Some((id, _)) = candidates.first() {
            *id
        } else {
            let id = self.next_shard_id;
            self.next_shard_id += 1;
            id
        }
    }

    fn ensure_shard(&mut self, shard_id: usize) -> mpsc::Sender<WsCommand> {
        if let Some(tx) = self.shard_txs.get(&shard_id) {
            if !tx.is_closed() {
                return tx.clone();
            }
        }

        let (tx, rx) = mpsc::channel(100);
        let ws_client = WsClient::with_receiver(
            self.config.clone(),
            self.price_cache.clone(),
            rx,
            shard_id,
            self.dirty_tx.clone(),
        );
        tokio::spawn(ws_client.run());
        self.shard_txs.insert(shard_id, tx.clone());
        self.shard_assets.entry(shard_id).or_default();
        runtime_info(
            &self.config,
            format!("WS supervisor: started shard {shard_id}"),
        );
        tx
    }

    fn assign_subscriptions(&mut self, assets: Vec<String>) -> HashMap<usize, Vec<String>> {
        let mut grouped: HashMap<usize, Vec<String>> = HashMap::new();
        let mut assets = assets;
        assets.sort();
        for asset in assets {
            if asset.trim().is_empty() || self.asset_to_shard.contains_key(&asset) {
                continue;
            }
            let shard_id = self.choose_shard();
            self.ensure_shard(shard_id);
            self.asset_to_shard.insert(asset.clone(), shard_id);
            self.shard_assets
                .entry(shard_id)
                .or_default()
                .insert(asset.clone());
            grouped.entry(shard_id).or_default().push(asset);
        }
        grouped
    }

    fn unassign_subscriptions(&mut self, assets: Vec<String>) -> HashMap<usize, Vec<String>> {
        let mut grouped: HashMap<usize, Vec<String>> = HashMap::new();
        for asset in assets {
            let Some(shard_id) = self.asset_to_shard.remove(&asset) else {
                continue;
            };
            if let Some(shard_assets) = self.shard_assets.get_mut(&shard_id) {
                shard_assets.remove(&asset);
            }
            grouped.entry(shard_id).or_default().push(asset);
        }
        grouped
    }

    async fn send_to_shards(&mut self, grouped: HashMap<usize, Vec<String>>, subscribe: bool) {
        for (shard_id, assets) in grouped {
            let tx = self.ensure_shard(shard_id);
            let command = if subscribe {
                WsCommand::Subscribe(assets)
            } else {
                WsCommand::Unsubscribe(assets)
            };
            if let Err(err) = tx.send(command).await {
                warn!("WS supervisor: failed to send command to shard {shard_id}: {err}");
            }
        }
    }

    pub async fn run(mut self) {
        self = self.initialize();
        runtime_info(
            &self.config,
            format!(
                "WS supervisor enabled: shard_size={} target active sockets grow on demand",
                self.shard_size
            ),
        );
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                WsCommand::Subscribe(assets) => {
                    let grouped = self.assign_subscriptions(assets);
                    let added: usize = grouped.values().map(Vec::len).sum();
                    if added > 0 {
                        debug!(
                            "WS supervisor: subscribing {added} assets across {} shard(s)",
                            grouped.len()
                        );
                        self.send_to_shards(grouped, true).await;
                    }
                }
                WsCommand::Unsubscribe(assets) => {
                    let grouped = self.unassign_subscriptions(assets);
                    let removed: usize = grouped.values().map(Vec::len).sum();
                    if removed > 0 {
                        debug!(
                            "WS supervisor: unsubscribing {removed} assets across {} shard(s)",
                            grouped.len()
                        );
                        self.send_to_shards(grouped, false).await;
                    }
                }
            }
        }
        runtime_info(&self.config, "WS supervisor: command channel closed.");
    }
}

impl WsClient {
    async fn send_initial_subscription<S>(
        &self,
        ws_stream: &mut S,
        assets: Vec<String>,
    ) -> Result<(), tokio_tungstenite::tungstenite::Error>
    where
        S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        for chunk in assets.chunks(WS_ASSET_CHUNK_SIZE.max(1)) {
            let Ok(payload) = serde_json::to_string(&WsInitialSubscription {
                msg_type: "market".into(),
                assets_ids: chunk.to_vec(),
                custom_feature_enabled: true,
            }) else {
                warn!("WS: Failed to serialize initial subscription payload");
                continue;
            };
            ws_stream.send(Message::Text(payload)).await?;
        }
        Ok(())
    }

    async fn send_operation<S>(
        &self,
        ws_stream: &mut S,
        operation: &str,
        assets: Vec<String>,
    ) -> Result<(), tokio_tungstenite::tungstenite::Error>
    where
        S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        for chunk in assets.chunks(WS_ASSET_CHUNK_SIZE.max(1)) {
            let Ok(payload) = serde_json::to_string(&WsOperationMessage {
                operation: operation.to_string(),
                assets_ids: chunk.to_vec(),
                custom_feature_enabled: if operation.eq_ignore_ascii_case("subscribe") {
                    Some(true)
                } else {
                    None
                },
            }) else {
                warn!("WS: Failed to serialize operation payload");
                continue;
            };
            ws_stream.send(Message::Text(payload)).await?;
        }
        Ok(())
    }

    fn apply_subscription_command(&mut self, cmd: WsCommand) -> (Vec<String>, Vec<String>) {
        match cmd {
            WsCommand::Subscribe(assets) => {
                let mut added = Vec::new();
                for asset in assets {
                    if self.subscribed_assets.insert(asset.clone()) {
                        added.push(asset);
                    }
                }
                (added, Vec::new())
            }
            WsCommand::Unsubscribe(assets) => {
                let mut removed = Vec::new();
                for asset in assets {
                    if self.subscribed_assets.remove(&asset) {
                        removed.push(asset);
                    }
                }
                (Vec::new(), removed)
            }
        }
    }

    async fn wait_for_first_subscription(&mut self) -> bool {
        while self.subscribed_assets.is_empty() {
            match self.cmd_rx.recv().await {
                Some(cmd) => {
                    let (added, _removed) = self.apply_subscription_command(cmd);
                    if !added.is_empty() {
                        return true;
                    }
                }
                None => return false,
            }
        }
        true
    }

    pub async fn run(mut self) {
        let url = match Url::parse(&self.config.clob_ws_url) {
            Ok(u) => u,
            Err(e) => {
                error!("Invalid WebSocket URL: {e}");
                return;
            }
        };

        loop {
            if self.subscribed_assets.is_empty() {
                debug!("WS: Waiting for first active-slice subscription before connecting.");
                if !self.wait_for_first_subscription().await {
                    runtime_info(
                        &self.config,
                        "WS: Command channel closed before any subscriptions were received.",
                    );
                    return;
                }
            }

            runtime_info(&self.config, format!("WS: Connecting to {}...", url));
            let mut reconnect_without_sleep = false;
            match connect_async(&url).await {
                Ok((mut ws_stream, _)) => {
                    runtime_info(
                        &self.config,
                        format!("WS shard {}: connected.", self.shard_id),
                    );
                    let mut heartbeat = time::interval(Duration::from_secs(10));
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    let silence_timeout = market_data_silence_timeout(&self.config);
                    let mut silence_check = time::interval(
                        silence_timeout
                            .map(market_data_silence_check_interval)
                            .unwrap_or(Duration::from_secs(3600)),
                    );
                    silence_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    let mut last_market_data_at = Instant::now();

                    let assets: Vec<String> = self.subscribed_assets.iter().cloned().collect();
                    if !assets.is_empty() {
                        let removed = self.remove_cached_assets(&assets).await;
                        if removed > 0 {
                            runtime_warn(
                                &self.config,
                                format!(
                                    "WS shard {}: invalidated {} cached quote(s) before subscription snapshot",
                                    self.shard_id, removed
                                ),
                            );
                        }
                    }
                    if !assets.is_empty() {
                        if let Err(e) = self.send_initial_subscription(&mut ws_stream, assets).await
                        {
                            runtime_warn(
                                &self.config,
                                format!("WS: Failed to send initial subscription: {e}"),
                            );
                            continue;
                        }
                    }

                    loop {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                if let Err(e) = ws_stream.send(Message::Text("PING".to_string())).await {
                                    runtime_warn(&self.config, format!("WS: Heartbeat send failed: {e}"));
                                    break;
                                }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if self.handle_message(&text).await {
                                            last_market_data_at = Instant::now();
                                        }
                                    }
                                    Some(Ok(Message::Ping(p))) => {
                                        let _ = ws_stream.send(Message::Pong(p)).await;
                                    }
                                    Some(Ok(Message::Close(_))) => {
                                        runtime_warn(&self.config, "WS: Socket closed by server.");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        runtime_warn(&self.config, format!("WS: Stream error: {e}"));
                                        break;
                                    }
                                    None => {
                                        runtime_warn(&self.config, "WS: Socket ended.");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            _ = silence_check.tick(), if silence_timeout.is_some() => {
                                let timeout = silence_timeout.unwrap_or_default();
                                if !self.subscribed_assets.is_empty() && last_market_data_at.elapsed() > timeout {
                                    let assets: Vec<String> = self.subscribed_assets.iter().cloned().collect();
                                    let removed = self.remove_cached_assets(&assets).await;
                                    runtime_warn(
                                        &self.config,
                                        format!(
                                            "WS shard {}: no market data for {}ms across {} subscribed assets; invalidated {} cached quote(s) and reconnecting",
                                            self.shard_id,
                                            last_market_data_at.elapsed().as_millis(),
                                            assets.len(),
                                            removed
                                        ),
                                    );
                                    reconnect_without_sleep = true;
                                    let _ = ws_stream.close(None).await;
                                    break;
                                }
                            }
                            cmd = self.cmd_rx.recv() => {
                                match cmd {
                                    Some(cmd) => {
                                        let (added, removed) = self.apply_subscription_command(cmd);
                                        if !added.is_empty() {
                                            debug!("WS: Subscribing to {} assets", added.len());
                                            let removed_cached = self.remove_cached_assets(&added).await;
                                            if removed_cached > 0 {
                                                runtime_warn(
                                                    &self.config,
                                                    format!(
                                                        "WS shard {}: invalidated {} cached quote(s) before incremental subscription snapshot",
                                                        self.shard_id, removed_cached
                                                    ),
                                                );
                                            }
                                            if let Err(e) = self.send_operation(&mut ws_stream, "subscribe", added).await {
                                                error!("WS: Failed to send subscribe op: {e}");
                                                break;
                                            }
                                            last_market_data_at = Instant::now();
                                        }
                                        if !removed.is_empty() {
                                            debug!("WS: Unsubscribing from {} assets", removed.len());
                                            if let Err(e) = self.send_operation(&mut ws_stream, "unsubscribe", removed).await {
                                                error!("WS: Failed to send unsubscribe op: {e}");
                                                break;
                                            }
                                        }
                                        if self.subscribed_assets.is_empty() {
                                            runtime_info(&self.config, "WS: Active slice is empty; closing idle market socket until new subscriptions arrive.");
                                            let _ = ws_stream.close(None).await;
                                            break;
                                        }
                                    }
                                    None => {
                                        runtime_info(&self.config, "WS: Command channel closed; stopping market socket task.");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => runtime_warn(
                    &self.config,
                    format!("WS: Failed to connect: {e}. Retrying in 5s..."),
                ),
            }

            if self.subscribed_assets.is_empty() {
                continue;
            }
            if reconnect_without_sleep {
                continue;
            }
            time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn update_price(
        &self,
        asset_id: &str,
        best_ask: Option<f64>,
        best_bid: Option<f64>,
        best_ask_size: Option<f64>,
        best_bid_size: Option<f64>,
        ask_depth: Option<Vec<(f64, f64)>>,
        bid_depth: Option<Vec<(f64, f64)>>,
        depth_delta: Option<DepthDelta>,
        tick_size: Option<f64>,
        clear_best_ask_size: bool,
        clear_best_bid_size: bool,
        venue_timestamp_ms: Option<u64>,
        book_hash: Option<String>,
        snapshot_ready: bool,
    ) {
        let mut cache = self.price_cache.write().await;
        if !snapshot_ready
            && !cache
                .get(asset_id)
                .map(|entry| entry.snapshot_ready)
                .unwrap_or(false)
        {
            debug!("WS: Ignoring delta quote for {asset_id} before initial book snapshot");
            return;
        }
        let entry = cache.entry(asset_id.to_string()).or_default();
        if invalidate_on_ws_integrity_gap(
            asset_id,
            entry,
            venue_timestamp_ms,
            book_hash.as_deref(),
            "quote",
        ) {
            return;
        }
        if let Some(ask) = best_ask {
            let depth_size_at_ask = entry
                .ask_depth
                .iter()
                .find(|(price, size)| {
                    (*price - ask).abs() < 1e-12
                        && price.is_finite()
                        && *price > 0.0
                        && size.is_finite()
                        && *size > 0.0
                })
                .map(|(_, size)| *size);
            if (clear_best_ask_size && depth_size_at_ask.is_none())
                || (best_ask_size.is_none() && entry.best_ask != Some(ask))
            {
                entry.best_ask_size = None;
            }
            entry.best_ask = Some(ask);
            if best_ask_size.is_none() && entry.best_ask_size.is_none() {
                entry.best_ask_size = depth_size_at_ask;
            }
        }
        if let Some(bid) = best_bid {
            let depth_size_at_bid = entry
                .bid_depth
                .iter()
                .find(|(price, size)| {
                    (*price - bid).abs() < 1e-12
                        && price.is_finite()
                        && *price > 0.0
                        && size.is_finite()
                        && *size > 0.0
                })
                .map(|(_, size)| *size);
            if (clear_best_bid_size && depth_size_at_bid.is_none())
                || (best_bid_size.is_none() && entry.best_bid != Some(bid))
            {
                entry.best_bid_size = None;
            }
            entry.best_bid = Some(bid);
            if best_bid_size.is_none() && entry.best_bid_size.is_none() {
                entry.best_bid_size = depth_size_at_bid;
            }
        }
        if best_ask_size.is_some() {
            entry.best_ask_size = best_ask_size;
        }
        if best_bid_size.is_some() {
            entry.best_bid_size = best_bid_size;
        }
        let ask_depth_updated = ask_depth.is_some()
            || matches!(
                depth_delta,
                Some(DepthDelta {
                    side: DepthSide::Ask,
                    ..
                })
            );
        let bid_depth_updated = bid_depth.is_some()
            || matches!(
                depth_delta,
                Some(DepthDelta {
                    side: DepthSide::Bid,
                    ..
                })
            );
        if let Some(levels) = ask_depth {
            entry.ask_depth = levels;
        }
        if let Some(levels) = bid_depth {
            entry.bid_depth = levels;
        }
        let observed_at = std::time::Instant::now();
        if let Some(delta) = depth_delta {
            record_depth_change(entry, &delta, venue_timestamp_ms, observed_at);
            match delta.side {
                DepthSide::Ask => {
                    apply_depth_delta(&mut entry.ask_depth, delta.price, delta.size, true)
                }
                DepthSide::Bid => {
                    apply_depth_delta(&mut entry.bid_depth, delta.price, delta.size, false)
                }
            }
        }
        if ask_depth_updated && best_ask.is_none() {
            let (ask, size) = best_from_depth(&entry.ask_depth);
            entry.best_ask = ask;
            entry.best_ask_size = size;
        } else if ask_depth_updated && best_ask_size.is_none() {
            entry.best_ask_size = entry.best_ask.and_then(|ask| {
                entry
                    .ask_depth
                    .iter()
                    .find(|(price, size)| {
                        (*price - ask).abs() < 1e-12
                            && price.is_finite()
                            && *price > 0.0
                            && size.is_finite()
                            && *size > 0.0
                    })
                    .map(|(_, size)| *size)
            });
        }
        if bid_depth_updated && best_bid.is_none() {
            let (bid, size) = best_from_depth(&entry.bid_depth);
            entry.best_bid = bid;
            entry.best_bid_size = size;
        } else if bid_depth_updated && best_bid_size.is_none() {
            entry.best_bid_size = entry.best_bid.and_then(|bid| {
                entry
                    .bid_depth
                    .iter()
                    .find(|(price, size)| {
                        (*price - bid).abs() < 1e-12
                            && price.is_finite()
                            && *price > 0.0
                            && size.is_finite()
                            && *size > 0.0
                    })
                    .map(|(_, size)| *size)
            });
        }
        if tick_size.is_some() {
            entry.tick_size = tick_size;
        }
        if venue_timestamp_ms.is_some() {
            entry.venue_timestamp_ms = venue_timestamp_ms;
        }
        if book_hash.is_some() {
            entry.book_hash = book_hash;
        }
        if snapshot_ready {
            entry.snapshot_ready = true;
        }
        clear_crossed_quote(asset_id, entry);
        entry.last_updated = observed_at;
    }

    async fn remove_ask_level(
        &self,
        asset_id: &str,
        removed_price: f64,
        best_ask: Option<f64>,
        best_bid: Option<f64>,
        tick_size: Option<f64>,
        venue_timestamp_ms: Option<u64>,
        book_hash: Option<String>,
        snapshot_ready: bool,
    ) {
        let mut cache = self.price_cache.write().await;
        if !snapshot_ready
            && !cache
                .get(asset_id)
                .map(|entry| entry.snapshot_ready)
                .unwrap_or(false)
        {
            debug!("WS: Ignoring ask removal for {asset_id} before initial book snapshot");
            return;
        }
        let entry = cache.entry(asset_id.to_string()).or_default();
        if invalidate_on_ws_integrity_gap(
            asset_id,
            entry,
            venue_timestamp_ms,
            book_hash.as_deref(),
            "ask removal",
        ) {
            return;
        }
        if let Some(ask) = best_ask {
            if entry.best_ask != Some(ask) {
                entry.best_ask_size = None;
            }
            entry.best_ask = Some(ask);
        }
        apply_depth_delta(&mut entry.ask_depth, removed_price, 0.0, true);
        if best_ask.is_none() {
            let (ask, size) = best_from_depth(&entry.ask_depth);
            entry.best_ask = ask;
            entry.best_ask_size = size;
        } else {
            entry.best_ask_size = entry.best_ask.and_then(|ask| {
                entry
                    .ask_depth
                    .iter()
                    .find(|(price, size)| {
                        (*price - ask).abs() < 1e-12
                            && price.is_finite()
                            && *price > 0.0
                            && size.is_finite()
                            && *size > 0.0
                    })
                    .map(|(_, size)| *size)
            });
        }
        if let Some(bid) = best_bid {
            entry.best_bid = Some(bid);
        }
        if let Some(tick) = tick_size {
            entry.tick_size = Some(tick);
        }
        let observed_at = std::time::Instant::now();
        record_depth_change(
            entry,
            &DepthDelta {
                side: DepthSide::Ask,
                price: removed_price,
                size: 0.0,
            },
            venue_timestamp_ms,
            observed_at,
        );
        if venue_timestamp_ms.is_some() {
            entry.venue_timestamp_ms = venue_timestamp_ms;
        }
        if book_hash.is_some() {
            entry.book_hash = book_hash;
        }
        if snapshot_ready {
            entry.snapshot_ready = true;
        }
        clear_crossed_quote(asset_id, entry);
        entry.last_updated = observed_at;
    }

    async fn remove_cached_assets(&self, asset_ids: &[String]) -> usize {
        let mut cache = self.price_cache.write().await;
        asset_ids
            .iter()
            .filter(|asset_id| cache.remove(asset_id.as_str()).is_some())
            .count()
    }

    async fn record_trade_print(&self, asset_id: &str, trade: TradePrint) {
        let mut cache = self.price_cache.write().await;
        let entry = cache.entry(asset_id.to_string()).or_default();
        entry.recent_trades.push_back(trade);
        while entry.recent_trades.len() > WS_TRADE_RING_LIMIT {
            entry.recent_trades.pop_front();
        }
    }

    async fn apply_tick_size_change(
        &self,
        asset_id: &str,
        tick_size: Option<f64>,
        venue_timestamp_ms: Option<u64>,
        book_hash: Option<String>,
    ) {
        let mut cache = self.price_cache.write().await;
        let entry = cache.entry(asset_id.to_string()).or_default();
        if invalidate_on_ws_integrity_gap(
            asset_id,
            entry,
            venue_timestamp_ms,
            book_hash.as_deref(),
            "tick-size change",
        ) {
            return;
        }
        entry.best_ask = None;
        entry.best_bid = None;
        entry.best_ask_size = None;
        entry.best_bid_size = None;
        entry.ask_depth.clear();
        entry.bid_depth.clear();
        if let Some(tick) = tick_size {
            entry.tick_size = Some(tick);
        }
        if venue_timestamp_ms.is_some() {
            entry.venue_timestamp_ms = venue_timestamp_ms;
        }
        if book_hash.is_some() {
            entry.book_hash = book_hash;
        }
        entry.snapshot_ready = false;
        entry.last_updated = std::time::Instant::now();
    }

    async fn emit_dirty_asset(&self, asset_id: &str) {
        let Some(tx) = self.dirty_tx.as_ref() else {
            return;
        };
        if tx.try_send(WsWake::Token(asset_id.to_string())).is_err() {
            debug!("WS: dirty-token queue full or closed; dropped {asset_id}");
        }
    }

    async fn emit_discovery_wake(&self, reason: &str) {
        let Some(tx) = self.dirty_tx.as_ref() else {
            return;
        };
        if tx.try_send(WsWake::Discovery).is_err() {
            debug!("WS: discovery-wake queue full or closed; dropped {reason}");
        }
    }

    async fn handle_price_changes(&self, value: &Value) {
        let Some(changes) = value.get("price_changes").and_then(Value::as_array) else {
            return;
        };
        let parent_timestamp_ms = event_timestamp_ms(value);
        let parent_book_hash = event_book_hash(value);
        let mut emitted_assets = HashSet::new();

        for change in changes {
            let Some(asset_id) = field_string(change, &["asset_id", "assetId"]) else {
                continue;
            };
            let venue_timestamp_ms = event_timestamp_ms(change).or(parent_timestamp_ms);
            let book_hash = event_book_hash(change).or_else(|| parent_book_hash.clone());
            let best_ask = field_f64(
                change,
                &["best_ask", "bestAsk", "best_ask_price", "bestAskPrice"],
            );
            let best_bid = field_f64(
                change,
                &["best_bid", "bestBid", "best_bid_price", "bestBidPrice"],
            );
            let tick_size = field_f64(
                change,
                &["tick_size", "tickSize", "new_tick_size", "newTickSize"],
            );
            let price = field_f64(change, &["price"]);
            let size = field_f64(change, &["size"]);
            let side = field_string(change, &["side"])
                .unwrap_or_default()
                .to_ascii_uppercase();
            if side == "SELL" && matches!(size, Some(size) if size <= f64::EPSILON) {
                if let Some(price) = price {
                    self.remove_ask_level(
                        &asset_id,
                        price,
                        best_ask,
                        best_bid,
                        tick_size,
                        venue_timestamp_ms,
                        book_hash,
                        false,
                    )
                    .await;
                    if emitted_assets.insert(asset_id.clone()) {
                        self.emit_dirty_asset(&asset_id).await;
                    }
                    continue;
                }
            }
            let depth_delta = match (side.as_str(), price, size) {
                ("SELL", Some(price), Some(size)) => Some(DepthDelta {
                    side: DepthSide::Ask,
                    price,
                    size,
                }),
                ("BUY", Some(price), Some(size)) => Some(DepthDelta {
                    side: DepthSide::Bid,
                    price,
                    size,
                }),
                _ => None,
            };
            let best_ask_size = if side == "SELL" && best_ask.is_some() && price == best_ask {
                size.filter(|size| *size > 0.0)
            } else {
                None
            };
            let best_bid_size = if side == "BUY" && best_bid.is_some() && price == best_bid {
                size.filter(|size| *size > 0.0)
            } else {
                None
            };
            self.update_price(
                &asset_id,
                best_ask,
                best_bid,
                best_ask_size,
                best_bid_size,
                None,
                None,
                depth_delta,
                tick_size,
                false,
                false,
                venue_timestamp_ms,
                book_hash,
                false,
            )
            .await;
            if emitted_assets.insert(asset_id.clone()) {
                self.emit_dirty_asset(&asset_id).await;
            }
        }
    }

    async fn handle_trade_print(&self, value: &Value) {
        let Some(asset_id) = field_string(
            value,
            &["asset_id", "assetId", "token_id", "tokenId", "market"],
        ) else {
            return;
        };
        let Some(price) = field_f64(value, &["price", "last_trade_price", "lastTradePrice"]) else {
            return;
        };
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let size = field_f64(value, &["size", "amount", "quantity", "matched_amount"])
            .filter(|size| size.is_finite() && *size > 0.0)
            .unwrap_or(0.0);
        self.record_trade_print(
            &asset_id,
            TradePrint {
                side: normalize_trade_side(field_string(
                    value,
                    &["side", "taker_side", "takerSide"],
                )),
                price,
                size,
                venue_timestamp_ms: event_timestamp_ms(value),
                observed_at: std::time::Instant::now(),
            },
        )
        .await;
        self.emit_dirty_asset(&asset_id).await;
    }

    async fn handle_message(&self, text: &str) -> bool {
        if text.trim().eq_ignore_ascii_case("PONG") || text.trim().eq_ignore_ascii_case("PING") {
            return false;
        }

        let Ok(value) = serde_json::from_str::<Value>(text) else {
            return false;
        };

        let event_type = field_string(&value, &["event_type", "eventType", "type"])
            .unwrap_or_default()
            .to_ascii_lowercase();

        match event_type.as_str() {
            "last_trade_price" | "trade" => {
                self.handle_trade_print(&value).await;
                true
            }
            "price_change" => {
                if value
                    .get("price_changes")
                    .and_then(Value::as_array)
                    .is_none()
                {
                    return false;
                }
                self.handle_price_changes(&value).await;
                true
            }
            "book" | "best_bid_ask" => {
                let Some(asset_id) = field_string(&value, &["asset_id", "assetId"]) else {
                    return false;
                };
                let ask_depth = normalized_depth_levels(value.get("asks"), true);
                let bid_depth = normalized_depth_levels(value.get("bids"), false);
                let (book_best_ask, book_best_ask_size) = best_ask_from_levels(value.get("asks"));
                let (book_best_bid, book_best_bid_size) = best_bid_from_levels(value.get("bids"));
                let best_ask = field_f64(
                    &value,
                    &["best_ask", "bestAsk", "best_ask_price", "bestAskPrice"],
                )
                .or(book_best_ask);
                let best_bid = field_f64(
                    &value,
                    &["best_bid", "bestBid", "best_bid_price", "bestBidPrice"],
                )
                .or(book_best_bid);
                let best_ask_size =
                    field_f64(&value, &["best_ask_size", "bestAskSize"]).or(book_best_ask_size);
                let best_bid_size =
                    field_f64(&value, &["best_bid_size", "bestBidSize"]).or(book_best_bid_size);
                let tick_size = field_f64(
                    &value,
                    &["tick_size", "tickSize", "new_tick_size", "newTickSize"],
                );
                let venue_timestamp_ms = event_timestamp_ms(&value);
                let book_hash = event_book_hash(&value);
                self.update_price(
                    &asset_id,
                    best_ask,
                    best_bid,
                    best_ask_size,
                    best_bid_size,
                    (event_type == "book").then_some(ask_depth),
                    (event_type == "book").then_some(bid_depth),
                    None,
                    tick_size,
                    event_type == "best_bid_ask" && best_ask_size.is_none(),
                    event_type == "best_bid_ask" && best_bid_size.is_none(),
                    venue_timestamp_ms,
                    book_hash,
                    event_type == "book",
                )
                .await;
                self.emit_dirty_asset(&asset_id).await;
                true
            }
            "tick_size_change" => {
                let Some(asset_id) = field_string(&value, &["asset_id", "assetId"]) else {
                    return false;
                };
                let tick_size = field_f64(
                    &value,
                    &["tick_size", "tickSize", "new_tick_size", "newTickSize"],
                );
                self.apply_tick_size_change(
                    &asset_id,
                    tick_size,
                    event_timestamp_ms(&value),
                    event_book_hash(&value),
                )
                .await;
                self.emit_dirty_asset(&asset_id).await;
                true
            }
            "market_resolved" => {
                let asset_ids = asset_ids_from_event(&value);
                let removed = self.remove_cached_assets(&asset_ids).await;
                if removed > 0 {
                    debug!("WS: Removed {removed} resolved market asset prices from cache");
                }
                self.emit_discovery_wake("market_resolved").await;
                true
            }
            "new_market" => {
                for asset_id in asset_ids_from_event(&value) {
                    self.emit_dirty_asset(&asset_id).await;
                }
                self.emit_discovery_wake("new_market").await;
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_silence_timeout_can_disable_and_checks_at_half_timeout() {
        let mut cfg = Config::from_env();
        cfg.ws_market_data_silence_timeout_ms = 0;
        assert!(market_data_silence_timeout(&cfg).is_none());

        cfg.ws_market_data_silence_timeout_ms = 2_500;
        assert_eq!(
            market_data_silence_timeout(&cfg),
            Some(Duration::from_millis(2_500))
        );
        assert_eq!(
            market_data_silence_check_interval(Duration::from_millis(2_500)),
            Duration::from_millis(1_250)
        );
        assert_eq!(
            market_data_silence_check_interval(Duration::from_millis(20)),
            Duration::from_millis(WS_MARKET_DATA_SILENCE_CHECK_MIN_MS)
        );
    }

    #[tokio::test]
    async fn supervisor_shards_subscriptions_by_configured_size() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let mut cfg = Config::from_env();
        cfg.ws_shard_size = 2;
        let (supervisor, _tx) = WsSupervisor::new(cfg, cache);
        let mut supervisor = supervisor.initialize();

        let grouped = supervisor.assign_subscriptions(vec![
            "asset-1".into(),
            "asset-2".into(),
            "asset-3".into(),
            "asset-4".into(),
            "asset-5".into(),
        ]);

        assert_eq!(grouped.len(), 3);
        assert_eq!(supervisor.asset_to_shard.len(), 5);
        assert!(supervisor
            .shard_assets
            .values()
            .all(|assets| assets.len() <= 2));
    }

    #[tokio::test]
    async fn supervisor_splits_501_assets_into_three_shards_at_200() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let mut cfg = Config::from_env();
        cfg.ws_shard_size = 200;
        let (supervisor, _tx) = WsSupervisor::new(cfg, cache);
        let mut supervisor = supervisor.initialize();
        let assets: Vec<String> = (0..501).map(|idx| format!("asset-{idx:03}")).collect();

        supervisor.assign_subscriptions(assets);

        let mut shard_sizes: Vec<usize> =
            supervisor.shard_assets.values().map(HashSet::len).collect();
        shard_sizes.sort_unstable();
        assert_eq!(shard_sizes, vec![101, 200, 200]);
    }

    #[tokio::test]
    async fn supervisor_unassigns_assets_from_original_shards() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let mut cfg = Config::from_env();
        cfg.ws_shard_size = 2;
        let (supervisor, _tx) = WsSupervisor::new(cfg, cache);
        let mut supervisor = supervisor.initialize();
        supervisor.assign_subscriptions(vec!["asset-1".into(), "asset-2".into(), "asset-3".into()]);

        let grouped = supervisor.unassign_subscriptions(vec!["asset-2".into(), "asset-3".into()]);

        let removed: usize = grouped.values().map(Vec::len).sum();
        assert_eq!(removed, 2);
        assert!(!supervisor.asset_to_shard.contains_key("asset-2"));
        assert!(!supervisor.asset_to_shard.contains_key("asset-3"));
        assert!(supervisor.asset_to_shard.contains_key("asset-1"));
    }

    #[tokio::test]
    async fn handle_message_updates_book_prices() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        assert!(client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-1","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await);

        let cache = cache.read().await;
        let price = cache.get("asset-1").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.42));
        assert_eq!(price.best_bid, Some(0.40));
        assert_eq!(price.best_ask_size, Some(25.0));
        assert_eq!(price.best_bid_size, Some(30.0));
        assert!(price.snapshot_ready);
    }

    #[tokio::test]
    async fn handle_message_marks_only_real_market_data_for_silence_watchdog() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        assert!(!client.handle_message("PONG").await);
        assert!(!client.handle_message("not-json").await);
        assert!(
            !client
                .handle_message(r#"{"event_type":"price_change"}"#)
                .await
        );
        assert!(client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-watch","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await);
        assert!(client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-watch","price":"0.41","size":"12","side":"SELL"}]}"#)
            .await);
    }

    #[tokio::test]
    async fn handle_message_emits_dirty_tokens_for_book_and_price_change() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (dirty_tx, mut dirty_rx) = mpsc::channel(10);
        let (client, _tx) = WsClient::new_with_dirty_tokens(cfg, cache, Some(dirty_tx));

        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-book","best_ask":"0.42","best_bid":"0.40"}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-price","price":"0.41","size":"12","side":"SELL"}]}"#)
            .await;

        let mut dirty = HashSet::new();
        for _ in 0..2 {
            match dirty_rx.try_recv().unwrap() {
                WsWake::Token(token_id) => {
                    dirty.insert(token_id);
                }
                WsWake::Discovery => panic!("unexpected discovery wake"),
            }
        }
        assert_eq!(
            dirty,
            HashSet::from(["asset-book".to_string(), "asset-price".to_string()])
        );
    }

    #[tokio::test]
    async fn price_change_emits_one_dirty_wake_per_asset_in_batch() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (dirty_tx, mut dirty_rx) = mpsc::channel(10);
        let (client, _tx) = WsClient::new_with_dirty_tokens(cfg, cache, Some(dirty_tx));

        client
            .handle_message(
                r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-a","price":"0.41","size":"12","side":"SELL"},{"asset_id":"asset-a","price":"0.42","size":"3","side":"SELL"},{"asset_id":"asset-b","price":"0.39","size":"2","side":"BUY"}]}"#,
            )
            .await;

        let mut dirty = HashSet::new();
        for _ in 0..2 {
            match dirty_rx.try_recv().unwrap() {
                WsWake::Token(token_id) => {
                    dirty.insert(token_id);
                }
                WsWake::Discovery => panic!("unexpected discovery wake"),
            }
        }
        assert_eq!(
            dirty,
            HashSet::from(["asset-a".to_string(), "asset-b".to_string()])
        );
        assert!(dirty_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handle_message_emits_discovery_wake_for_new_market() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (dirty_tx, mut dirty_rx) = mpsc::channel(10);
        let (client, _tx) = WsClient::new_with_dirty_tokens(cfg, cache, Some(dirty_tx));

        client
            .handle_message(
                r#"{"event_type":"new_market","condition_id":"cond","clob_token_ids":["asset-yes","asset-no"]}"#,
            )
            .await;

        let mut wakes = HashSet::new();
        for _ in 0..3 {
            wakes.insert(dirty_rx.try_recv().unwrap());
        }
        assert!(wakes.contains(&WsWake::Discovery));
        assert!(wakes.contains(&WsWake::Token("asset-yes".into())));
        assert!(wakes.contains(&WsWake::Token("asset-no".into())));
    }

    #[tokio::test]
    async fn handle_message_selects_best_prices_from_unsorted_book() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-unsorted","asks":[{"price":"0.45","size":"10"},{"price":"0.42","size":"25"},{"price":"0.42","size":"5"}],"bids":[{"price":"0.38","size":"30"},{"price":"0.40","size":"12"},{"price":"0.40","size":"8"}]}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-unsorted").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.42));
        assert_eq!(price.best_bid, Some(0.40));
        assert_eq!(price.best_ask_size, Some(30.0));
        assert_eq!(price.best_bid_size, Some(20.0));
        assert_eq!(price.ask_depth, vec![(0.42, 30.0), (0.45, 10.0)]);
        assert_eq!(price.bid_depth, vec![(0.40, 20.0), (0.38, 30.0)]);
    }

    #[tokio::test]
    async fn handle_message_updates_tick_size() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(
                r#"{"event_type":"tick_size_change","asset_id":"asset-2","new_tick_size":"0.01"}"#,
            )
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-2").expect("price inserted");
        assert_eq!(price.tick_size, Some(0.01));
    }

    #[tokio::test]
    async fn tick_size_change_clears_cached_prices() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-tick","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(
                r#"{"event_type":"tick_size_change","asset_id":"asset-tick","new_tick_size":"0.01"}"#,
            )
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-tick").expect("price inserted");
        assert_eq!(price.best_ask, None);
        assert_eq!(price.best_bid, None);
        assert_eq!(price.best_ask_size, None);
        assert_eq!(price.best_bid_size, None);
        assert_eq!(price.tick_size, Some(0.01));
    }

    #[tokio::test]
    async fn handle_message_updates_best_bid_ask_payload() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-3","asks":[{"price":"0.56","size":"25"}],"bids":[{"price":"0.52","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-3","best_ask":"0.55","best_bid":"0.53"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-3").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.55));
        assert_eq!(price.best_bid, Some(0.53));
    }

    #[tokio::test]
    async fn best_bid_ask_before_book_snapshot_is_ignored() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-untrusted","best_ask":"0.55","best_bid":"0.53"}"#)
            .await;

        let cache = cache.read().await;
        assert!(!cache.contains_key("asset-untrusted"));
    }

    #[tokio::test]
    async fn crossed_best_bid_ask_clears_executable_quote() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-crossed","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}],"tick_size":"0.01"}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-crossed","best_ask":"0.42","best_bid":"0.44"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-crossed").expect("price inserted");
        assert_eq!(price.best_ask, None);
        assert_eq!(price.best_bid, None);
        assert_eq!(price.best_ask_size, None);
        assert_eq!(price.tick_size, Some(0.01));
        assert!(!price.snapshot_ready);
    }

    #[tokio::test]
    async fn older_timestamped_price_update_invalidates_cache_for_rest_repair() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (dirty_tx, mut dirty_rx) = mpsc::channel(10);
        let (client, _tx) = WsClient::new_with_dirty_tokens(cfg, cache.clone(), Some(dirty_tx));

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-ts","timestamp":"1700000002000","hash":"newer","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-ts","timestamp":"1700000001000","hash":"older","best_ask":"0.55","best_bid":"0.53"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-ts").expect("price inserted");
        assert_eq!(price.best_ask, None);
        assert_eq!(price.best_bid, None);
        assert_eq!(price.best_ask_size, None);
        assert_eq!(price.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(price.book_hash.as_deref(), Some("newer"));
        assert!(!price.snapshot_ready);
        drop(cache);

        let first_wake = dirty_rx.try_recv().unwrap();
        let second_wake = dirty_rx.try_recv().unwrap();
        assert_eq!(first_wake, WsWake::Token("asset-ts".into()));
        assert_eq!(second_wake, WsWake::Token("asset-ts".into()));
    }

    #[tokio::test]
    async fn same_timestamp_hash_mismatch_applies_as_same_ms_update() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-hash","timestamp":"1700000002000","hash":"hash-a","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-hash","timestamp":"1700000002000","hash":"hash-b","best_ask":"0.43","best_bid":"0.41"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-hash").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.43));
        assert_eq!(price.best_bid, Some(0.41));
        assert_eq!(price.venue_timestamp_ms, Some(1_700_000_002_000));
        assert_eq!(price.book_hash.as_deref(), Some("hash-b"));
        assert!(price.snapshot_ready);
    }

    #[tokio::test]
    async fn older_timestamped_ask_removal_invalidates_cache_for_rest_repair() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-remove-ts","timestamp":"1700000002000","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"price_change","timestamp":"1700000001000","price_changes":[{"asset_id":"asset-remove-ts","price":"0.42","size":"0","side":"SELL"}]}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-remove-ts").expect("price inserted");
        assert_eq!(price.best_ask, None);
        assert_eq!(price.best_ask_size, None);
        assert_eq!(price.venue_timestamp_ms, Some(1_700_000_002_000));
        assert!(!price.snapshot_ready);
    }

    #[tokio::test]
    async fn market_resolved_removes_cached_asset_prices() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut cache = cache.write().await;
            cache.insert(
                "asset-a".to_string(),
                Price {
                    best_ask: Some(0.42),
                    ..Default::default()
                },
            );
            cache.insert(
                "asset-b".to_string(),
                Price {
                    best_ask: Some(0.43),
                    ..Default::default()
                },
            );
            cache.insert(
                "asset-keep".to_string(),
                Price {
                    best_ask: Some(0.44),
                    ..Default::default()
                },
            );
        }
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(
                r#"{"event_type":"market_resolved","assets_ids":["asset-a"],"clob_token_ids":["asset-b"]}"#,
            )
            .await;

        let cache = cache.read().await;
        assert!(!cache.contains_key("asset-a"));
        assert!(!cache.contains_key("asset-b"));
        assert!(cache.contains_key("asset-keep"));
    }

    #[tokio::test]
    async fn subscription_snapshot_invalidation_removes_stale_ready_cache() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut cache = cache.write().await;
            cache.insert(
                "asset-stale".to_string(),
                Price {
                    best_ask: Some(0.42),
                    snapshot_ready: true,
                    ..Default::default()
                },
            );
        }
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        let removed = client
            .remove_cached_assets(&["asset-stale".to_string()])
            .await;

        assert_eq!(removed, 1);
        assert!(!cache.read().await.contains_key("asset-stale"));
    }

    #[tokio::test]
    async fn best_ask_change_without_size_clears_stale_size() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-size","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-size","best_ask":"0.43","best_bid":"0.41"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-size").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.43));
        assert_eq!(price.best_ask_size, None);
    }

    #[tokio::test]
    async fn best_bid_ask_without_size_keeps_depth_size_for_same_price() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-same-size","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-same-size","best_ask":"0.42","best_bid":"0.41"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-same-size").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.42));
        assert_eq!(price.best_ask_size, Some(25.0));
    }

    #[tokio::test]
    async fn best_bid_ask_without_size_keeps_bid_depth_size_for_same_price() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-bid-same-size","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-bid-same-size","best_ask":"0.42","best_bid":"0.40"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-bid-same-size").expect("price inserted");
        assert_eq!(price.best_bid, Some(0.40));
        assert_eq!(price.best_bid_size, Some(30.0));
    }

    #[tokio::test]
    async fn best_bid_change_without_size_clears_stale_bid_size() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-bid-size","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.41","size":"10"},{"price":"0.40","size":"20"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-bid-size","best_ask":"0.42","best_bid":"0.39"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-bid-size").expect("price inserted");
        assert_eq!(price.best_bid, Some(0.39));
        assert_eq!(price.best_bid_size, None);
    }

    #[tokio::test]
    async fn best_bid_change_without_size_uses_depth_size_for_new_price() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-bid-depth-size","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.41","size":"10"},{"price":"0.40","size":"20"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"best_bid_ask","asset_id":"asset-bid-depth-size","best_ask":"0.42","best_bid":"0.40"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-bid-depth-size").expect("price inserted");
        assert_eq!(price.best_bid, Some(0.40));
        assert_eq!(price.best_bid_size, Some(20.0));
    }

    #[tokio::test]
    async fn zero_size_sell_price_change_removes_current_best_ask() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-remove","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-remove","price":"0.42","size":"0","side":"SELL"}]}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-remove").expect("price inserted");
        assert_eq!(price.best_ask, None);
        assert_eq!(price.best_ask_size, None);
        assert_eq!(price.best_bid, Some(0.40));
    }

    #[tokio::test]
    async fn sell_price_change_recomputes_best_ask_from_depth() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-depth","asks":[{"price":"0.42","size":"25"},{"price":"0.45","size":"10"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-depth","price":"0.41","size":"12","side":"SELL"}]}"#)
            .await;

        {
            let cache = cache.read().await;
            let price = cache.get("asset-depth").expect("price inserted");
            assert_eq!(price.best_ask, Some(0.41));
            assert_eq!(price.best_ask_size, Some(12.0));
            assert_eq!(
                price.ask_depth,
                vec![(0.41, 12.0), (0.42, 25.0), (0.45, 10.0)]
            );
        }

        client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-depth","price":"0.41","size":"0","side":"SELL"}]}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-depth").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.42));
        assert_eq!(price.best_ask_size, Some(25.0));
        assert_eq!(price.ask_depth, vec![(0.42, 25.0), (0.45, 10.0)]);
    }

    #[tokio::test]
    async fn price_change_records_depth_change() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-flow","asks":[{"price":"0.42","size":"25"}],"bids":[{"price":"0.40","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"price_change","timestamp":"1700000001000","price_changes":[{"asset_id":"asset-flow","price":"0.42","size":"5","side":"SELL"}]}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-flow").expect("price inserted");
        assert_eq!(price.ask_depth, vec![(0.42, 5.0)]);
        let change = price.recent_depth_changes.back().expect("depth change");
        assert_eq!(change.side, "ASK");
        assert_eq!(change.price, 0.42);
        assert_eq!(change.old_size, 25.0);
        assert_eq!(change.new_size, 5.0);
        assert_eq!(change.level_index, Some(0));
        assert_eq!(change.venue_timestamp_ms, Some(1_700_000_001_000));
    }

    #[tokio::test]
    async fn handle_message_updates_nested_price_changes() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-4","asks":[{"price":"0.51","size":"20"}],"bids":[{"price":"0.48","size":"30"}]}"#)
            .await;
        client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-4","price":"0.50","size":"200","side":"SELL","best_bid":"0.49","best_ask":"0.50"}]}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-4").expect("price inserted");
        assert_eq!(price.best_ask, Some(0.50));
        assert_eq!(price.best_bid, Some(0.49));
        assert_eq!(price.best_ask_size, Some(200.0));
        assert_eq!(price.ask_depth, vec![(0.50, 200.0), (0.51, 20.0)]);
    }

    #[tokio::test]
    async fn handle_message_records_last_trade_print_without_refreshing_quote() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"book","asset_id":"asset-trade","asks":[{"price":"0.51","size":"20"}],"bids":[{"price":"0.48","size":"30"}]}"#)
            .await;
        let quote_updated_at = cache
            .read()
            .await
            .get("asset-trade")
            .expect("book cache")
            .last_updated;
        client
            .handle_message(r#"{"event_type":"last_trade_price","asset_id":"asset-trade","price":"0.52","size":"12","side":"BUY","timestamp":"1700000002000"}"#)
            .await;

        let cache = cache.read().await;
        let price = cache.get("asset-trade").expect("price inserted");
        assert_eq!(price.last_updated, quote_updated_at);
        assert_eq!(price.recent_trades.len(), 1);
        let trade = price.recent_trades.front().unwrap();
        assert_eq!(trade.side, "BUY");
        assert_eq!(trade.price, 0.52);
        assert_eq!(trade.size, 12.0);
        assert_eq!(trade.venue_timestamp_ms, Some(1_700_000_002_000));
    }

    #[tokio::test]
    async fn price_change_before_book_snapshot_is_ignored() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());

        client
            .handle_message(r#"{"event_type":"price_change","price_changes":[{"asset_id":"asset-delta-first","price":"0.50","size":"200","side":"SELL","best_bid":"0.49","best_ask":"0.50"}]}"#)
            .await;

        let cache = cache.read().await;
        assert!(!cache.contains_key("asset-delta-first"));
    }

    #[tokio::test]
    async fn handle_message_ignores_invalid_payloads() {
        let cache: PriceCache = Arc::new(RwLock::new(HashMap::new()));
        let cfg = Config::from_env();
        let (client, _tx) = WsClient::new(cfg, cache.clone());
        client.handle_message("not-json").await;
        assert!(cache.read().await.is_empty());
    }
}
