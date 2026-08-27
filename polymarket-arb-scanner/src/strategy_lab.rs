use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::fees;
use crate::models::OutcomeSide;

const MAX_RETRY_BACKOFF_MS: u64 = 60_000;
const DEFAULT_TICK_SIZE: f64 = 0.01;

#[derive(Debug, Clone)]
pub struct StrategyLab {
    latest_markets: Vec<StrategyMarket>,
    accounts: HashMap<StrategyId, StrategyAccount>,
    last_refresh_instant: Option<Instant>,
    last_refresh_wall: Option<DateTime<Utc>>,
}

impl StrategyLab {
    pub fn new(config: &Config) -> Self {
        let mut accounts = HashMap::new();
        for id in ALL_STRATEGIES.iter().copied() {
            accounts.insert(
                id,
                StrategyAccount::new(config.strategy_lab_initial_capital_usd),
            );
        }
        Self {
            latest_markets: Vec::new(),
            accounts,
            last_refresh_instant: None,
            last_refresh_wall: None,
        }
    }

    pub fn should_refresh(&self, config: &Config) -> bool {
        match self.last_refresh_instant {
            None => true,
            Some(last) => {
                last.elapsed()
                    >= Duration::from_secs(config.strategy_lab_refresh_interval_secs.max(1))
            }
        }
    }

    pub async fn maybe_refresh(&mut self, client: &Client, config: &Config) -> Option<String> {
        if !self.should_refresh(config) {
            return None;
        }

        let markets = match fetch_strategy_markets(client, config).await {
            Ok(markets) => markets,
            Err(err) => {
                warn!("Strategy lab refresh failed: {err}");
                return Some(format!("strategy lab refresh failed: {err}"));
            }
        };

        if markets.is_empty() {
            warn!("Strategy lab refresh returned no liquid active binary markets");
            return Some(
                "strategy lab refresh returned no liquid active binary markets".to_string(),
            );
        }

        self.latest_markets = markets;
        self.last_refresh_instant = Some(Instant::now());
        self.last_refresh_wall = Some(Utc::now());
        let note = self.advance(config);
        Some(note)
    }

    pub fn print_summary(&self) {
        info!("Strategy lab summary");
        info!("  Market universe: {}", self.latest_markets.len());
        if let Some(ts) = self.last_refresh_wall {
            info!("  Last refresh:   {}", ts.to_rfc3339());
        }

        let mut rows: Vec<(StrategyId, f64)> = ALL_STRATEGIES
            .iter()
            .copied()
            .map(|id| {
                (
                    id,
                    self.accounts
                        .get(&id)
                        .map(|acc| acc.total_pnl())
                        .unwrap_or(0.0),
                )
            })
            .collect();
        rows.sort_by(|a, b| b.1.total_cmp(&a.1));

        for (id, _) in rows.iter().take(8) {
            let def = definition(*id);
            if let Some(account) = self.accounts.get(id) {
                info!(
                    "  {:<5} {:<20} closed={} open={} pending={} realized={:+.2} unrealized={:+.2} total={:+.2} win={:.1}%",
                    id.short_code(),
                    def.name,
                    account.trades_closed,
                    account.open_positions.len(),
                    account.pending_orders.len(),
                    account.realized_pnl_usd,
                    account.unrealized_pnl_usd,
                    account.total_pnl(),
                    account.win_rate_pct(),
                );
            }
        }
    }

    fn advance(&mut self, config: &Config) -> String {
        let now = Utc::now();
        let market_map: HashMap<String, StrategyMarket> = self
            .latest_markets
            .iter()
            .cloned()
            .map(|market| (market.market_id.clone(), market))
            .collect();

        let markets_ref = &self.latest_markets;
        let market_map_ref = &market_map;

        let results: Vec<(StrategyId, StrategyAccount, usize, usize, usize)> = ALL_STRATEGIES
            .par_iter()
            .copied()
            .filter_map(|id| {
                let account = self.accounts.get(&id)?.clone();
                let mut account = account;

                let closed =
                    settle_existing_positions(id, &mut account, market_map_ref, config, now);
                let order_fills =
                    process_pending_orders(id, &mut account, market_map_ref, config, now);
                update_unrealized(&mut account, market_map_ref, config);

                let held_markets = account.active_market_ids();
                let mut candidates = rank_candidates(id, markets_ref, &held_markets, config, now);

                let mut opened = 0usize;
                while account.open_positions.len() + account.pending_orders.len()
                    < config.strategy_lab_max_positions_per_strategy
                    && !candidates.is_empty()
                {
                    let candidate = candidates.remove(0);
                    if account.is_market_active(&candidate.market_id) {
                        continue;
                    }
                    if let Some(event) =
                        submit_candidate(id, &mut account, candidate, market_map_ref, config, now)
                    {
                        match event {
                            SubmissionOutcome::Opened(text) => {
                                opened += 1;
                                account.push_event(text);
                            }
                            SubmissionOutcome::Queued(text) => {
                                account.push_event(text);
                            }
                        }
                    }
                }

                update_unrealized(&mut account, market_map_ref, config);

                Some((id, account, opened, closed, order_fills))
            })
            .collect();

        let mut opened = 0usize;
        let mut closed = 0usize;
        let mut order_fills = 0usize;

        for (id, account, o, c, f) in results {
            self.accounts.insert(id, account);
            opened += o;
            closed += c;
            order_fills += f;
        }

        debug!(
            "Strategy lab refresh: markets={} opened={} closed={} order_fills={}",
            self.latest_markets.len(),
            opened,
            closed,
            order_fills,
        );

        format!(
            "strategy lab refresh: {} markets | opened {} | closed {} | maker fills {}",
            self.latest_markets.len(),
            opened,
            closed,
            order_fills,
        )
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    market_id: String,
    side: OutcomeSide,
    score: f64,
    entry_price: f64,
    label: String,
    reason: String,
    mode: CandidateMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateMode {
    Taker,
    MakerBid,
}

#[derive(Debug, Clone)]
struct StrategyAccount {
    cash_usd: f64,
    realized_pnl_usd: f64,
    unrealized_pnl_usd: f64,
    trades_opened: usize,
    trades_closed: usize,
    wins: usize,
    losses: usize,
    open_positions: Vec<PaperPosition>,
    pending_orders: Vec<PendingOrder>,
    recent_events: Vec<String>,
}

impl Default for StrategyAccount {
    fn default() -> Self {
        Self::new(0.0)
    }
}

impl StrategyAccount {
    fn new(initial_cash_usd: f64) -> Self {
        Self {
            cash_usd: initial_cash_usd,
            realized_pnl_usd: 0.0,
            unrealized_pnl_usd: 0.0,
            trades_opened: 0,
            trades_closed: 0,
            wins: 0,
            losses: 0,
            open_positions: Vec::new(),
            pending_orders: Vec::new(),
            recent_events: Vec::new(),
        }
    }

    fn reserved_cash_usd(&self) -> f64 {
        self.pending_orders
            .iter()
            .map(|order| order.price * order.shares)
            .sum()
    }

    fn available_cash_usd(&self) -> f64 {
        (self.cash_usd - self.reserved_cash_usd()).max(0.0)
    }

    fn win_rate_pct(&self) -> f64 {
        if self.trades_closed == 0 {
            0.0
        } else {
            (self.wins as f64 / self.trades_closed as f64) * 100.0
        }
    }

    fn total_pnl(&self) -> f64 {
        self.realized_pnl_usd + self.unrealized_pnl_usd
    }

    fn push_event(&mut self, event: String) {
        if event.trim().is_empty() {
            return;
        }
        if self
            .recent_events
            .last()
            .map(|last| last == &event)
            .unwrap_or(false)
        {
            return;
        }
        self.recent_events.push(event);
        if self.recent_events.len() > 24 {
            let overflow = self.recent_events.len().saturating_sub(24);
            self.recent_events.drain(0..overflow);
        }
    }

    fn active_market_ids(&self) -> HashSet<String> {
        self.open_positions
            .iter()
            .map(|position| position.market_id.clone())
            .chain(
                self.pending_orders
                    .iter()
                    .map(|order| order.market_id.clone()),
            )
            .collect()
    }

    fn is_market_active(&self, market_id: &str) -> bool {
        self.open_positions
            .iter()
            .any(|position| position.market_id == market_id)
            || self
                .pending_orders
                .iter()
                .any(|order| order.market_id == market_id)
    }
}

#[derive(Debug, Clone)]
struct PaperPosition {
    market_id: String,
    question: String,
    side: OutcomeSide,
    entry_price: f64,
    shares: f64,
    entry_fee_usd: f64,
    opened_at: DateTime<Utc>,
    max_hold_secs: i64,
    take_profit_pct: f64,
    stop_loss_pct: f64,
    last_unrealized_pnl_usd: f64,
}

#[derive(Debug, Clone)]
struct PendingOrder {
    market_id: String,
    question: String,
    side: OutcomeSide,
    price: f64,
    shares: f64,
    expires_at: DateTime<Utc>,
    max_hold_secs: i64,
    take_profit_pct: f64,
    stop_loss_pct: f64,
}

#[derive(Debug, Clone)]
enum SubmissionOutcome {
    Opened(String),
    Queued(String),
}

fn settle_existing_positions(
    strategy_id: StrategyId,
    account: &mut StrategyAccount,
    market_map: &HashMap<String, StrategyMarket>,
    config: &Config,
    now: DateTime<Utc>,
) -> usize {
    let mut closed = 0usize;
    let mut survivors = Vec::with_capacity(account.open_positions.len());
    let mut events = Vec::new();

    for mut position in account.open_positions.drain(..) {
        let Some(market) = market_map.get(&position.market_id) else {
            survivors.push(position);
            continue;
        };

        let conservative_bid = market
            .side_bid(position.side)
            .unwrap_or_else(|| market.side_mid(position.side) - market.estimated_spread() / 2.0)
            .clamp(0.001, 0.999);
        let exit_fee = fees::fee_per_share(conservative_bid, market.effective_fee_rate(config))
            * position.shares;
        let gross_pnl = (conservative_bid - position.entry_price) * position.shares;
        let net_pnl = gross_pnl - position.entry_fee_usd - exit_fee;
        position.last_unrealized_pnl_usd = net_pnl;
        let return_pct = if position.entry_price > 0.0 {
            (conservative_bid - position.entry_price) / position.entry_price
        } else {
            0.0
        };
        let age_secs = (now - position.opened_at).num_seconds();
        let hours_to_close = market.hours_to_close(now).unwrap_or(9_999.0);
        let reverse = strategy_reverse_signal(strategy_id, market);

        let exit_reason = if return_pct >= position.take_profit_pct {
            Some("take-profit")
        } else if return_pct <= -position.stop_loss_pct {
            Some("stop-loss")
        } else if age_secs >= position.max_hold_secs {
            Some("time-stop")
        } else if hours_to_close <= 0.25 {
            Some("pre-close")
        } else if reverse {
            Some("signal-reversal")
        } else {
            None
        };

        if let Some(reason) = exit_reason {
            let proceeds = conservative_bid * position.shares;
            account.cash_usd += proceeds - exit_fee;
            account.realized_pnl_usd += net_pnl;
            account.trades_closed += 1;
            if net_pnl >= 0.0 {
                account.wins += 1;
            } else {
                account.losses += 1;
            }
            events.push(format!(
                "exit {} {} '{}' @ {:.3} ({}) pnl {:+.2}",
                strategy_id.short_code(),
                position.side.as_str().to_ascii_uppercase(),
                short_text(&position.question, 40),
                conservative_bid,
                reason,
                net_pnl,
            ));
            closed += 1;
        } else {
            survivors.push(position);
        }
    }

    account.open_positions = survivors;
    for event in events {
        account.push_event(event);
    }
    closed
}

fn process_pending_orders(
    strategy_id: StrategyId,
    account: &mut StrategyAccount,
    market_map: &HashMap<String, StrategyMarket>,
    _config: &Config,
    now: DateTime<Utc>,
) -> usize {
    let mut fills = 0usize;
    let mut survivors = Vec::with_capacity(account.pending_orders.len());
    let mut events = Vec::new();

    for order in account.pending_orders.drain(..) {
        let Some(market) = market_map.get(&order.market_id) else {
            continue;
        };

        let ask = market
            .side_ask(order.side)
            .unwrap_or_else(|| market.side_mid(order.side) + market.estimated_spread() / 2.0);
        let spread = market
            .side_spread(order.side)
            .unwrap_or_else(|| market.estimated_spread());
        let expired = now >= order.expires_at;
        let reverse = strategy_reverse_signal(strategy_id, market);

        if ask <= order.price && spread >= 0.01 {
            let total_cost = order.price * order.shares;
            if total_cost > account.cash_usd {
                events.push(format!(
                    "skip fill {} {} '{}' (cash shortfall)",
                    strategy_id.short_code(),
                    order.side.as_str().to_ascii_uppercase(),
                    short_text(&order.question, 36),
                ));
                continue;
            }
            account.cash_usd -= total_cost;
            account.open_positions.push(PaperPosition {
                market_id: order.market_id.clone(),
                question: order.question.clone(),
                side: order.side,
                entry_price: order.price,
                shares: order.shares,
                entry_fee_usd: 0.0,
                opened_at: now,
                max_hold_secs: order.max_hold_secs,
                take_profit_pct: order.take_profit_pct,
                stop_loss_pct: order.stop_loss_pct,
                last_unrealized_pnl_usd: 0.0,
            });
            account.trades_opened += 1;
            events.push(format!(
                "fill {} {} maker bid '{}' @ {:.3}",
                strategy_id.short_code(),
                order.side.as_str().to_ascii_uppercase(),
                short_text(&order.question, 40),
                order.price,
            ));
            fills += 1;
            continue;
        }

        if expired || reverse {
            events.push(format!(
                "cancel {} {} maker bid '{}' ({})",
                strategy_id.short_code(),
                order.side.as_str().to_ascii_uppercase(),
                short_text(&order.question, 40),
                if expired { "expired" } else { "reversal" },
            ));
            continue;
        }

        survivors.push(order);
    }

    account.pending_orders = survivors;
    for event in events {
        account.push_event(event);
    }
    fills
}

fn update_unrealized(
    account: &mut StrategyAccount,
    market_map: &HashMap<String, StrategyMarket>,
    config: &Config,
) {
    let mut unrealized = 0.0;
    for position in account.open_positions.iter_mut() {
        if let Some(market) = market_map.get(&position.market_id) {
            let bid = market
                .side_bid(position.side)
                .unwrap_or_else(|| market.side_mid(position.side) - market.estimated_spread() / 2.0)
                .clamp(0.001, 0.999);
            let exit_fee =
                fees::fee_per_share(bid, market.effective_fee_rate(config)) * position.shares;
            let gross_pnl = (bid - position.entry_price) * position.shares;
            let net_pnl = gross_pnl - position.entry_fee_usd - exit_fee;
            position.last_unrealized_pnl_usd = net_pnl;
            unrealized += net_pnl;
        }
    }
    account.unrealized_pnl_usd = unrealized;
}

fn submit_candidate(
    strategy_id: StrategyId,
    account: &mut StrategyAccount,
    candidate: Candidate,
    market_map: &HashMap<String, StrategyMarket>,
    config: &Config,
    now: DateTime<Utc>,
) -> Option<SubmissionOutcome> {
    let market = market_map.get(&candidate.market_id)?;
    let notional = position_notional_usd(account, market, config)?;
    let shares = round_down(notional / candidate.entry_price.max(0.0001), 0.0001);
    if shares <= 0.0 {
        return None;
    }

    let def = definition(strategy_id);
    match candidate.mode {
        CandidateMode::Taker => {
            let entry_fee =
                fees::fee_per_share(candidate.entry_price, market.effective_fee_rate(config))
                    * shares;
            let total_cost = candidate.entry_price * shares + entry_fee;
            if total_cost > account.available_cash_usd() {
                return None;
            }
            account.cash_usd -= total_cost;
            account.open_positions.push(PaperPosition {
                market_id: candidate.market_id.clone(),
                question: candidate.label.clone(),
                side: candidate.side,
                entry_price: candidate.entry_price,
                shares,
                entry_fee_usd: entry_fee,
                opened_at: now,
                max_hold_secs: def.max_hold_secs,
                take_profit_pct: def.take_profit_pct,
                stop_loss_pct: def.stop_loss_pct,
                last_unrealized_pnl_usd: 0.0,
            });
            account.trades_opened += 1;
            Some(SubmissionOutcome::Opened(format!(
                "enter {} {} '{}' @ {:.3} | {:.2} sh | {}",
                strategy_id.short_code(),
                candidate.side.as_str().to_ascii_uppercase(),
                short_text(&candidate.label, 40),
                candidate.entry_price,
                shares,
                short_text(&candidate.reason, 56),
            )))
        }
        CandidateMode::MakerBid => {
            let reserved = candidate.entry_price * shares;
            if reserved > account.available_cash_usd() {
                return None;
            }
            account.pending_orders.push(PendingOrder {
                market_id: candidate.market_id.clone(),
                question: candidate.label.clone(),
                side: candidate.side,
                price: candidate.entry_price,
                shares,
                expires_at: now + chrono::Duration::seconds(def.order_ttl_secs),
                max_hold_secs: def.max_hold_secs,
                take_profit_pct: def.take_profit_pct,
                stop_loss_pct: def.stop_loss_pct,
            });
            Some(SubmissionOutcome::Queued(format!(
                "queue {} {} maker bid '{}' @ {:.3} | {:.2} sh | {}",
                strategy_id.short_code(),
                candidate.side.as_str().to_ascii_uppercase(),
                short_text(&candidate.label, 40),
                candidate.entry_price,
                shares,
                short_text(&candidate.reason, 56),
            )))
        }
    }
}

fn position_notional_usd(
    account: &StrategyAccount,
    market: &StrategyMarket,
    config: &Config,
) -> Option<f64> {
    let liquidity_cap =
        (market.liquidity * 0.002).clamp(5.0, config.strategy_lab_position_size_usd);
    let volume_cap =
        (market.volume24hr.max(1.0) * 0.005).clamp(5.0, config.strategy_lab_position_size_usd);
    let cash_cap = (account.available_cash_usd() * 0.40).max(0.0);
    let notional = config
        .strategy_lab_position_size_usd
        .min(liquidity_cap)
        .min(volume_cap)
        .min(cash_cap);
    if notional >= 5.0 {
        Some(round_down(notional, 0.01))
    } else {
        None
    }
}

fn rank_candidates(
    strategy_id: StrategyId,
    markets: &[StrategyMarket],
    held_markets: &HashSet<String>,
    config: &Config,
    now: DateTime<Utc>,
) -> Vec<Candidate> {
    let mut candidates: Vec<Candidate> = markets
        .iter()
        .filter(|market| !held_markets.contains(&market.market_id))
        .filter_map(|market| score_candidate(strategy_id, market, config, now))
        .collect();
    candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
    candidates.truncate(config.strategy_lab_candidate_cap_per_strategy);
    candidates
}

fn score_candidate(
    strategy_id: StrategyId,
    market: &StrategyMarket,
    config: &Config,
    now: DateTime<Utc>,
) -> Option<Candidate> {
    if market.closed || !market.accepting_orders {
        return None;
    }

    let yes_mid = market.yes_mid();
    let no_mid = market.no_mid();
    let yes_spread = market
        .yes_spread()
        .unwrap_or_else(|| market.estimated_spread());
    let no_spread = market.no_spread().unwrap_or(yes_spread);
    let hours_to_close = market.hours_to_close(now).unwrap_or(24.0 * 365.0);
    let one_hour_change = market.one_hour_change;
    let one_day_change = market.one_day_change;
    let fee_rate = market.effective_fee_rate(config);

    match strategy_id {
        StrategyId::AnchorYes => {
            let ask = market
                .side_ask(OutcomeSide::Yes)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::Yes) + yes_spread / 2.0);
            let gap = market.gamma_yes_price - ask;
            let fee_drag = fees::fee_per_share(ask, fee_rate) * 2.0;
            let edge = gap - fee_drag - yes_spread * 0.15;
            if gap < 0.04
                || edge < 0.01
                || !(0.15..=0.85).contains(&yes_mid)
                || yes_spread > 0.04
                || market.liquidity < 10_000.0
                || market.volume24hr < 2_500.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: edge + market.liquidity.sqrt() * 0.0001,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "gamma gap {:.1}c | spr {:.1}c | 1h {:+.1}c | fee {:.2}%",
                    gap * 100.0,
                    yes_spread * 100.0,
                    one_hour_change * 100.0,
                    fee_rate * 100.0
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::AnchorNo => {
            let ask = market
                .side_ask(OutcomeSide::No)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::No) + no_spread / 2.0);
            let gap = market.gamma_no_price - ask;
            let fee_drag = fees::fee_per_share(ask, fee_rate) * 2.0;
            let edge = gap - fee_drag - no_spread * 0.15;
            if gap < 0.04
                || edge < 0.01
                || !(0.15..=0.85).contains(&no_mid)
                || no_spread > 0.04
                || market.liquidity < 10_000.0
                || market.volume24hr < 2_500.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: edge + market.liquidity.sqrt() * 0.0001,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "gamma NO gap {:.1}c | spr {:.1}c | 1h {:+.1}c | fee {:.2}%",
                    gap * 100.0,
                    no_spread * 100.0,
                    (-one_hour_change) * 100.0,
                    fee_rate * 100.0
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::MomentumYes => {
            let ask = market.side_ask(OutcomeSide::Yes)?;
            if one_hour_change < 0.05
                || !(0.20..=0.80).contains(&yes_mid)
                || yes_spread > 0.03
                || market.volume24hr < 5_000.0
                || market.liquidity < 15_000.0
                || hours_to_close < 1.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: one_hour_change + one_day_change.max(0.0) * 0.25 - yes_spread * 0.50,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "1h mom {:+.1}c | 1d {:+.1}c | spr {:.1}c | close {:.1}h",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    yes_spread * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::MomentumNo => {
            let ask = market.side_ask(OutcomeSide::No)?;
            if one_hour_change > -0.05
                || !(0.20..=0.80).contains(&no_mid)
                || no_spread > 0.03
                || market.volume24hr < 5_000.0
                || market.liquidity < 15_000.0
                || hours_to_close < 1.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: (-one_hour_change) + (-one_day_change).max(0.0) * 0.25 - no_spread * 0.50,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "1h down {:+.1}c | 1d {:+.1}c | spr {:.1}c | close {:.1}h",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    no_spread * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::MeanRevertYes => {
            let ask = market
                .side_ask(OutcomeSide::Yes)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::Yes) + yes_spread / 2.0);
            if one_hour_change > -0.08
                || !(0.10..=0.45).contains(&yes_mid)
                || yes_spread > 0.06
                || market.volume24hr < 5_000.0
                || market.liquidity < 12_000.0
                || hours_to_close < 6.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: (-one_hour_change)
                    - yes_spread * 0.35
                    - one_day_change.min(0.0).abs() * 0.10,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "fade {:+.1}c 1h dump | spr {:.1}c | close {:.1}h",
                    one_hour_change * 100.0,
                    yes_spread * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::MeanRevertNo => {
            let ask = market
                .side_ask(OutcomeSide::No)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::No) + no_spread / 2.0);
            if one_hour_change < 0.08
                || !(0.55..=0.90).contains(&yes_mid)
                || no_spread > 0.06
                || market.volume24hr < 5_000.0
                || market.liquidity < 12_000.0
                || hours_to_close < 6.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: one_hour_change - no_spread * 0.35 - one_day_change.max(0.0) * 0.10,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "fade {:+.1}c 1h spike | spr {:.1}c | close {:.1}h",
                    one_hour_change * 100.0,
                    no_spread * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::FavoriteYes => {
            let ask = market.side_ask(OutcomeSide::Yes)?;
            if !(0.75..=0.93).contains(&yes_mid)
                || yes_spread > 0.02
                || market.liquidity < 20_000.0
                || market.volume24hr < 10_000.0
                || hours_to_close > 24.0 * 14.0
                || one_hour_change < -0.02
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: (yes_mid - 0.75) * 0.7
                    + (0.02 - yes_spread) * 2.0
                    + one_hour_change.max(-0.02),
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "favorite {:.1}c | spr {:.1}c | 1h {:+.1}c | close {:.1}h",
                    yes_mid * 100.0,
                    yes_spread * 100.0,
                    one_hour_change * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::LongshotNo => {
            let ask = market.side_ask(OutcomeSide::No)?;
            if !(0.03..=0.15).contains(&yes_mid)
                || no_spread > 0.03
                || market.liquidity < 15_000.0
                || market.volume24hr < 5_000.0
                || hours_to_close > 24.0 * 14.0
                || one_hour_change > 0.02
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: (0.15 - yes_mid) * 0.8
                    + (0.03 - no_spread) * 1.5
                    + (-one_hour_change).max(-0.02),
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "longshot yes {:.1}c | NO spr {:.1}c | 1h {:+.1}c | close {:.1}h",
                    yes_mid * 100.0,
                    no_spread * 100.0,
                    one_hour_change * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::ExpiryFavoriteYes => {
            let ask = market.side_ask(OutcomeSide::Yes)?;
            if !(0.82..=0.98).contains(&yes_mid)
                || yes_spread > 0.015
                || market.volume24hr < 15_000.0
                || !(0.50..=48.0).contains(&hours_to_close)
                || (one_hour_change < 0.0 && one_day_change < 0.0)
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: (yes_mid - 0.82) + (48.0 - hours_to_close) / 96.0 + one_hour_change.max(0.0),
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "near-exp fav {:.1}c | spr {:.1}c | 1h {:+.1}c | close {:.1}h",
                    yes_mid * 100.0,
                    yes_spread * 100.0,
                    one_hour_change * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::ExpiryLongshotNo => {
            let ask = market.side_ask(OutcomeSide::No)?;
            if !(0.02..=0.18).contains(&yes_mid)
                || no_spread > 0.015
                || market.volume24hr < 15_000.0
                || !(0.50..=48.0).contains(&hours_to_close)
                || (one_hour_change > 0.0 && one_day_change > 0.0)
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: (0.18 - yes_mid)
                    + (48.0 - hours_to_close) / 96.0
                    + (-one_hour_change).max(0.0),
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "near-exp longshot {:.1}c | spr {:.1}c | 1h {:+.1}c | close {:.1}h",
                    yes_mid * 100.0,
                    no_spread * 100.0,
                    one_hour_change * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::MakerYes => {
            let bid = market.side_bid(OutcomeSide::Yes)?;
            let ask = market.side_ask(OutcomeSide::Yes)?;
            let spread = (ask - bid).max(0.0);
            if !(0.03..=0.10).contains(&spread)
                || one_hour_change.abs() > 0.02
                || !(0.25..=0.75).contains(&yes_mid)
                || market.liquidity < 25_000.0
                || market.volume24hr < 10_000.0
            {
                return None;
            }
            let tick = market.tick_size();
            let price = (ask - tick)
                .min(bid + tick.max(spread * 0.25))
                .max(bid + tick.min(spread / 2.0));
            if price >= ask {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: spread - one_hour_change.abs() * 0.5 + market.volume24hr.sqrt() * 0.0001,
                entry_price: round_down(price, tick),
                label: market.question.clone(),
                reason: format!(
                    "maker YES spread {:.1}c | 1h {:+.1}c | mid {:.1}c",
                    spread * 100.0,
                    one_hour_change * 100.0,
                    yes_mid * 100.0
                ),
                mode: CandidateMode::MakerBid,
            })
        }
        StrategyId::MakerNo => {
            let bid = market.side_bid(OutcomeSide::No)?;
            let ask = market.side_ask(OutcomeSide::No)?;
            let spread = (ask - bid).max(0.0);
            if !(0.03..=0.10).contains(&spread)
                || one_hour_change.abs() > 0.02
                || !(0.25..=0.75).contains(&no_mid)
                || market.liquidity < 25_000.0
                || market.volume24hr < 10_000.0
            {
                return None;
            }
            let tick = market.tick_size();
            let price = (ask - tick)
                .min(bid + tick.max(spread * 0.25))
                .max(bid + tick.min(spread / 2.0));
            if price >= ask {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: spread - one_hour_change.abs() * 0.5 + market.volume24hr.sqrt() * 0.0001,
                entry_price: round_down(price, tick),
                label: market.question.clone(),
                reason: format!(
                    "maker NO spread {:.1}c | 1h {:+.1}c | mid {:.1}c",
                    spread * 100.0,
                    (-one_hour_change) * 100.0,
                    no_mid * 100.0
                ),
                mode: CandidateMode::MakerBid,
            })
        }
        StrategyId::VolumeMomentumYes => {
            let ask = market.side_ask(OutcomeSide::Yes)?;
            if one_hour_change < 0.04
                || one_day_change < 0.02
                || !(0.20..=0.80).contains(&yes_mid)
                || yes_spread > 0.03
                || market.volume24hr < 20_000.0
                || market.liquidity < 15_000.0
                || hours_to_close < 1.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: one_hour_change + one_day_change * 0.5 - yes_spread * 0.40,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "vol mom 1h {:+.1}c 1d {:+.1}c | vol {:.0} | spr {:.1}c",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    market.volume24hr,
                    yes_spread * 100.0
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::VolumeMomentumNo => {
            let ask = market.side_ask(OutcomeSide::No)?;
            if one_hour_change > -0.04
                || one_day_change > -0.02
                || !(0.20..=0.80).contains(&no_mid)
                || no_spread > 0.03
                || market.volume24hr < 20_000.0
                || market.liquidity < 15_000.0
                || hours_to_close < 1.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: (-one_hour_change) + (-one_day_change) * 0.5 - no_spread * 0.40,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "vol mom 1h {:+.1}c 1d {:+.1}c | vol {:.0} | spr {:.1}c",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    market.volume24hr,
                    no_spread * 100.0
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::GammaCarryYes => {
            let ask = market
                .side_ask(OutcomeSide::Yes)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::Yes) + yes_spread / 2.0);
            let gap = market.gamma_yes_price - ask;
            if !(0.02..0.06).contains(&gap)
                || one_hour_change < 0.0
                || !(0.20..=0.75).contains(&yes_mid)
                || yes_spread > 0.04
                || market.liquidity < 10_000.0
                || market.volume24hr < 2_500.0
            {
                return None;
            }
            let fee_drag = fees::fee_per_share(ask, fee_rate) * 2.0;
            let edge = gap - fee_drag - yes_spread * 0.15;
            if edge < 0.005 {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: edge * 0.7 + market.liquidity.sqrt() * 0.00008,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "carry gap {:.1}c | spr {:.1}c | 1h {:+.1}c | fee {:.2}%",
                    gap * 100.0,
                    yes_spread * 100.0,
                    one_hour_change * 100.0,
                    fee_rate * 100.0
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::DivergenceFadeYes => {
            let ask = market
                .side_ask(OutcomeSide::Yes)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::Yes) + yes_spread / 2.0);
            if one_hour_change < 0.05
                || one_day_change > -0.02
                || !(0.15..=0.70).contains(&yes_mid)
                || yes_spread > 0.05
                || market.volume24hr < 5_000.0
                || market.liquidity < 8_000.0
                || hours_to_close < 2.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: (-one_day_change) * 0.8 + one_hour_change * 0.3 - yes_spread * 0.30,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "fade 1h {:+.1}c vs 1d {:+.1}c | spr {:.1}c | close {:.1}h",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    yes_spread * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::DivergenceFadeNo => {
            let ask = market
                .side_ask(OutcomeSide::No)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::No) + no_spread / 2.0);
            if one_hour_change > -0.05
                || one_day_change < 0.02
                || !(0.15..=0.70).contains(&no_mid)
                || no_spread > 0.05
                || market.volume24hr < 5_000.0
                || market.liquidity < 8_000.0
                || hours_to_close < 2.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: one_day_change * 0.8 + (-one_hour_change) * 0.3 - no_spread * 0.30,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "fade 1h {:+.1}c vs 1d {:+.1}c | spr {:.1}c | close {:.1}h",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    no_spread * 100.0,
                    hours_to_close
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::GammaCarryNo => {
            let ask = market
                .side_ask(OutcomeSide::No)
                .unwrap_or_else(|| market.side_mid(OutcomeSide::No) + no_spread / 2.0);
            let gap = market.gamma_no_price - ask;
            if !(0.02..0.06).contains(&gap)
                || one_hour_change > 0.0
                || !(0.20..=0.75).contains(&no_mid)
                || no_spread > 0.04
                || market.liquidity < 10_000.0
                || market.volume24hr < 2_500.0
            {
                return None;
            }
            let fee_drag = fees::fee_per_share(ask, fee_rate) * 2.0;
            let edge = gap - fee_drag - no_spread * 0.15;
            if edge < 0.005 {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: edge * 0.7 + market.liquidity.sqrt() * 0.00008,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "carry NO gap {:.1}c | spr {:.1}c | 1h {:+.1}c | fee {:.2}%",
                    gap * 100.0,
                    no_spread * 100.0,
                    (-one_hour_change) * 100.0,
                    fee_rate * 100.0
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::SlowGrindYes => {
            let ask = market.side_ask(OutcomeSide::Yes)?;
            if !(0.01..=0.06).contains(&one_hour_change)
                || one_day_change < 0.01
                || !(0.25..=0.75).contains(&yes_mid)
                || yes_spread > 0.02
                || market.volume24hr < 25_000.0
                || market.liquidity < 25_000.0
                || hours_to_close < 1.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::Yes,
                score: one_hour_change * 0.6 + one_day_change.max(0.0) * 0.3 - yes_spread * 0.50,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "grind 1h {:+.1}c 1d {:+.1}c | spr {:.1}c | liq {:.0}",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    yes_spread * 100.0,
                    market.liquidity
                ),
                mode: CandidateMode::Taker,
            })
        }
        StrategyId::SlowGrindNo => {
            let ask = market.side_ask(OutcomeSide::No)?;
            if !(-0.06..=-0.01).contains(&one_hour_change)
                || one_day_change > -0.01
                || !(0.25..=0.75).contains(&no_mid)
                || no_spread > 0.02
                || market.volume24hr < 25_000.0
                || market.liquidity < 25_000.0
                || hours_to_close < 1.0
            {
                return None;
            }
            Some(Candidate {
                market_id: market.market_id.clone(),
                side: OutcomeSide::No,
                score: (-one_hour_change) * 0.6 + (-one_day_change).max(0.0) * 0.3
                    - no_spread * 0.50,
                entry_price: ask,
                label: market.question.clone(),
                reason: format!(
                    "grind 1h {:+.1}c 1d {:+.1}c | spr {:.1}c | liq {:.0}",
                    one_hour_change * 100.0,
                    one_day_change * 100.0,
                    no_spread * 100.0,
                    market.liquidity
                ),
                mode: CandidateMode::Taker,
            })
        }
    }
}

fn strategy_reverse_signal(strategy_id: StrategyId, market: &StrategyMarket) -> bool {
    let hours_to_close = market.hours_to_close(Utc::now()).unwrap_or(9_999.0);
    match strategy_id {
        StrategyId::AnchorYes => {
            market.gamma_yes_price - market.side_mid(OutcomeSide::Yes) < 0.005
                || market.one_hour_change < -0.03
        }
        StrategyId::AnchorNo => {
            market.gamma_no_price - market.side_mid(OutcomeSide::No) < 0.005
                || market.one_hour_change > 0.03
        }
        StrategyId::MomentumYes => market.one_hour_change <= -0.02,
        StrategyId::MomentumNo => market.one_hour_change >= 0.02,
        StrategyId::MeanRevertYes => market.one_hour_change >= -0.01 || market.yes_mid() >= 0.50,
        StrategyId::MeanRevertNo => market.one_hour_change <= 0.01 || market.yes_mid() <= 0.50,
        StrategyId::FavoriteYes => market.yes_mid() < 0.72 || market.one_hour_change < -0.03,
        StrategyId::LongshotNo => market.yes_mid() > 0.20 || market.one_hour_change > 0.05,
        StrategyId::ExpiryFavoriteYes => market.yes_mid() < 0.78 || hours_to_close < 0.25,
        StrategyId::ExpiryLongshotNo => market.yes_mid() > 0.22 || hours_to_close < 0.25,
        StrategyId::MakerYes | StrategyId::MakerNo => {
            market.estimated_spread() < 0.01 || market.one_hour_change.abs() > 0.04
        }
        StrategyId::VolumeMomentumYes => {
            market.one_hour_change <= -0.02 || market.volume24hr < 10_000.0
        }
        StrategyId::VolumeMomentumNo => {
            market.one_hour_change >= 0.02 || market.volume24hr < 10_000.0
        }
        StrategyId::GammaCarryYes => {
            market.gamma_yes_price - market.side_mid(OutcomeSide::Yes) < 0.005
                || market.one_hour_change < -0.015
        }
        StrategyId::GammaCarryNo => {
            market.gamma_no_price - market.side_mid(OutcomeSide::No) < 0.005
                || market.one_hour_change > 0.015
        }
        StrategyId::DivergenceFadeYes => market.one_day_change > 0.0 || market.yes_mid() > 0.75,
        StrategyId::DivergenceFadeNo => market.one_day_change < 0.0 || market.yes_mid() < 0.25,
        StrategyId::SlowGrindYes => {
            market.one_hour_change < -0.005
                || market.one_day_change < -0.005
                || market.yes_mid() > 0.80
                || market.yes_mid() < 0.20
        }
        StrategyId::SlowGrindNo => {
            market.one_hour_change > 0.005
                || market.one_day_change > 0.005
                || market.yes_mid() > 0.80
                || market.yes_mid() < 0.20
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StrategyId {
    AnchorYes,
    AnchorNo,
    MomentumYes,
    MomentumNo,
    MeanRevertYes,
    MeanRevertNo,
    FavoriteYes,
    LongshotNo,
    ExpiryFavoriteYes,
    ExpiryLongshotNo,
    MakerYes,
    MakerNo,
    VolumeMomentumYes,
    VolumeMomentumNo,
    GammaCarryYes,
    GammaCarryNo,
    DivergenceFadeYes,
    DivergenceFadeNo,
    SlowGrindYes,
    SlowGrindNo,
}

impl StrategyId {
    fn short_code(self) -> &'static str {
        match self {
            StrategyId::AnchorYes => "FVY",
            StrategyId::AnchorNo => "FVN",
            StrategyId::MomentumYes => "MOMY",
            StrategyId::MomentumNo => "MOMN",
            StrategyId::MeanRevertYes => "MRY",
            StrategyId::MeanRevertNo => "MRN",
            StrategyId::FavoriteYes => "FAVY",
            StrategyId::LongshotNo => "LSNO",
            StrategyId::ExpiryFavoriteYes => "EXPY",
            StrategyId::ExpiryLongshotNo => "EXPN",
            StrategyId::MakerYes => "MKY",
            StrategyId::MakerNo => "MKN",
            StrategyId::VolumeMomentumYes => "VMY",
            StrategyId::VolumeMomentumNo => "VMN",
            StrategyId::GammaCarryYes => "GCY",
            StrategyId::GammaCarryNo => "GCN",
            StrategyId::DivergenceFadeYes => "DVY",
            StrategyId::DivergenceFadeNo => "DVN",
            StrategyId::SlowGrindYes => "SGY",
            StrategyId::SlowGrindNo => "SGN",
        }
    }
}

impl fmt::Display for StrategyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short_code())
    }
}

const ALL_STRATEGIES: [StrategyId; 20] = [
    StrategyId::AnchorYes,
    StrategyId::AnchorNo,
    StrategyId::MomentumYes,
    StrategyId::MomentumNo,
    StrategyId::MeanRevertYes,
    StrategyId::MeanRevertNo,
    StrategyId::FavoriteYes,
    StrategyId::LongshotNo,
    StrategyId::ExpiryFavoriteYes,
    StrategyId::ExpiryLongshotNo,
    StrategyId::MakerYes,
    StrategyId::MakerNo,
    StrategyId::VolumeMomentumYes,
    StrategyId::VolumeMomentumNo,
    StrategyId::GammaCarryYes,
    StrategyId::GammaCarryNo,
    StrategyId::DivergenceFadeYes,
    StrategyId::DivergenceFadeNo,
    StrategyId::SlowGrindYes,
    StrategyId::SlowGrindNo,
];

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct StrategyDefinition {
    name: &'static str,
    family: &'static str,
    mode_label: &'static str,
    mode_short: &'static str,
    horizon: &'static str,
    thesis: &'static str,
    entry_rules: &'static [&'static str],
    exit_rules: &'static [&'static str],
    take_profit_pct: f64,
    stop_loss_pct: f64,
    max_hold_secs: i64,
    order_ttl_secs: i64,
}

fn definition(id: StrategyId) -> StrategyDefinition {
    match id {
        StrategyId::AnchorYes => StrategyDefinition {
            name: "fair-value YES discount",
            family: "relative value",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "minutes to hours",
            thesis: "Buy YES when the displayed offer is materially below the Gamma fair-value anchor in liquid books and exit once the discount closes.",
            entry_rules: &[
                "YES ask at least 4c below Gamma YES anchor",
                "YES mid between 15c and 85c",
                "spread <= 4c, strong liquidity, non-stale market",
            ],
            exit_rules: &[
                "take profit at +8% return or once anchor gap collapses",
                "stop at -6%, pre-close, time stop ~6h, or signal reversal",
            ],
            take_profit_pct: 0.08,
            stop_loss_pct: 0.06,
            max_hold_secs: 6 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::AnchorNo => StrategyDefinition {
            name: "fair-value NO discount",
            family: "relative value",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "minutes to hours",
            thesis: "Buy NO when the synthetic NO offer is materially below the Gamma NO anchor and close on convergence.",
            entry_rules: &[
                "NO ask at least 4c below Gamma NO anchor",
                "NO mid between 15c and 85c",
                "spread <= 4c and liquid book",
            ],
            exit_rules: &[
                "take profit at +8% or after gap convergence",
                "stop at -6%, pre-close, time stop ~6h, or reversal",
            ],
            take_profit_pct: 0.08,
            stop_loss_pct: 0.06,
            max_hold_secs: 6 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::MomentumYes => StrategyDefinition {
            name: "momentum YES",
            family: "trend",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "minutes to a few hours",
            thesis: "Short-horizon underreaction means strong positive one-hour moves can continue for a while in active markets.",
            entry_rules: &[
                "1h price change >= +5c",
                "mid between 20c and 80c with spread <= 3c",
                "liquid, actively traded, not too close to close",
            ],
            exit_rules: &[
                "take profit at +10%",
                "stop at -5%, reverse if 1h change flips negative, max hold ~4h",
            ],
            take_profit_pct: 0.10,
            stop_loss_pct: 0.05,
            max_hold_secs: 4 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::MomentumNo => StrategyDefinition {
            name: "momentum NO",
            family: "trend",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "minutes to a few hours",
            thesis: "Negative one-hour drift can persist, so buy NO into sharp downside momentum on the YES contract.",
            entry_rules: &[
                "1h YES change <= -5c",
                "NO mid between 20c and 80c with spread <= 3c",
                "liquid, actively traded, not too close to close",
            ],
            exit_rules: &[
                "take profit at +10%",
                "stop at -5%, reverse if 1h change flips positive, max hold ~4h",
            ],
            take_profit_pct: 0.10,
            stop_loss_pct: 0.05,
            max_hold_secs: 4 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::MeanRevertYes => StrategyDefinition {
            name: "mean reversion YES",
            family: "reversal",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "1-3 hours",
            thesis: "Large downside shocks can overshoot, especially in still-live books away from expiry, creating bounce opportunities in YES.",
            entry_rules: &[
                "1h change <= -8c",
                "YES mid between 10c and 45c, spread <= 6c",
                "liquid market, at least 6h to close",
            ],
            exit_rules: &[
                "take profit at +7%",
                "stop at -5%, exit on normalization/reversal, max hold ~3h",
            ],
            take_profit_pct: 0.07,
            stop_loss_pct: 0.05,
            max_hold_secs: 3 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::MeanRevertNo => StrategyDefinition {
            name: "mean reversion NO",
            family: "reversal",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "1-3 hours",
            thesis: "Fade sharp one-hour rallies by buying NO when the YES contract has likely overshot.",
            entry_rules: &[
                "1h change >= +8c",
                "YES mid between 55c and 90c, spread <= 6c",
                "liquid market, at least 6h to close",
            ],
            exit_rules: &[
                "take profit at +7%",
                "stop at -5%, exit on normalization/reversal, max hold ~3h",
            ],
            take_profit_pct: 0.07,
            stop_loss_pct: 0.05,
            max_hold_secs: 3 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::FavoriteYes => StrategyDefinition {
            name: "favorite YES",
            family: "calibration bias",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "hours to days",
            thesis: "Prediction and betting markets often overprice longshots and slightly underprice favorites, so buy strong YES favorites with tight spreads.",
            entry_rules: &[
                "YES mid between 75c and 93c",
                "spread <= 2c, strong liquidity and 24h volume",
                "within ~14 days of close and not collapsing intraday",
            ],
            exit_rules: &[
                "take profit at +4% or near 97c",
                "stop at -3%, time stop ~48h, or major reversal",
            ],
            take_profit_pct: 0.04,
            stop_loss_pct: 0.03,
            max_hold_secs: 48 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::LongshotNo => StrategyDefinition {
            name: "longshot fade via NO",
            family: "calibration bias",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "hours to days",
            thesis: "Extreme YES longshots are often overpriced, so buying NO on 3c-15c YES contracts is the cleaner way to monetize the longshot side.",
            entry_rules: &[
                "YES mid between 3c and 15c",
                "NO spread <= 3c, good liquidity and volume",
                "within ~14 days of close and not sharply rebounding",
            ],
            exit_rules: &[
                "take profit at +4% or near 97c on NO",
                "stop at -3%, time stop ~48h, or major reversal",
            ],
            take_profit_pct: 0.04,
            stop_loss_pct: 0.03,
            max_hold_secs: 48 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::ExpiryFavoriteYes => StrategyDefinition {
            name: "near-expiry favorite YES",
            family: "expiry calibration",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "sub-2 days",
            thesis: "As resolution approaches, high-probability contracts often converge more cleanly, so near-expiry favorites can be followed with tight exits.",
            entry_rules: &[
                "0.5h to 48h until close",
                "YES mid >= 82c and spread <= 1.5c",
                "strong volume and non-negative recent drift",
            ],
            exit_rules: &[
                "take profit at +3% or near 98c",
                "stop at -2.5%, exit shortly before close or on reversal",
            ],
            take_profit_pct: 0.03,
            stop_loss_pct: 0.025,
            max_hold_secs: 18 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::ExpiryLongshotNo => StrategyDefinition {
            name: "near-expiry longshot NO",
            family: "expiry calibration",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "sub-2 days",
            thesis: "Near-expiry tail outcomes often remain too rich; buying NO on them is a cleaner expiration-convergence trade.",
            entry_rules: &[
                "0.5h to 48h until close",
                "YES mid <= 18c and spread <= 1.5c",
                "strong volume and non-positive recent drift",
            ],
            exit_rules: &[
                "take profit at +3% or near 98c on NO",
                "stop at -2.5%, exit shortly before close or on reversal",
            ],
            take_profit_pct: 0.03,
            stop_loss_pct: 0.025,
            max_hold_secs: 18 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::MakerYes => StrategyDefinition {
            name: "passive maker YES",
            family: "market making",
            mode_label: "maker bid",
            mode_short: "MKR",
            horizon: "minutes to hours",
            thesis: "Wide, calm books allow passive YES bidding inside the spread, reducing taker fees and adverse selection; maker rebates are ignored here for conservatism.",
            entry_rules: &[
                "YES spread between 3c and 10c",
                "1h change roughly flat, mid between 25c and 75c",
                "liquid book; quote one tick inside the bid/ask",
            ],
            exit_rules: &[
                "cancel stale quotes on expiry or reversal",
                "after fill, take +3%, stop -2%, time stop ~2h",
            ],
            take_profit_pct: 0.03,
            stop_loss_pct: 0.02,
            max_hold_secs: 2 * 3600,
            order_ttl_secs: 45 * 60,
        },
        StrategyId::MakerNo => StrategyDefinition {
            name: "passive maker NO",
            family: "market making",
            mode_label: "maker bid",
            mode_short: "MKR",
            horizon: "minutes to hours",
            thesis: "Mirror of the YES maker strategy on the NO side for wide, quiet books.",
            entry_rules: &[
                "NO spread between 3c and 10c",
                "1h change roughly flat, NO mid between 25c and 75c",
                "liquid book; quote one tick inside the bid/ask",
            ],
            exit_rules: &[
                "cancel stale quotes on expiry or reversal",
                "after fill, take +3%, stop -2%, time stop ~2h",
            ],
            take_profit_pct: 0.03,
            stop_loss_pct: 0.02,
            max_hold_secs: 2 * 3600,
            order_ttl_secs: 45 * 60,
        },
        StrategyId::VolumeMomentumYes => StrategyDefinition {
            name: "volume breakout YES",
            family: "volume trend",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "minutes to hours",
            thesis: "High 24h volume combined with positive momentum indicates strong informed buying flow; ride the breakout.",
            entry_rules: &[
                "1h change >= +4c and 1d change >= +2c",
                "YES mid between 20c and 80c, spread <= 3c",
                "volume24hr >= 20_000, liquidity >= 15_000",
            ],
            exit_rules: &[
                "take profit at +8%",
                "stop at -4%, reverse if momentum fades, max hold ~3h",
            ],
            take_profit_pct: 0.08,
            stop_loss_pct: 0.04,
            max_hold_secs: 3 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::VolumeMomentumNo => StrategyDefinition {
            name: "volume breakout NO",
            family: "volume trend",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "minutes to hours",
            thesis: "High volume with sustained downside momentum; buy NO into the informed selling flow.",
            entry_rules: &[
                "1h change <= -4c and 1d change <= -2c",
                "NO mid between 20c and 80c, spread <= 3c",
                "volume24hr >= 20_000, liquidity >= 15_000",
            ],
            exit_rules: &[
                "take profit at +8%",
                "stop at -4%, reverse if momentum fades, max hold ~3h",
            ],
            take_profit_pct: 0.08,
            stop_loss_pct: 0.04,
            max_hold_secs: 3 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::GammaCarryYes => StrategyDefinition {
            name: "gamma carry YES",
            family: "convergence",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "hours to days",
            thesis: "When Gamma fair value and market price agree on direction but the market offers a small favorable gap, the convergence tends to be slower but more reliable.",
            entry_rules: &[
                "Gamma YES >= market YES + 2c but < 6c",
                "1h change confirms direction, mid between 20c and 75c",
                "spread <= 4c, good liquidity and volume",
            ],
            exit_rules: &[
                "take profit at +6% or when gap collapses",
                "stop at -4%, max hold ~12h",
            ],
            take_profit_pct: 0.06,
            stop_loss_pct: 0.04,
            max_hold_secs: 12 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::GammaCarryNo => StrategyDefinition {
            name: "gamma carry NO",
            family: "convergence",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "hours to days",
            thesis: "Mirror carry trade on the NO side when Gamma NO is slightly above market NO with confirming drift.",
            entry_rules: &[
                "Gamma NO >= market NO + 2c but < 6c",
                "1h change confirms direction, NO mid between 20c and 75c",
                "spread <= 4c, good liquidity and volume",
            ],
            exit_rules: &[
                "take profit at +6% or when gap collapses",
                "stop at -4%, max hold ~12h",
            ],
            take_profit_pct: 0.06,
            stop_loss_pct: 0.04,
            max_hold_secs: 12 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::DivergenceFadeYes => StrategyDefinition {
            name: "divergence fade YES",
            family: "reversal",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "1-4 hours",
            thesis: "When 1h and 1d price changes point in opposite directions, the shorter timeframe move is often a temporary overreaction worth fading.",
            entry_rules: &[
                "1h change >= +5c but 1d change <= -2c (divergence)",
                "YES mid between 15c and 70c, spread <= 5c",
                "liquid market, at least 2h to close",
            ],
            exit_rules: &[
                "take profit at +5% when reversion begins",
                "stop at -4%, time stop ~4h",
            ],
            take_profit_pct: 0.05,
            stop_loss_pct: 0.04,
            max_hold_secs: 4 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::DivergenceFadeNo => StrategyDefinition {
            name: "divergence fade NO",
            family: "reversal",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "1-4 hours",
            thesis: "Mirror fade: 1h sell-off against positive 1d trend suggests temporary fear; buy NO to capture the snap-back.",
            entry_rules: &[
                "1h change <= -5c but 1d change >= +2c (divergence)",
                "NO mid between 15c and 70c, spread <= 5c",
                "liquid market, at least 2h to close",
            ],
            exit_rules: &[
                "take profit at +5% when reversion begins",
                "stop at -4%, time stop ~4h",
            ],
            take_profit_pct: 0.05,
            stop_loss_pct: 0.04,
            max_hold_secs: 4 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::SlowGrindYes => StrategyDefinition {
            name: "slow grind YES",
            family: "trend",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "hours to half a day",
            thesis: "Persistent small positive drift across both 1h and 1d in very liquid books often reflects steady informed accumulation rather than noise.",
            entry_rules: &[
                "1h change between +1c and +6c, 1d change >= +1c",
                "YES mid between 25c and 75c, spread <= 2c",
                "volume24hr >= 25_000, liquidity >= 25_000",
            ],
            exit_rules: &[
                "take profit at +5%",
                "stop at -3%, time stop ~8h",
            ],
            take_profit_pct: 0.05,
            stop_loss_pct: 0.03,
            max_hold_secs: 8 * 3600,
            order_ttl_secs: 0,
        },
        StrategyId::SlowGrindNo => StrategyDefinition {
            name: "slow grind NO",
            family: "trend",
            mode_label: "taker entry",
            mode_short: "TKR",
            horizon: "hours to half a day",
            thesis: "Persistent small negative drift across timeframes in liquid books suggests steady informed distribution; capture via NO.",
            entry_rules: &[
                "1h change between -1c and -6c, 1d change <= -1c",
                "NO mid between 25c and 75c, spread <= 2c",
                "volume24hr >= 25_000, liquidity >= 25_000",
            ],
            exit_rules: &[
                "take profit at +5%",
                "stop at -3%, time stop ~8h",
            ],
            take_profit_pct: 0.05,
            stop_loss_pct: 0.03,
            max_hold_secs: 8 * 3600,
            order_ttl_secs: 0,
        },
    }
}

#[derive(Debug, Clone)]
struct StrategyMarket {
    market_id: String,
    question: String,
    category: String,
    gamma_yes_price: f64,
    gamma_no_price: f64,
    yes_bid: Option<f64>,
    yes_ask: Option<f64>,
    no_bid: Option<f64>,
    no_ask: Option<f64>,
    spread: Option<f64>,
    liquidity: f64,
    volume24hr: f64,
    last_trade_price: Option<f64>,
    one_hour_change: f64,
    one_day_change: f64,
    end_date: Option<DateTime<Utc>>,
    accepting_orders: bool,
    closed: bool,
    fees_enabled: Option<bool>,
    taker_fee_rate: Option<f64>,
    tick_size: Option<f64>,
}

impl StrategyMarket {
    fn yes_mid(&self) -> f64 {
        match (self.yes_bid, self.yes_ask) {
            (Some(bid), Some(ask)) if ask >= bid => ((bid + ask) / 2.0).clamp(0.001, 0.999),
            _ => self
                .last_trade_price
                .unwrap_or(self.gamma_yes_price)
                .clamp(0.001, 0.999),
        }
    }

    fn no_mid(&self) -> f64 {
        match (self.no_bid, self.no_ask) {
            (Some(bid), Some(ask)) if ask >= bid => ((bid + ask) / 2.0).clamp(0.001, 0.999),
            _ => self.gamma_no_price.clamp(0.001, 0.999),
        }
    }

    fn side_mid(&self, side: OutcomeSide) -> f64 {
        match side {
            OutcomeSide::Yes => self.yes_mid(),
            OutcomeSide::No => self.no_mid(),
        }
    }

    fn side_bid(&self, side: OutcomeSide) -> Option<f64> {
        match side {
            OutcomeSide::Yes => self.yes_bid,
            OutcomeSide::No => self.no_bid,
        }
        .filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0)
    }

    fn side_ask(&self, side: OutcomeSide) -> Option<f64> {
        match side {
            OutcomeSide::Yes => self.yes_ask,
            OutcomeSide::No => self.no_ask,
        }
        .filter(|price| price.is_finite() && *price > 0.0 && *price < 1.0)
    }

    fn yes_spread(&self) -> Option<f64> {
        match (self.yes_bid, self.yes_ask) {
            (Some(bid), Some(ask)) if ask >= bid => Some((ask - bid).max(0.0)),
            _ => self.spread,
        }
    }

    fn no_spread(&self) -> Option<f64> {
        match (self.no_bid, self.no_ask) {
            (Some(bid), Some(ask)) if ask >= bid => Some((ask - bid).max(0.0)),
            _ => self.yes_spread(),
        }
    }

    fn side_spread(&self, side: OutcomeSide) -> Option<f64> {
        match side {
            OutcomeSide::Yes => self.yes_spread(),
            OutcomeSide::No => self.no_spread(),
        }
    }

    fn estimated_spread(&self) -> f64 {
        self.yes_spread()
            .or(self.spread)
            .unwrap_or(0.02)
            .clamp(0.005, 0.10)
    }

    fn hours_to_close(&self, now: DateTime<Utc>) -> Option<f64> {
        let end = self.end_date?;
        Some((end - now).num_seconds() as f64 / 3600.0)
    }

    fn effective_fee_rate(&self, config: &Config) -> f64 {
        if matches!(self.fees_enabled, Some(false)) {
            return 0.0;
        }
        self.taker_fee_rate
            .unwrap_or_else(|| config.fee_theta(&self.category))
    }

    fn tick_size(&self) -> f64 {
        self.tick_size
            .filter(|value| value.is_finite() && *value > 0.0 && *value <= 1.0)
            .unwrap_or(DEFAULT_TICK_SIZE)
    }
}

#[derive(Debug, Deserialize, Default)]
struct RawStrategyMarket {
    id: Option<Value>,
    question: Option<String>,
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    slug: Option<String>,
    outcomes: Option<Value>,
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<Value>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<Value>,
    #[serde(rename = "bestBid")]
    best_bid: Option<Value>,
    #[serde(rename = "bestAsk")]
    best_ask: Option<Value>,
    spread: Option<Value>,
    #[serde(rename = "lastTradePrice")]
    last_trade_price: Option<Value>,
    #[serde(rename = "oneHourPriceChange")]
    one_hour_price_change: Option<Value>,
    #[serde(rename = "oneDayPriceChange")]
    one_day_price_change: Option<Value>,
    liquidity: Option<Value>,
    #[serde(rename = "volume24hr")]
    volume24hr: Option<Value>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    category: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    #[serde(rename = "acceptingOrders")]
    accepting_orders: Option<bool>,
    #[serde(rename = "feesEnabled")]
    fees_enabled: Option<bool>,
    #[serde(rename = "takerBaseFee")]
    taker_base_fee: Option<Value>,
    #[serde(rename = "orderPriceMinTickSize")]
    order_price_min_tick_size: Option<Value>,
    events: Option<Value>,
}

async fn fetch_strategy_markets(client: &Client, config: &Config) -> Result<Vec<StrategyMarket>> {
    let mut markets = Vec::new();
    let page_size = 200usize.min(config.strategy_lab_market_limit.max(1));
    let mut offset = 0usize;

    loop {
        let url = format!("{}/markets", config.gamma_api_url);
        let params: Vec<(String, String)> = vec![
            ("active".to_string(), "true".to_string()),
            ("closed".to_string(), "false".to_string()),
            ("limit".to_string(), page_size.to_string()),
            ("offset".to_string(), offset.to_string()),
            ("order".to_string(), "volume24hr".to_string()),
            ("ascending".to_string(), "false".to_string()),
        ];
        let page = request_with_retry(client, &url, &params, config)
            .await?
            .context("strategy market response was empty")?;
        let raw_markets: Vec<RawStrategyMarket> =
            serde_json::from_value(page).context("failed to deserialize strategy market page")?;
        if raw_markets.is_empty() {
            break;
        }

        let batch_len = raw_markets.len();
        for raw in raw_markets {
            if let Some(market) = parse_strategy_market(raw) {
                if market.liquidity >= config.min_liquidity_usd {
                    markets.push(market);
                }
            }
            if markets.len() >= config.strategy_lab_market_limit {
                break;
            }
        }
        if markets.len() >= config.strategy_lab_market_limit || batch_len < page_size {
            break;
        }
        offset += page_size;
    }

    debug!("Strategy lab loaded {} markets", markets.len());
    Ok(markets)
}

fn parse_strategy_market(raw: RawStrategyMarket) -> Option<StrategyMarket> {
    let outcomes_raw = parse_jsonish_array(raw.outcomes.as_ref())?;
    let outcomes: Vec<String> = outcomes_raw.iter().filter_map(value_to_string).collect();
    if outcomes.len() != 2 {
        return None;
    }
    let normalized: Vec<String> = outcomes
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .collect();
    let yes_idx = normalized.iter().position(|outcome| outcome == "yes")?;
    let no_idx = normalized.iter().position(|outcome| outcome == "no")?;

    let prices_raw = parse_jsonish_array(raw.outcome_prices.as_ref())?;
    let prices: Vec<f64> = prices_raw.iter().filter_map(value_to_f64).collect();
    if prices.len() <= yes_idx.max(no_idx) {
        return None;
    }
    let gamma_yes_price = prices[yes_idx].clamp(0.001, 0.999);
    let gamma_no_price = prices[no_idx].clamp(0.001, 0.999);

    let market_id = raw
        .id
        .as_ref()
        .and_then(value_to_string)
        .or(raw.condition_id.clone())
        .or(raw.slug.clone())?;

    let question = raw.question.unwrap_or_else(|| "Unknown market".to_string());
    let category = raw
        .category
        .unwrap_or_else(|| "other".to_string())
        .to_ascii_lowercase();
    let yes_bid = raw
        .best_bid
        .as_ref()
        .and_then(value_to_f64)
        .map(|value| value.clamp(0.001, 0.999));
    let yes_ask = raw
        .best_ask
        .as_ref()
        .and_then(value_to_f64)
        .map(|value| value.clamp(0.001, 0.999));
    let no_bid = yes_ask.map(|ask| (1.0 - ask).clamp(0.001, 0.999));
    let no_ask = yes_bid.map(|bid| (1.0 - bid).clamp(0.001, 0.999));
    let spread = raw
        .spread
        .as_ref()
        .and_then(value_to_f64)
        .map(|value| value.clamp(0.0, 0.50));
    let liquidity = raw.liquidity.as_ref().and_then(value_to_f64).unwrap_or(0.0);
    let volume24hr = raw
        .volume24hr
        .as_ref()
        .and_then(value_to_f64)
        .unwrap_or(0.0);
    let last_trade_price = raw
        .last_trade_price
        .as_ref()
        .and_then(value_to_f64)
        .map(|value| value.clamp(0.001, 0.999));
    let one_hour_change = raw
        .one_hour_price_change
        .as_ref()
        .and_then(value_to_f64)
        .unwrap_or(0.0);
    let one_day_change = raw
        .one_day_price_change
        .as_ref()
        .and_then(value_to_f64)
        .unwrap_or(0.0);
    let end_date = raw.end_date.and_then(|value| parse_datetime(&value));
    let accepting_orders = raw.accepting_orders.unwrap_or(raw.active.unwrap_or(true));
    let closed = raw.closed.unwrap_or(false);
    let fees_enabled = raw.fees_enabled;
    let taker_fee_rate = normalize_fee_rate(raw.taker_base_fee.as_ref());
    let tick_size = raw
        .order_price_min_tick_size
        .as_ref()
        .and_then(value_to_f64);

    let _ = raw.slug;
    let _ = raw.clob_token_ids;
    let _ = raw.events;

    Some(StrategyMarket {
        market_id,
        question,
        category,
        gamma_yes_price,
        gamma_no_price,
        yes_bid,
        yes_ask,
        no_bid,
        no_ask,
        spread,
        liquidity,
        volume24hr,
        last_trade_price,
        one_hour_change,
        one_day_change,
        end_date,
        accepting_orders,
        closed,
        fees_enabled,
        taker_fee_rate,
        tick_size,
    })
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

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

async fn request_with_retry(
    client: &Client,
    url: &str,
    params: &[(String, String)],
    config: &Config,
) -> Result<Option<Value>> {
    for attempt in 0..=config.max_retries {
        let response = client
            .get(url)
            .query(params)
            .timeout(Duration::from_secs(config.api_timeout_secs.max(1)))
            .send()
            .await;

        match response {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    let data = response
                        .json::<Value>()
                        .await
                        .context("strategy lab json decode failed")?;
                    return Ok(Some(data));
                }
                if (status.as_u16() == 429 || status.is_server_error())
                    && attempt < config.max_retries
                {
                    let wait = retry_backoff_ms(config.retry_backoff_base_ms, attempt);
                    warn!(
                        "Strategy lab retry for {url}: status={status} wait={}ms",
                        wait
                    );
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    continue;
                }
                return Err(anyhow::anyhow!("HTTP {status} for {url}"));
            }
            Err(err) => {
                if (err.is_timeout() || err.is_connect()) && attempt < config.max_retries {
                    let wait = retry_backoff_ms(config.retry_backoff_base_ms, attempt);
                    warn!(
                        "Strategy lab request retry for {url}: {err} wait={}ms",
                        wait
                    );
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    continue;
                }
                return Err(err).with_context(|| format!("request failed for {url}"));
            }
        }
    }
    Ok(None)
}

fn retry_backoff_ms(base_ms: u64, attempt: u32) -> u64 {
    base_ms
        .saturating_mul(2_u64.saturating_pow(attempt))
        .min(MAX_RETRY_BACKOFF_MS)
}

fn parse_jsonish_array(value: Option<&Value>) -> Option<Vec<Value>> {
    let value = value?;
    match value {
        Value::Array(items) => Some(items.clone()),
        Value::String(text) => serde_json::from_str::<Vec<Value>>(text).ok(),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn round_down(value: f64, step: f64) -> f64 {
    if !value.is_finite() || value <= 0.0 {
        return 0.0;
    }
    if !step.is_finite() || step <= 0.0 {
        return value;
    }
    (value / step).floor() * step
}

fn short_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        text.to_string()
    } else {
        let mut out = text
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_strategy_market_handles_yes_no_arrays() {
        let raw = RawStrategyMarket {
            id: Some(Value::String("m1".into())),
            question: Some("Will it rain?".into()),
            condition_id: Some("c1".into()),
            slug: Some("rain".into()),
            outcomes: Some(Value::Array(vec![
                Value::String("Yes".into()),
                Value::String("No".into()),
            ])),
            outcome_prices: Some(Value::Array(vec![Value::from(0.42), Value::from(0.58)])),
            best_bid: Some(Value::from(0.40)),
            best_ask: Some(Value::from(0.44)),
            spread: Some(Value::from(0.04)),
            liquidity: Some(Value::from(10000)),
            volume24hr: Some(Value::from(5000)),
            active: Some(true),
            accepting_orders: Some(true),
            category: Some("sports".into()),
            ..Default::default()
        };
        let market = parse_strategy_market(raw).expect("expected market");
        assert!((market.gamma_yes_price - 0.42).abs() < 1e-9);
        assert_eq!(market.side_bid(OutcomeSide::Yes), Some(0.40));
        assert!(market.no_ask.is_some());
    }

    #[test]
    fn position_notional_uses_available_cash() {
        let mut account = StrategyAccount::new(100.0);
        account.pending_orders.push(PendingOrder {
            market_id: "m".into(),
            question: "Q".into(),
            side: OutcomeSide::Yes,
            price: 0.5,
            shares: 100.0,
            expires_at: Utc::now(),
            max_hold_secs: 1,
            take_profit_pct: 0.1,
            stop_loss_pct: 0.1,
        });
        let market = StrategyMarket {
            market_id: "m2".into(),
            question: "Q2".into(),
            category: "sports".into(),
            gamma_yes_price: 0.5,
            gamma_no_price: 0.5,
            yes_bid: Some(0.49),
            yes_ask: Some(0.51),
            no_bid: Some(0.49),
            no_ask: Some(0.51),
            spread: Some(0.02),
            liquidity: 10000.0,
            volume24hr: 5000.0,
            last_trade_price: Some(0.5),
            one_hour_change: 0.0,
            one_day_change: 0.0,
            end_date: None,
            accepting_orders: true,
            closed: false,
            fees_enabled: Some(true),
            taker_fee_rate: Some(0.03),
            tick_size: Some(0.01),
        };
        let mut cfg = Config::from_env();
        cfg.strategy_lab_position_size_usd = 25.0;
        let notional = position_notional_usd(&account, &market, &cfg).expect("notional");
        assert!(notional <= 20.0);
    }
}
