use anyhow::{Context, Result};
use chrono::Utc;
use csv::{ReaderBuilder, Writer, WriterBuilder};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::fd::AsRawFd;

use crate::config::DEFAULT_DIAGNOSTICS_CSV_MAX_BYTES;

type CsvWriter = Writer<CountingWriter<BufWriter<File>>>;

#[derive(Clone)]
pub struct DiagnosticsLogger {
    root_dir: PathBuf,
    policy: DiagnosticsPolicy,
    last_error: Arc<Mutex<Option<String>>>,
    candidate_evaluations: Arc<Mutex<DiagnosticsCsv>>,
    candidate_rejections: Arc<Mutex<DiagnosticsCsv>>,
    trade_log: Arc<Mutex<DiagnosticsCsv>>,
    scan_summary: Arc<Mutex<DiagnosticsCsv>>,
    latency_budget: Arc<Mutex<DiagnosticsCsv>>,
    // Keep the lock last so writer handles are dropped before the directory is
    // unlocked when the final logger clone is released.
    _directory_lock: Arc<DiagnosticsDirectoryLock>,
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.bytes_written = self.bytes_written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct DiagnosticsCsv {
    path: PathBuf,
    header_fields: Vec<String>,
    header_bytes: u64,
    max_bytes: Option<u64>,
    writer: Option<CsvWriter>,
}

struct DiagnosticsDirectoryLock {
    #[cfg(unix)]
    file: File,
}

#[cfg(unix)]
impl Drop for DiagnosticsDirectoryLock {
    fn drop(&mut self) {
        // SAFETY: `file` remains open for the lifetime of this guard.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn acquire_diagnostics_directory_lock(root_dir: &Path) -> Result<DiagnosticsDirectoryLock> {
    let path = root_dir.join(".scanner-diagnostics.lock");
    #[cfg(unix)]
    {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening diagnostics directory lock {}", path.display()))?;
        // SAFETY: `file` is a valid open descriptor retained by the guard.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            anyhow::bail!(
                "another scanner holds diagnostics directory lock {}: {}",
                path.display(),
                std::io::Error::last_os_error(),
            );
        }
        Ok(DiagnosticsDirectoryLock { file })
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(DiagnosticsDirectoryLock {})
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .with_context(|| format!("opening directory for fsync {}", path.display()))?
            .sync_all()
            .with_context(|| format!("fsyncing directory {}", path.display()))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DiagnosticsPolicy {
    pub log_all_candidate_evaluations: bool,
    pub log_routine_rejections: bool,
}

impl Default for DiagnosticsPolicy {
    fn default() -> Self {
        Self {
            log_all_candidate_evaluations: true,
            log_routine_rejections: true,
        }
    }
}

const COMPACT_CANDIDATE_EVALUATION_RANK_LIMIT: usize = 10;

#[derive(Debug, Clone)]
pub struct CandidateEvaluationRow {
    pub timestamp: String,
    pub scan_id: u64,
    pub pool: String,
    pub selected: bool,
    pub selected_rank: Option<usize>,
    pub selection_state: String,
    pub event_id: String,
    pub event_title: String,
    pub event_slug: String,
    pub market_question: String,
    pub outcome_side: String,
    pub candidate_score: f64,
    pub theory_hint: f64,
    pub tradable_legs: usize,
    pub total_tokens: usize,
    pub cached_tokens: usize,
    pub missing_tokens: usize,
    pub quote_budget: usize,
    pub active_token_budget: usize,
}

#[derive(Debug, Clone)]
pub struct CandidateRejectionRow {
    pub timestamp: String,
    pub scan_id: u64,
    pub pool: String,
    pub event_id: String,
    pub event_title: String,
    pub event_slug: String,
    pub market_question: String,
    pub arb_type: String,
    pub outcome_side: String,
    pub stage: String,
    pub reason: String,
    pub theory_hint: f64,
    pub quote_ready: bool,
    pub total_cost: Option<f64>,
    pub gross_profit: Option<f64>,
    pub total_fees: Option<f64>,
    pub projected_net_profit: Option<f64>,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct TradeLogRow {
    pub timestamp: String,
    pub scan_id: u64,
    pub mode: String,
    pub status: String,
    pub pnl_scale: String,
    pub event_id: String,
    pub event_title: String,
    pub arb_type: String,
    pub legs_summary: String,
    pub target_position_usd: f64,
    pub projected_net_profit: f64,
    pub projected_roi_pct: f64,
    pub filled_cost_usd: Option<f64>,
    pub conservative_pnl_usd: Option<f64>,
    pub conservative_roi_pct: Option<f64>,
    pub planned_basket_units: Option<f64>,
    pub hedged_basket_units: Option<f64>,
    pub fill_count: Option<usize>,
    pub partial_fill: Option<bool>,
    pub parity_ok: Option<bool>,
    pub unhedged_notional_usd: Option<f64>,
    pub prices_from_clob: bool,
    pub note: String,
}

#[derive(Debug, Clone)]
pub struct ScanSummaryRow {
    pub timestamp: String,
    pub scan_id: u64,
    pub opportunities_found: usize,
    pub neg_risk_events_total: usize,
    pub bundle_markets_total: usize,
    pub ranked_families_discovered: usize,
    pub ranked_families_scanned: usize,
    pub raw_yes_candidates: usize,
    pub raw_no_candidates: usize,
    pub raw_bundle_candidates: usize,
    pub raw_ranked_candidates: usize,
    pub yes_candidates_total: usize,
    pub no_candidates_total: usize,
    pub yes_selected_events: usize,
    pub no_selected_events: usize,
    pub bundle_markets_scanned: usize,
    pub quote_tokens_total: usize,
    pub quote_tokens_unique_selected: usize,
    pub quote_ready_yes_events: usize,
    pub quote_ready_no_events: usize,
    pub quote_ready_bundle_markets: usize,
    pub quote_hard_unresolved_tokens: usize,
    pub quote_no_ask_tokens: usize,
    pub quote_missing_book_tokens: usize,
    pub quote_deferred_tokens: usize,
    pub target_projection_rejections: usize,
    pub target_size_rejections: usize,
    pub suppressed_duplicates: usize,
    pub theory_hint_yes: usize,
    pub theory_hint_no: usize,
    pub theory_hint_bundle: usize,
    pub best_raw_edge_type: String,
    pub best_raw_edge_event_id: String,
    pub best_raw_edge_event_title: String,
    pub best_raw_edge_cost: Option<f64>,
    pub best_raw_edge_revenue: Option<f64>,
    pub best_raw_edge_gross_profit: Option<f64>,
    pub best_raw_edge_total_fees: Option<f64>,
    pub best_raw_edge_net_profit: Option<f64>,
    pub best_raw_edge_roi_pct: Option<f64>,
    pub best_raw_edge_prices_from_clob: Option<bool>,
    pub scan_duration_ms: f64,
    pub cumulative_trades_executed: usize,
    pub cumulative_pnl_usd: f64,
    pub cumulative_pnl_pct: f64,
}

#[derive(Debug, Clone)]
pub struct LatencyBudgetRow {
    pub timestamp: String,
    pub scan_id: u64,
    pub status: String,
    pub blockers: String,
    pub scan_duration_ms: f64,
    pub max_signal_age_ms: f64,
    pub ws_snapshot_wait_ms: f64,
    pub ws_snapshot_ready_tokens: usize,
    pub ws_snapshot_total_tokens: usize,
    pub ws_snapshot_min_ready_tokens: usize,
    pub ws_snapshot_satisfied: bool,
    pub quote_tokens_unique_selected: usize,
    pub quote_cache_hits: usize,
    pub quote_rest_requested: usize,
    pub quote_rest_resolved: usize,
    pub quote_rest_batches: usize,
    pub quote_rest_resolution_pct: f64,
    pub quote_deferred_tokens: usize,
    pub quote_hard_unresolved_tokens: usize,
    pub target_size_rejections: usize,
}

impl DiagnosticsLogger {
    #[cfg(test)]
    pub fn new(root_dir: PathBuf) -> Result<Self> {
        Self::new_with_policy(root_dir, DiagnosticsPolicy::default())
    }

    pub fn new_with_policy(root_dir: PathBuf, policy: DiagnosticsPolicy) -> Result<Self> {
        Self::new_with_policy_and_max_bytes(root_dir, policy, DEFAULT_DIAGNOSTICS_CSV_MAX_BYTES)
    }

    pub fn new_with_policy_and_max_bytes(
        root_dir: PathBuf,
        policy: DiagnosticsPolicy,
        max_file_bytes: u64,
    ) -> Result<Self> {
        let root_existed = root_dir.exists();
        fs::create_dir_all(&root_dir)
            .with_context(|| format!("creating diagnostics directory {}", root_dir.display()))?;
        if !root_existed {
            if let Some(parent) = root_dir.parent() {
                sync_directory(if parent.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    parent
                })?;
            }
        }
        let directory_lock = Arc::new(acquire_diagnostics_directory_lock(&root_dir)?);

        let candidate_evaluations_path = root_dir.join("candidate_evaluations.csv");
        let candidate_rejections_path = root_dir.join("candidate_rejections.csv");
        let trade_log_path = root_dir.join("trades.csv");
        let scan_summary_path = root_dir.join("scan_summary.csv");
        let latency_budget_path = root_dir.join("latency_budget.csv");

        let candidate_evaluations = Arc::new(Mutex::new(DiagnosticsCsv::open(
            &candidate_evaluations_path,
            &[
                "timestamp",
                "scan_id",
                "pool",
                "selected",
                "selected_rank",
                "selection_state",
                "event_id",
                "event_title",
                "event_slug",
                "market_question",
                "outcome_side",
                "candidate_score",
                "theory_hint",
                "tradable_legs",
                "total_tokens",
                "cached_tokens",
                "missing_tokens",
                "quote_budget",
                "active_token_budget",
            ],
            Some(max_file_bytes),
        )?));
        let candidate_rejections = Arc::new(Mutex::new(DiagnosticsCsv::open(
            &candidate_rejections_path,
            &[
                "timestamp",
                "scan_id",
                "pool",
                "event_id",
                "event_title",
                "event_slug",
                "market_question",
                "arb_type",
                "outcome_side",
                "stage",
                "reason",
                "theory_hint",
                "quote_ready",
                "total_cost",
                "gross_profit",
                "total_fees",
                "projected_net_profit",
                "note",
            ],
            Some(max_file_bytes),
        )?));
        let trade_log = Arc::new(Mutex::new(DiagnosticsCsv::open(
            &trade_log_path,
            &[
                "timestamp",
                "scan_id",
                "mode",
                "status",
                "pnl_scale",
                "event_id",
                "event_title",
                "arb_type",
                "legs_summary",
                "target_position_usd",
                "projected_net_profit",
                "projected_roi_pct",
                "filled_cost_usd",
                "conservative_pnl_usd",
                "conservative_roi_pct",
                "planned_basket_units",
                "hedged_basket_units",
                "fill_count",
                "partial_fill",
                "parity_ok",
                "unhedged_notional_usd",
                "prices_from_clob",
                "note",
            ],
            None,
        )?));
        let scan_summary = Arc::new(Mutex::new(DiagnosticsCsv::open(
            &scan_summary_path,
            &[
                "timestamp",
                "scan_id",
                "opportunities_found",
                "neg_risk_events_total",
                "bundle_markets_total",
                "ranked_families_discovered",
                "ranked_families_scanned",
                "raw_yes_candidates",
                "raw_no_candidates",
                "raw_bundle_candidates",
                "raw_ranked_candidates",
                "yes_candidates_total",
                "no_candidates_total",
                "yes_selected_events",
                "no_selected_events",
                "bundle_markets_scanned",
                "quote_tokens_total",
                "quote_tokens_unique_selected",
                "quote_ready_yes_events",
                "quote_ready_no_events",
                "quote_ready_bundle_markets",
                "quote_hard_unresolved_tokens",
                "quote_no_ask_tokens",
                "quote_missing_book_tokens",
                "quote_deferred_tokens",
                "target_projection_rejections",
                "target_size_rejections",
                "suppressed_duplicates",
                "theory_hint_yes",
                "theory_hint_no",
                "theory_hint_bundle",
                "best_raw_edge_type",
                "best_raw_edge_event_id",
                "best_raw_edge_event_title",
                "best_raw_edge_cost",
                "best_raw_edge_revenue",
                "best_raw_edge_gross_profit",
                "best_raw_edge_total_fees",
                "best_raw_edge_net_profit",
                "best_raw_edge_roi_pct",
                "best_raw_edge_prices_from_clob",
                "scan_duration_ms",
                "cumulative_trades_executed",
                "cumulative_pnl_usd",
                "cumulative_pnl_pct",
            ],
            Some(max_file_bytes),
        )?));
        let latency_budget = Arc::new(Mutex::new(DiagnosticsCsv::open(
            &latency_budget_path,
            &[
                "timestamp",
                "scan_id",
                "status",
                "blockers",
                "scan_duration_ms",
                "max_signal_age_ms",
                "ws_snapshot_wait_ms",
                "ws_snapshot_ready_tokens",
                "ws_snapshot_total_tokens",
                "ws_snapshot_min_ready_tokens",
                "ws_snapshot_satisfied",
                "quote_tokens_unique_selected",
                "quote_cache_hits",
                "quote_rest_requested",
                "quote_rest_resolved",
                "quote_rest_batches",
                "quote_rest_resolution_pct",
                "quote_deferred_tokens",
                "quote_hard_unresolved_tokens",
                "target_size_rejections",
            ],
            Some(max_file_bytes),
        )?));

        Ok(Self {
            root_dir,
            policy,
            last_error: Arc::new(Mutex::new(None)),
            candidate_evaluations,
            candidate_rejections,
            trade_log,
            scan_summary,
            latency_budget,
            _directory_lock: directory_lock,
        })
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Fail closed when evidence logging has become unavailable. The scanner
    /// checks this after every paper scan so an unattended campaign cannot
    /// continue while silently losing evidence.
    pub fn ensure_healthy(&self) -> Result<()> {
        let guard = self
            .last_error
            .lock()
            .map_err(|_| anyhow::anyhow!("diagnostics health lock poisoned"))?;
        if let Some(error) = guard.as_ref() {
            anyhow::bail!("diagnostics logging failed: {error}");
        }
        Ok(())
    }

    pub fn record_candidate_evaluation(&self, row: CandidateEvaluationRow) {
        if !self.policy.log_all_candidate_evaluations && !compact_candidate_evaluation(&row) {
            return;
        }

        let fields = vec![
            row.timestamp,
            row.scan_id.to_string(),
            row.pool,
            row.selected.to_string(),
            row.selected_rank.map(|v| v.to_string()).unwrap_or_default(),
            row.selection_state,
            row.event_id,
            row.event_title,
            row.event_slug,
            row.market_question,
            row.outcome_side,
            format_f64(row.candidate_score),
            format_f64(row.theory_hint),
            row.tradable_legs.to_string(),
            row.total_tokens.to_string(),
            row.cached_tokens.to_string(),
            row.missing_tokens.to_string(),
            row.quote_budget.to_string(),
            row.active_token_budget.to_string(),
        ];
        let _ = write_fields(
            &self.candidate_evaluations,
            &fields,
            &self.last_error,
            false,
        );
    }

    pub fn record_candidate_rejection(&self, row: CandidateRejectionRow) {
        if !self.policy.log_routine_rejections && routine_rejection(&row) {
            return;
        }

        let fields = vec![
            row.timestamp,
            row.scan_id.to_string(),
            row.pool,
            row.event_id,
            row.event_title,
            row.event_slug,
            row.market_question,
            row.arb_type,
            row.outcome_side,
            row.stage,
            row.reason,
            format_f64(row.theory_hint),
            row.quote_ready.to_string(),
            row.total_cost.map(format_f64).unwrap_or_default(),
            row.gross_profit.map(format_f64).unwrap_or_default(),
            row.total_fees.map(format_f64).unwrap_or_default(),
            row.projected_net_profit.map(format_f64).unwrap_or_default(),
            row.note,
        ];
        let _ = write_fields(&self.candidate_rejections, &fields, &self.last_error, false);
    }

    pub fn record_trade(&self, row: TradeLogRow) -> Result<()> {
        let fields = vec![
            row.timestamp,
            row.scan_id.to_string(),
            row.mode,
            row.status,
            row.pnl_scale,
            row.event_id,
            row.event_title,
            row.arb_type,
            row.legs_summary,
            format_f64(row.target_position_usd),
            format_f64(row.projected_net_profit),
            format_f64(row.projected_roi_pct),
            row.filled_cost_usd.map(format_f64).unwrap_or_default(),
            row.conservative_pnl_usd.map(format_f64).unwrap_or_default(),
            row.conservative_roi_pct.map(format_f64).unwrap_or_default(),
            row.planned_basket_units.map(format_f64).unwrap_or_default(),
            row.hedged_basket_units.map(format_f64).unwrap_or_default(),
            row.fill_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            row.partial_fill
                .map(|value| value.to_string())
                .unwrap_or_default(),
            row.parity_ok
                .map(|value| value.to_string())
                .unwrap_or_default(),
            row.unhedged_notional_usd
                .map(format_f64)
                .unwrap_or_default(),
            row.prices_from_clob.to_string(),
            row.note,
        ];
        // Trade rows are profitability evidence. Flush and fsync each one to
        // minimize the journal/CSV crash window and fail-stop on any error.
        write_fields(&self.trade_log, &fields, &self.last_error, true)
    }

    pub fn record_scan_summary(&self, row: ScanSummaryRow) {
        let fields = vec![
            row.timestamp,
            row.scan_id.to_string(),
            row.opportunities_found.to_string(),
            row.neg_risk_events_total.to_string(),
            row.bundle_markets_total.to_string(),
            row.ranked_families_discovered.to_string(),
            row.ranked_families_scanned.to_string(),
            row.raw_yes_candidates.to_string(),
            row.raw_no_candidates.to_string(),
            row.raw_bundle_candidates.to_string(),
            row.raw_ranked_candidates.to_string(),
            row.yes_candidates_total.to_string(),
            row.no_candidates_total.to_string(),
            row.yes_selected_events.to_string(),
            row.no_selected_events.to_string(),
            row.bundle_markets_scanned.to_string(),
            row.quote_tokens_total.to_string(),
            row.quote_tokens_unique_selected.to_string(),
            row.quote_ready_yes_events.to_string(),
            row.quote_ready_no_events.to_string(),
            row.quote_ready_bundle_markets.to_string(),
            row.quote_hard_unresolved_tokens.to_string(),
            row.quote_no_ask_tokens.to_string(),
            row.quote_missing_book_tokens.to_string(),
            row.quote_deferred_tokens.to_string(),
            row.target_projection_rejections.to_string(),
            row.target_size_rejections.to_string(),
            row.suppressed_duplicates.to_string(),
            row.theory_hint_yes.to_string(),
            row.theory_hint_no.to_string(),
            row.theory_hint_bundle.to_string(),
            row.best_raw_edge_type,
            row.best_raw_edge_event_id,
            row.best_raw_edge_event_title,
            row.best_raw_edge_cost.map(format_f64).unwrap_or_default(),
            row.best_raw_edge_revenue
                .map(format_f64)
                .unwrap_or_default(),
            row.best_raw_edge_gross_profit
                .map(format_f64)
                .unwrap_or_default(),
            row.best_raw_edge_total_fees
                .map(format_f64)
                .unwrap_or_default(),
            row.best_raw_edge_net_profit
                .map(format_f64)
                .unwrap_or_default(),
            row.best_raw_edge_roi_pct
                .map(format_f64)
                .unwrap_or_default(),
            row.best_raw_edge_prices_from_clob
                .map(|value| value.to_string())
                .unwrap_or_default(),
            format_f64(row.scan_duration_ms),
            row.cumulative_trades_executed.to_string(),
            format_f64(row.cumulative_pnl_usd),
            format_f64(row.cumulative_pnl_pct),
        ];
        let _ = write_fields(&self.scan_summary, &fields, &self.last_error, false);
    }

    pub fn record_latency_budget(&self, row: LatencyBudgetRow) {
        let fields = vec![
            row.timestamp,
            row.scan_id.to_string(),
            row.status,
            row.blockers,
            format_f64(row.scan_duration_ms),
            format_f64(row.max_signal_age_ms),
            format_f64(row.ws_snapshot_wait_ms),
            row.ws_snapshot_ready_tokens.to_string(),
            row.ws_snapshot_total_tokens.to_string(),
            row.ws_snapshot_min_ready_tokens.to_string(),
            row.ws_snapshot_satisfied.to_string(),
            row.quote_tokens_unique_selected.to_string(),
            row.quote_cache_hits.to_string(),
            row.quote_rest_requested.to_string(),
            row.quote_rest_resolved.to_string(),
            row.quote_rest_batches.to_string(),
            format_f64(row.quote_rest_resolution_pct),
            row.quote_deferred_tokens.to_string(),
            row.quote_hard_unresolved_tokens.to_string(),
            row.target_size_rejections.to_string(),
        ];
        let _ = write_fields(&self.latency_budget, &fields, &self.last_error, false);
    }
}

fn compact_candidate_evaluation(row: &CandidateEvaluationRow) -> bool {
    row.selected
        && row
            .selected_rank
            .is_some_and(|rank| rank <= COMPACT_CANDIDATE_EVALUATION_RANK_LIMIT)
}

fn routine_rejection(row: &CandidateRejectionRow) -> bool {
    matches!(
        row.reason.as_str(),
        "no_raw_opportunity" | "quote_not_ready" | "event_lifecycle_cutoff"
    ) || matches!(row.stage.as_str(), "lifecycle" | "quote")
}

impl DiagnosticsCsv {
    fn open(path: &Path, header_fields: &[&str], max_bytes: Option<u64>) -> Result<Self> {
        recover_interrupted_rotation(path)?;
        let header_fields = header_fields
            .iter()
            .map(|field| (*field).to_string())
            .collect::<Vec<_>>();
        let header_bytes = serialized_record_bytes(&header_fields)?;
        let writer = open_csv(path, &header_fields)?;
        let mut csv = Self {
            path: path.to_path_buf(),
            header_fields,
            header_bytes,
            max_bytes: max_bytes.map(|bytes| bytes.max(1)),
            writer: Some(writer),
        };

        if csv.should_rotate() {
            csv.rotate()?;
        }
        Ok(csv)
    }

    fn write_record(&mut self, fields: &[String]) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .with_context(|| format!("csv writer unavailable for {}", self.path.display()))?;
        writer
            .write_record(fields)
            .with_context(|| format!("writing csv record {}", self.path.display()))?;
        writer
            .flush()
            .with_context(|| format!("flushing csv record {}", self.path.display()))?;

        if self.should_rotate() {
            self.rotate()?;
        }
        Ok(())
    }

    fn sync_data(&mut self) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .with_context(|| format!("csv writer unavailable for {}", self.path.display()))?;
        writer
            .flush()
            .with_context(|| format!("flushing csv before fsync {}", self.path.display()))?;
        writer
            .get_ref()
            .inner
            .get_ref()
            .sync_data()
            .with_context(|| format!("fsyncing csv record {}", self.path.display()))
    }

    fn should_rotate(&self) -> bool {
        let Some(max_bytes) = self.max_bytes else {
            return false;
        };
        let bytes_written = self
            .writer
            .as_ref()
            .map(|writer| writer.get_ref().bytes_written)
            .unwrap_or(0);

        bytes_written > self.header_bytes && bytes_written >= max_bytes
    }

    fn rotate(&mut self) -> Result<()> {
        self.writer
            .as_mut()
            .with_context(|| format!("csv writer unavailable for {}", self.path.display()))?
            .flush()
            .with_context(|| format!("flushing csv before rotation {}", self.path.display()))?;
        drop(self.writer.take());

        let previous_path = previous_generation_path(&self.path);
        let backup_path = rotation_backup_path(&self.path);
        let rotation_result = (|| {
            recover_interrupted_rotation(&self.path)?;
            if previous_path.exists() {
                fs::rename(&previous_path, &backup_path).with_context(|| {
                    format!(
                        "staging previous csv generation {} at {}",
                        previous_path.display(),
                        backup_path.display()
                    )
                })?;
            }
            if let Err(error) = fs::rename(&self.path, &previous_path) {
                if backup_path.exists() {
                    fs::rename(&backup_path, &previous_path).with_context(|| {
                        format!(
                            "restoring previous csv generation {} after rotation failed: {error}",
                            previous_path.display()
                        )
                    })?;
                }
                return Err(error).with_context(|| {
                    format!(
                        "rotating csv {} to {}",
                        self.path.display(),
                        previous_path.display()
                    )
                });
            }
            if backup_path.exists() {
                fs::remove_file(&backup_path).with_context(|| {
                    format!("removing staged csv generation {}", backup_path.display())
                })?;
            }
            Ok(())
        })();

        match open_csv(&self.path, &self.header_fields) {
            Ok(writer) => self.writer = Some(writer),
            Err(reopen_error) => return Err(reopen_error),
        }
        rotation_result
    }
}

fn serialized_record_bytes(fields: &[String]) -> Result<u64> {
    let mut bytes = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut bytes);
        writer
            .write_record(fields)
            .context("serializing diagnostics csv header")?;
        writer
            .flush()
            .context("flushing serialized diagnostics csv header")?;
    }
    Ok(bytes.len() as u64)
}

fn previous_generation_path(path: &Path) -> PathBuf {
    let mut path = path.as_os_str().to_os_string();
    path.push(".1");
    PathBuf::from(path)
}

fn rotation_backup_path(path: &Path) -> PathBuf {
    let mut path = previous_generation_path(path).into_os_string();
    path.push(".rotation-backup");
    PathBuf::from(path)
}

fn recover_interrupted_rotation(path: &Path) -> Result<()> {
    let previous_path = previous_generation_path(path);
    let backup_path = rotation_backup_path(path);
    if !backup_path.exists() {
        return Ok(());
    }
    if previous_path.exists() {
        fs::remove_file(&backup_path).with_context(|| {
            format!(
                "removing stale csv rotation backup {}",
                backup_path.display()
            )
        })
    } else {
        fs::rename(&backup_path, &previous_path).with_context(|| {
            format!(
                "restoring interrupted csv rotation backup {} to {}",
                backup_path.display(),
                previous_path.display()
            )
        })
    }
}

fn open_csv(path: &Path, header_fields: &[String]) -> Result<CsvWriter> {
    let (existing_bytes, existed) = match fs::metadata(path) {
        Ok(metadata) => (metadata.len(), true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (0, false),
        Err(error) => {
            return Err(error).with_context(|| format!("reading csv metadata {}", path.display()));
        }
    };
    if existing_bytes > 0 {
        validate_existing_header(path, header_fields)?;
    }
    let needs_header = existing_bytes == 0;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening csv file {}", path.display()))?;
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .from_writer(CountingWriter {
            inner: BufWriter::new(file),
            bytes_written: existing_bytes,
        });
    if needs_header {
        writer
            .write_record(header_fields)
            .with_context(|| format!("writing csv header {}", path.display()))?;
        writer
            .flush()
            .with_context(|| format!("flushing csv header {}", path.display()))?;
        writer
            .get_ref()
            .inner
            .get_ref()
            .sync_all()
            .with_context(|| format!("syncing csv header {}", path.display()))?;
        if !existed {
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
        }
    }
    Ok(writer)
}

fn validate_existing_header(path: &Path, expected: &[String]) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_path(path)
        .with_context(|| format!("opening existing csv header {}", path.display()))?;
    let actual = reader
        .records()
        .next()
        .transpose()
        .with_context(|| format!("reading existing csv header {}", path.display()))?
        .with_context(|| format!("existing csv is missing header {}", path.display()))?;
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(expected)
            .any(|(actual, expected)| actual != expected)
    {
        anyhow::bail!(
            "existing csv header does not match current schema: {}",
            path.display()
        );
    }
    Ok(())
}

fn write_fields(
    writer: &Arc<Mutex<DiagnosticsCsv>>,
    fields: &[String],
    last_error: &Arc<Mutex<Option<String>>>,
    durable: bool,
) -> Result<()> {
    let mut guard = match writer.lock() {
        Ok(guard) => guard,
        Err(_) => {
            let error = "writer lock poisoned".to_string();
            record_diagnostics_error(last_error, error.clone());
            anyhow::bail!(error);
        }
    };
    if let Err(err) =
        guard
            .write_record(fields)
            .and_then(|()| if durable { guard.sync_data() } else { Ok(()) })
    {
        let error = format!("{err:#}");
        record_diagnostics_error(last_error, error.clone());
        anyhow::bail!(error);
    }
    Ok(())
}

fn record_diagnostics_error(last_error: &Arc<Mutex<Option<String>>>, error: String) {
    eprintln!("diagnostics csv write failed: {error}");
    let Ok(mut guard) = last_error.lock() else {
        eprintln!("diagnostics health lock poisoned while recording failure");
        return;
    };
    if guard.is_none() {
        *guard = Some(error);
    }
}

fn format_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.8}")
    } else {
        String::new()
    }
}

pub fn timestamp_now() -> String {
    Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_diagnostics_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{nanos}"))
    }

    fn sample_trade_row() -> TradeLogRow {
        TradeLogRow {
            timestamp: "now".into(),
            scan_id: 1,
            mode: "paper".into(),
            status: "ok".into(),
            pnl_scale: "filled_hedged".into(),
            event_id: "event".into(),
            event_title: "title".into(),
            arb_type: "YES/NO".into(),
            legs_summary: "yes/no".into(),
            target_position_usd: 25.0,
            projected_net_profit: 1.0,
            projected_roi_pct: 4.0,
            filled_cost_usd: Some(25.0),
            conservative_pnl_usd: Some(1.0),
            conservative_roi_pct: Some(4.0),
            planned_basket_units: Some(1.0),
            hedged_basket_units: Some(1.0),
            fill_count: Some(2),
            partial_fill: Some(false),
            parity_ok: Some(true),
            unhedged_notional_usd: Some(0.0),
            prices_from_clob: true,
            note: "paper_attempt_id=test".into(),
        }
    }

    #[test]
    fn diagnostics_logger_quotes_special_csv_fields() {
        let dir = temp_diagnostics_dir("diagnostics-csv");
        let logger = DiagnosticsLogger::new(dir.clone()).expect("logger");

        logger.record_candidate_rejection(CandidateRejectionRow {
            timestamp: "now".into(),
            scan_id: 1,
            pool: "pool".into(),
            event_id: "event".into(),
            event_title: "title, with comma".into(),
            event_slug: "slug".into(),
            market_question: "question".into(),
            arb_type: "YES/NO".into(),
            outcome_side: "yes".into(),
            stage: "stage".into(),
            reason: "quote \"moved\"".into(),
            theory_hint: 0.0,
            quote_ready: false,
            total_cost: None,
            gross_profit: None,
            total_fees: None,
            projected_net_profit: None,
            note: "line one\nline two".into(),
        });

        drop(logger);
        let output = fs::read_to_string(dir.join("candidate_rejections.csv")).expect("csv output");
        assert!(output.contains("\"title, with comma\""));
        assert!(output.contains("\"quote \"\"moved\"\"\""));
        assert!(output.contains("\"line one\nline two\""));
        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn required_trade_write_returns_error_and_latches_unhealthy_state() {
        let dir = temp_diagnostics_dir("diagnostics-required-trade");
        let logger = DiagnosticsLogger::new(dir.clone()).expect("logger");

        logger
            .record_trade(sample_trade_row())
            .expect("durable trade record");
        logger.ensure_healthy().expect("healthy logger");

        logger.trade_log.lock().expect("trade lock").writer = None;
        let error = logger
            .record_trade(sample_trade_row())
            .expect_err("missing writer must fail");
        assert!(error.to_string().contains("writer unavailable"));
        assert!(logger
            .ensure_healthy()
            .expect_err("write failure must latch")
            .to_string()
            .contains("diagnostics logging failed"));

        drop(logger);
        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn diagnostics_directory_lock_is_owned_for_logger_lifetime() {
        let dir = temp_diagnostics_dir("diagnostics-directory-lock");
        let first = DiagnosticsLogger::new(dir.clone()).expect("first logger");
        let error = match DiagnosticsLogger::new(dir.clone()) {
            Ok(_) => panic!("second logger must not share diagnostics directory"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("another scanner holds diagnostics directory lock"));

        let clone = first.clone();
        drop(first);
        assert!(DiagnosticsLogger::new(dir.clone()).is_err());
        drop(clone);
        DiagnosticsLogger::new(dir.clone()).expect("lock released with last logger clone");

        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn managed_csvs_rotate_in_process_and_trades_never_rotate() {
        let dir = temp_diagnostics_dir("diagnostics-rotation");
        let logger = DiagnosticsLogger::new_with_policy_and_max_bytes(
            dir.clone(),
            DiagnosticsPolicy::default(),
            1,
        )
        .expect("logger");

        let managed = [
            (
                &logger.candidate_evaluations,
                "candidate_evaluations.csv",
                "evaluation-record",
            ),
            (
                &logger.candidate_rejections,
                "candidate_rejections.csv",
                "rejection-record",
            ),
            (&logger.scan_summary, "scan_summary.csv", "summary-record"),
            (
                &logger.latency_budget,
                "latency_budget.csv",
                "latency-record",
            ),
        ];

        for (writer, file_name, marker) in managed {
            let field_count = writer.lock().expect("writer lock").header_fields.len();
            let mut fields = vec![String::new(); field_count];
            fields[0] = marker.to_string();
            write_fields(writer, &fields, &logger.last_error, false).expect("managed csv write");
            let current = fs::read_to_string(dir.join(file_name)).expect("current csv");
            let previous = fs::read_to_string(previous_generation_path(&dir.join(file_name)))
                .expect("previous csv generation");
            assert!(current.starts_with("timestamp,"));
            assert_eq!(current.lines().count(), 1);
            assert!(previous.starts_with("timestamp,"));
            assert!(previous.contains(marker));
        }

        let trade_field_count = logger
            .trade_log
            .lock()
            .expect("trade writer lock")
            .header_fields
            .len();
        let mut trade_one = vec![String::new(); trade_field_count];
        trade_one[0] = "trade-one".to_string();
        write_fields(&logger.trade_log, &trade_one, &logger.last_error, true)
            .expect("first trade write");
        let mut trade_two = vec![String::new(); trade_field_count];
        trade_two[0] = "trade-two".to_string();
        write_fields(&logger.trade_log, &trade_two, &logger.last_error, true)
            .expect("second trade write");
        let trades = fs::read_to_string(dir.join("trades.csv")).expect("trades csv");
        assert!(trades.contains("trade-one"));
        assert!(trades.contains("trade-two"));
        assert!(!previous_generation_path(&dir.join("trades.csv")).exists());

        let evaluation_field_count = logger
            .candidate_evaluations
            .lock()
            .expect("evaluation writer lock")
            .header_fields
            .len();
        let mut second_evaluation = vec![String::new(); evaluation_field_count];
        second_evaluation[0] = "evaluation-second-generation".to_string();
        write_fields(
            &logger.candidate_evaluations,
            &second_evaluation,
            &logger.last_error,
            false,
        )
        .expect("second evaluation write");
        let evaluation_previous = fs::read_to_string(previous_generation_path(
            &dir.join("candidate_evaluations.csv"),
        ))
        .expect("replacement previous generation");
        assert!(evaluation_previous.contains("evaluation-second-generation"));
        assert!(!evaluation_previous.contains("evaluation-record"));
        assert!(!dir.join("candidate_evaluations.csv.2").exists());

        drop(logger);
        let reopened = DiagnosticsLogger::new_with_policy_and_max_bytes(
            dir.clone(),
            DiagnosticsPolicy::default(),
            1,
        )
        .expect("reopened logger");
        let evaluation_previous = fs::read_to_string(previous_generation_path(
            &dir.join("candidate_evaluations.csv"),
        ))
        .expect("preserved previous generation");
        assert!(evaluation_previous.contains("evaluation-second-generation"));

        drop(reopened);
        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn oversized_existing_managed_csv_rotates_when_reopened() {
        let dir = temp_diagnostics_dir("diagnostics-existing-rotation");
        fs::create_dir_all(&dir).expect("create temp diagnostics");
        let path = dir.join("candidate_evaluations.csv");
        let headers = ["timestamp", "marker"];

        let mut unbounded = DiagnosticsCsv::open(&path, &headers, None).expect("unbounded csv");
        unbounded
            .write_record(&["now".to_string(), "legacy-record".to_string()])
            .expect("legacy record");
        drop(unbounded);

        let rolling = DiagnosticsCsv::open(&path, &headers, Some(1)).expect("rolling csv");
        let current = fs::read_to_string(&path).expect("current csv");
        let previous = fs::read_to_string(previous_generation_path(&path)).expect("previous csv");
        assert_eq!(current, "timestamp,marker\n");
        assert!(previous.contains("legacy-record"));

        drop(rolling);
        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn existing_csv_header_mismatch_fails_closed() {
        let dir = temp_diagnostics_dir("diagnostics-header-mismatch");
        fs::create_dir_all(&dir).expect("create temp diagnostics");
        let path = dir.join("trades.csv");
        fs::write(&path, "old,header\nvalue,row\n").expect("write stale schema");

        let error = match DiagnosticsLogger::new(dir.clone()) {
            Ok(_) => panic!("stale diagnostics schema must be rejected"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("existing csv header does not match current schema"));

        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn interrupted_rotation_backup_is_recovered_without_losing_previous_generation() {
        let dir = temp_diagnostics_dir("diagnostics-rotation-recovery");
        fs::create_dir_all(&dir).expect("create temp diagnostics");
        let path = dir.join("candidate_evaluations.csv");
        let previous = previous_generation_path(&path);
        let backup = rotation_backup_path(&path);
        fs::write(&path, "timestamp,marker\ncurrent,row\n").expect("write current");
        fs::write(&backup, "timestamp,marker\nprevious,row\n").expect("write backup");

        let csv = DiagnosticsCsv::open(&path, &["timestamp", "marker"], None)
            .expect("recover interrupted rotation");

        assert!(fs::read_to_string(&previous)
            .expect("restored previous")
            .contains("previous,row"));
        assert!(fs::read_to_string(&path)
            .expect("preserved current")
            .contains("current,row"));
        assert!(!backup.exists());

        drop(csv);
        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }

    #[test]
    fn compact_policy_keeps_actionable_diagnostics_only() {
        let dir = temp_diagnostics_dir("diagnostics-compact");
        let logger = DiagnosticsLogger::new_with_policy(
            dir.clone(),
            DiagnosticsPolicy {
                log_all_candidate_evaluations: false,
                log_routine_rejections: false,
            },
        )
        .expect("logger");

        let base_eval = CandidateEvaluationRow {
            timestamp: "now".into(),
            scan_id: 1,
            pool: "neg_yes".into(),
            selected: false,
            selected_rank: None,
            selection_state: "deferred_by_rotation_or_budget".into(),
            event_id: "event".into(),
            event_title: "title".into(),
            event_slug: "slug".into(),
            market_question: String::new(),
            outcome_side: "yes".into(),
            candidate_score: 1.0,
            theory_hint: 0.0,
            tradable_legs: 2,
            total_tokens: 2,
            cached_tokens: 0,
            missing_tokens: 2,
            quote_budget: 100,
            active_token_budget: 100,
        };
        logger.record_candidate_evaluation(base_eval.clone());
        logger.record_candidate_evaluation(CandidateEvaluationRow {
            selected: true,
            selected_rank: Some(1),
            selection_state: "selected".into(),
            event_id: "selected-event".into(),
            ..base_eval.clone()
        });
        logger.record_candidate_evaluation(CandidateEvaluationRow {
            selected: true,
            selected_rank: Some(COMPACT_CANDIDATE_EVALUATION_RANK_LIMIT + 1),
            selection_state: "selected".into(),
            event_id: "rank-tail-event".into(),
            ..base_eval.clone()
        });
        logger.record_candidate_evaluation(CandidateEvaluationRow {
            selection_state: "deferred_dirty".into(),
            event_id: "dirty-event".into(),
            ..base_eval
        });

        let base_rejection = CandidateRejectionRow {
            timestamp: "now".into(),
            scan_id: 1,
            pool: "neg_yes".into(),
            event_id: "event".into(),
            event_title: "title".into(),
            event_slug: "slug".into(),
            market_question: "question".into(),
            arb_type: "YES".into(),
            outcome_side: "yes".into(),
            stage: "raw".into(),
            reason: "no_raw_opportunity".into(),
            theory_hint: 0.0,
            quote_ready: true,
            total_cost: None,
            gross_profit: None,
            total_fees: None,
            projected_net_profit: None,
            note: "routine".into(),
        };
        logger.record_candidate_rejection(base_rejection.clone());
        logger.record_candidate_rejection(CandidateRejectionRow {
            stage: "markout".into(),
            reason: "adverse_selection_markout_blocked".into(),
            note: "actionable".into(),
            ..base_rejection
        });

        drop(logger);
        let evaluations =
            fs::read_to_string(dir.join("candidate_evaluations.csv")).expect("eval csv");
        assert_eq!(evaluations.lines().count(), 2);
        assert!(evaluations.contains("selected-event"));
        assert!(!evaluations.contains("rank-tail-event"));
        assert!(!evaluations.contains("dirty-event"));

        let rejections =
            fs::read_to_string(dir.join("candidate_rejections.csv")).expect("rejection csv");
        assert_eq!(rejections.lines().count(), 2);
        assert!(!rejections.contains("routine"));
        assert!(rejections.contains("actionable"));
        fs::remove_dir_all(dir).expect("remove temp diagnostics");
    }
}
