import { defineConfig, type PreviewServer, type ViteDevServer } from 'vite'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { execFile, spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import fsSync from 'node:fs'
import fs from 'node:fs/promises'
import type { IncomingMessage } from 'node:http'
import path from 'node:path'
import { StringDecoder } from 'node:string_decoder'
import { promisify } from 'node:util'

const run = promisify(execFile)
const diagnosticsDir = path.resolve(
  process.cwd(),
  '..',
  process.env.DIAGNOSTICS_DIR ?? 'runtime_diagnostics',
)
const paperCommand = process.env.EXTERNAL_PAPER_COMMAND ?? 'pm-trader'
const paperDataDir = path.resolve(
  process.cwd(),
  '..',
  process.env.EXTERNAL_PAPER_DATA_DIR ?? '.pm-trader-scanner',
)
const paperAccount = process.env.EXTERNAL_PAPER_ACCOUNT ?? 'smoke-arb'
const csvFiles = [
  'scan_summary.csv',
  'latency_budget.csv',
  'trades.csv',
  'candidate_evaluations.csv',
  'candidate_rejections.csv',
]
const readinessJsonFiles = {
  live: 'live_readiness_report.json',
  combo: 'combo_rfq_route_promotion_report.json',
  codeCeiling: 'live_code_ceiling_report.json',
  unblockPlan: 'live-unblock-plan.json',
  paperLiveParityAudit: 'paper-live-parity-audit.json',
  bundleManifest: 'readiness-bundle-manifest.json',
  tradeResult: 'trade_readiness_result.json',
  operatorPreflightManifest: 'live-operator-preflight-manifest.json',
  activationPacket: 'live-activation-packet.json',
} as const
const scannerCwd = path.resolve(process.cwd(), '..')
const scannerIntervalSeconds = process.env.SCAN_INTERVAL_SECONDS ?? '1'
const configuredFillPollTimeoutSeconds = Number(
  process.env.LIVE_FILL_POLL_TIMEOUT_SECONDS ?? '30',
)
const scannerDrainTimeoutMs = Math.max(
  60_000,
  ((Number.isFinite(configuredFillPollTimeoutSeconds) && configuredFillPollTimeoutSeconds >= 0
    ? configuredFillPollTimeoutSeconds
    : 30) +
    30) *
    1_000,
)
const scannerPidFile = path.join(diagnosticsDir, 'scanner.pid')
const scannerLockFile = path.join(diagnosticsDir, 'scanner.lock')
const diagnosticsTailBytes = Math.max(
  64_000,
  Number(process.env.DIAGNOSTICS_TAIL_BYTES ?? 250_000) || 250_000,
)

type ScannerExit = {
  code: number | null
  signal: NodeJS.Signals | null
  at: string
}
type ScannerPidRecord = {
  pid: number
  startedAt: string | null
  ownerPid?: number
  ownerToken?: string
  binaryPath?: string
  binarySha256?: string
}
type ScannerLaunchContract = {
  binaryPath: string
  binarySha256: string
  readinessManifestPath: string
  buildProvenancePath: string
  profitCompatibilityFingerprint: string
}

let scannerProcess: ChildProcessWithoutNullStreams | null = null
let scannerStartedAt: string | null = null
let scannerStopping = false
let scannerLastExit: ScannerExit | null = null
let scannerLog: string[] = []
let scannerLaunchContract: ScannerLaunchContract | null = null
let scannerLaunchError: string | null = null
let scannerLifecycleQueue: Promise<void> = Promise.resolve()

async function readCsv(name: string) {
  const filePath = path.join(diagnosticsDir, name)
  try {
    const stats = await fs.stat(filePath)
    if (stats.size <= diagnosticsTailBytes) return await fs.readFile(filePath, 'utf8')

    const handle = await fs.open(filePath, 'r')
    try {
      const headerSize = Math.min(8192, stats.size)
      const headerBuffer = Buffer.alloc(headerSize)
      await handle.read(headerBuffer, 0, headerSize, 0)
      const header = headerBuffer.toString('utf8').split(/\r?\n/, 1)[0] ?? ''

      const tailSize = Math.min(diagnosticsTailBytes, stats.size)
      const tailBuffer = Buffer.alloc(tailSize)
      await handle.read(tailBuffer, 0, tailSize, stats.size - tailSize)
      const tail = tailBuffer.toString('utf8').replace(/^[\s\S]*?\r?\n/, '')
      return `${header}\n${tail}`
    } finally {
      await handle.close()
    }
  } catch {
    return ''
  }
}

function journalStatus(row: Record<string, unknown>) {
  const value = row.status
  return typeof value === 'string' ? value.toLowerCase() : ''
}

function isLiveJournalEvidence(row: Record<string, unknown>) {
  const status = journalStatus(row)
  return status.length === 0 || !status.startsWith('blocked')
}

function numericValue(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const parsed = Number(value.replace(/[$,%+,]/g, '').trim())
    if (Number.isFinite(parsed)) return parsed
  }
  return undefined
}

type EvidenceCount = {
  count: number
  complete: boolean
  malformedRows: number
  error: string | null
}
type JsonlEvidenceCache = {
  dev: number
  ino: number
  position: number
  decoder: StringDecoder
  remainder: string
  count: number
  malformedRows: number
  invalidated: string | null
}

const jsonlEvidenceCaches = new Map<string, JsonlEvidenceCache>()

function evidenceError(error: unknown) {
  return error instanceof Error ? error.message : String(error)
}

async function streamVerifiedFileRange(
  filePath: string,
  expected: fsSync.Stats,
  start: number,
  consume: (buffer: Buffer) => void,
) {
  if (expected.size <= start) return
  const handle = await fs.open(filePath, 'r')
  try {
    const opened = await handle.stat()
    if (opened.dev !== expected.dev || opened.ino !== expected.ino || opened.size < expected.size) {
      throw new Error(`${path.basename(filePath)} changed between stat and open`)
    }
    const stream = handle.createReadStream({
      start,
      end: expected.size - 1,
      autoClose: false,
    })
    for await (const chunk of stream) {
      consume(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
    }
  } finally {
    await handle.close()
  }
}

async function countJsonlRows(
  name: string,
  includeRow: (row: Record<string, unknown>) => boolean,
): Promise<EvidenceCount> {
  const filePath = path.join(diagnosticsDir, name)
  let stats
  try {
    stats = await fs.stat(filePath)
  } catch (error) {
    return {
      count: 0,
      complete: false,
      malformedRows: 0,
      error: `${name} unavailable: ${evidenceError(error)}`,
    }
  }

  let cache = jsonlEvidenceCaches.get(name)
  if (!cache) {
    cache = {
      dev: stats.dev,
      ino: stats.ino,
      position: 0,
      decoder: new StringDecoder('utf8'),
      remainder: '',
      count: 0,
      malformedRows: 0,
      invalidated: null,
    }
    jsonlEvidenceCaches.set(name, cache)
  } else if (cache.dev !== stats.dev || cache.ino !== stats.ino || stats.size < cache.position) {
    cache.invalidated = `${name} was replaced or truncated during this dashboard session`
  }

  if (cache.invalidated) {
    return {
      count: cache.count,
      complete: false,
      malformedRows: cache.malformedRows,
      error: cache.invalidated,
    }
  }

  try {
    if (stats.size > cache.position) {
      await streamVerifiedFileRange(filePath, stats, cache.position, (buffer) => {
        cache.position += buffer.length
        const lines = `${cache.remainder}${cache.decoder.write(buffer)}`.split(/\r?\n/)
        cache.remainder = lines.pop() ?? ''
        for (const line of lines) {
          const trimmed = line.trim()
          if (!trimmed) continue
          try {
            const row = JSON.parse(trimmed)
            if (!row || typeof row !== 'object' || Array.isArray(row)) {
              cache.malformedRows += 1
            } else if (includeRow(row as Record<string, unknown>)) {
              cache.count += 1
            }
          } catch {
            cache.malformedRows += 1
          }
        }
      })
    }
  } catch (error) {
    return {
      count: cache.count,
      complete: false,
      malformedRows: cache.malformedRows,
      error: `${name} read failed: ${evidenceError(error)}`,
    }
  }

  const partialRow = cache.remainder.trim().length > 0
  return {
    count: cache.count,
    complete: cache.malformedRows === 0 && !partialRow,
    malformedRows: cache.malformedRows,
    error:
      cache.malformedRows > 0
        ? `${name} contains ${cache.malformedRows} malformed row(s)`
        : partialRow
          ? `${name} ends with an incomplete row`
          : null,
  }
}

function parseCsv(text = ''): Record<string, string>[] {
  const rows: string[][] = []
  let field = ''
  let row: string[] = []
  let quoted = false

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i]
    const next = text[i + 1]
    if (char === '"' && quoted && next === '"') {
      field += '"'
      i += 1
    } else if (char === '"') {
      quoted = !quoted
    } else if (char === ',' && !quoted) {
      row.push(field)
      field = ''
    } else if ((char === '\n' || char === '\r') && !quoted) {
      if (char === '\r' && next === '\n') i += 1
      row.push(field)
      if (row.some((cell) => cell.length)) rows.push(row)
      field = ''
      row = []
    } else {
      field += char
    }
  }

  if (field || row.length) {
    row.push(field)
    rows.push(row)
  }

  const [header, ...body] = rows
  if (!header) return []
  return body.map((cells) =>
    Object.fromEntries(header.map((key, index) => [key, cells[index] ?? ''])),
  )
}

function isLiveSubmissionRow(row: Record<string, string>) {
  const mode = (row.mode || '').toLowerCase()
  const status = (row.status || '').toLowerCase()
  if (mode === 'live') return !status.startsWith('blocked')
  if (mode === 'live_combo_rfq') return !status.startsWith('blocked')
  return false
}

function isPaperExecutionRow(row: Record<string, string>) {
  const mode = (row.mode || '').toLowerCase()
  const status = (row.status || '').toLowerCase()
  const parityOk = (row.parity_ok || '').toLowerCase()
  return mode === 'paper' && status === 'ok' && parityOk !== 'false'
}

async function readJsonPath(filePath: string) {
  try {
    return JSON.parse(await fs.readFile(filePath, 'utf8'))
  } catch (error) {
    return {
      unavailable: true,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

async function readJsonFile(name: string) {
  return readJsonPath(path.join(diagnosticsDir, name))
}

async function readJsonNearby(name: string) {
  const candidates = [
    path.join(diagnosticsDir, name),
    path.join(path.dirname(diagnosticsDir), name),
  ]
  for (const filePath of Array.from(new Set(candidates))) {
    try {
      return JSON.parse(await fs.readFile(filePath, 'utf8'))
    } catch {
      continue
    }
  }
  return readJsonPath(candidates[0])
}

async function readOperatorPreflightManifest() {
  const manifestName = readinessJsonFiles.operatorPreflightManifest
  const explicitManifest =
    process.env.OPERATOR_PREFLIGHT_MANIFEST ?? process.env.LIVE_OPERATOR_PREFLIGHT_MANIFEST
  const explicitRoot = process.env.OPERATOR_PREFLIGHT_ROOT ?? process.env.LIVE_OPERATOR_PREFLIGHT_ROOT
  const candidates = [
    explicitManifest,
    explicitRoot ? path.join(explicitRoot, manifestName) : null,
    path.join(diagnosticsDir, manifestName),
    path.join(path.dirname(diagnosticsDir), manifestName),
  ]
    .filter((candidate): candidate is string => Boolean(candidate))
    .map((candidate) => path.resolve(candidate))

  for (const filePath of Array.from(new Set(candidates))) {
    try {
      const manifest = JSON.parse(await fs.readFile(filePath, 'utf8'))
      const auditPath =
        typeof manifest?.env_audit?.path === 'string'
          ? manifest.env_audit.path
          : path.join(path.dirname(filePath), 'live-env-audit.json')
      const envAudit = await readJsonPath(auditPath)
      return {
        ...manifest,
        liveEnvAudit: envAudit.unavailable ? null : envAudit,
      }
    } catch {
      continue
    }
  }
  return readJsonPath(candidates[0] ?? path.join(diagnosticsDir, manifestName))
}

type ActivationPacketCache = {
  signature: string
  value: Record<string, unknown>
}

let activationPacketCache: ActivationPacketCache | null = null
let activationPacketRequest: Promise<Record<string, unknown>> | null = null

async function activationPacketSignature(
  packetPath: string,
  packet: Record<string, unknown>,
) {
  const artifacts =
    packet.artifacts && typeof packet.artifacts === 'object'
      ? Object.values(packet.artifacts).filter((value): value is string => typeof value === 'string')
      : []
  const paths = Array.from(new Set([packetPath, ...artifacts])).sort()
  const identities = await Promise.all(
    paths.map(async (filePath) => {
      const stats = await fs.stat(filePath)
      return `${filePath}:${stats.dev}:${stats.ino}:${stats.size}:${stats.mtimeMs}`
    }),
  )
  return identities.join('|')
}

async function loadActivationPacket(): Promise<Record<string, unknown>> {
  const explicitPacket = process.env.ACTIVATION_PACKET_PATH
  if (explicitPacket && !path.isAbsolute(explicitPacket)) {
    return {
      unavailable: true,
      error: 'ACTIVATION_PACKET_PATH must be absolute',
      verification: { ok: false, error: 'non-absolute activation packet path' },
    }
  }
  const requestedPath = explicitPacket ?? path.join(diagnosticsDir, readinessJsonFiles.activationPacket)

  let packetPath: string
  let packet: Record<string, unknown>
  try {
    packetPath = await fs.realpath(requestedPath)
    if (packetPath !== path.resolve(requestedPath)) {
      throw new Error('activation packet path must be canonical and may not be a symlink')
    }
    const parsed = JSON.parse(await fs.readFile(packetPath, 'utf8'))
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('activation packet must be a JSON object')
    }
    packet = parsed as Record<string, unknown>
  } catch (error) {
    return {
      unavailable: true,
      error: evidenceError(error),
      verification: { ok: false, error: evidenceError(error) },
    }
  }

  let signature: string
  try {
    signature = await activationPacketSignature(packetPath, packet)
  } catch (error) {
    return {
      ...packet,
      can_enable_live: false,
      status: 'unverified',
      packet_file: packetPath,
      verification: { ok: false, error: `artifact binding failed: ${evidenceError(error)}` },
    }
  }
  if (activationPacketCache?.signature === signature) return activationPacketCache.value

  const verifier = path.join(scannerCwd, 'scripts', 'verify-live-activation-packet.sh')
  try {
    await run(verifier, [packetPath], {
      cwd: scannerCwd,
      timeout: 120_000,
      maxBuffer: 4 * 1024 * 1024,
    })
    const value = {
      ...packet,
      packet_file: packetPath,
      verification: {
        ok: true,
        verified_at: new Date().toISOString(),
        verifier,
      },
    }
    activationPacketCache = { signature, value }
    return value
  } catch (error) {
    const value = {
      ...packet,
      can_enable_live: false,
      status: 'unverified',
      gate: {
        ...(packet.gate && typeof packet.gate === 'object' ? packet.gate : {}),
        ok: false,
      },
      packet_file: packetPath,
      verification: {
        ok: false,
        error: `activation packet verifier failed: ${evidenceError(error)}`,
      },
    }
    activationPacketCache = { signature, value }
    return value
  }
}

function readActivationPacket() {
  if (activationPacketRequest) return activationPacketRequest
  const request = loadActivationPacket()
  activationPacketRequest = request
  void request.finally(() => {
    if (activationPacketRequest === request) activationPacketRequest = null
  })
  return request
}

async function paperJson(command: string) {
  const { stdout } = await run(paperCommand, [
    '--data-dir',
    paperDataDir,
    '--account',
    paperAccount,
    command,
  ])
  return JSON.parse(stdout)
}

function numericStat(stdout: string, pattern: RegExp) {
  const match = stdout.match(pattern)
  if (!match?.[1]) return undefined
  const normalized = match[1].replace(/[$,%+,]/g, '').trim()
  const value = Number(normalized)
  return Number.isFinite(value) ? value : undefined
}

async function paperStatsJson() {
  const { stdout } = await run(paperCommand, [
    '--data-dir',
    paperDataDir,
    '--account',
    paperAccount,
    'stats',
  ])
  try {
    return JSON.parse(stdout)
  } catch {
    return {
      data: {
        roi_pct: numericStat(stdout, /ROI:\s*([+$\d.,%-]+)/i),
        win_rate: numericStat(stdout, /Win Rate:\s*([+$\d.,%-]+)/i),
        total_trades: numericStat(stdout, /Trades:\s*([+$\d.,%-]+)/i),
        pnl: numericStat(stdout, /P&L:\s*([+$\d.,%-]+)/i),
        total_value: numericStat(stdout, /Portfolio:\s*([+$\d.,%-]+)/i),
      },
    }
  }
}

async function loadPaperStats() {
  try {
    const [balance, stats] = await Promise.all([paperJson('balance'), paperStatsJson()])
    return {
      ok: true,
      account: paperAccount,
      dataDir: paperDataDir,
      balance: balance.data,
      stats: stats.data,
    }
  } catch (error) {
    return {
      ok: false,
      account: paperAccount,
      dataDir: paperDataDir,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

let paperStatsRequest: ReturnType<typeof loadPaperStats> | null = null

function paperStats() {
  if (paperStatsRequest) return paperStatsRequest

  const request = loadPaperStats()
  paperStatsRequest = request
  void request.then(
    () => {
      if (paperStatsRequest === request) paperStatsRequest = null
    },
    () => {
      if (paperStatsRequest === request) paperStatsRequest = null
    },
  )
  return request
}

function readinessStateFromChecks(
  checks: Array<{ key?: string; state?: string; detail?: string }> | undefined,
) {
  if (!checks?.length) return 'unknown'
  return checks.every((check) => check.state === 'ready') ? 'ready' : 'blocked'
}

function readinessBlockers(
  checks: Array<{ key?: string; state?: string; detail?: string }> | undefined,
) {
  return (checks ?? [])
    .filter((check) => check.state !== 'ready')
    .slice(0, 4)
    .map((check) => `${check.key ?? 'check'}: ${check.detail ?? check.state ?? 'unknown'}`)
}

function liveAction(key?: string) {
  switch (key) {
    case 'live_route_matrix':
      return 'Promote one live route; Combo/RFQ requires route flag and promotion gates.'
    case 'clob_engine_mode':
      return 'Run live diagnostics until engine mode has a fresh normal observation.'
    case 'protocol_drift':
      return 'Resolve protocol drift report before live submit.'
    case 'user_channel_config':
      return 'Enable authenticated user channel with LIVE_USER_WS_ENABLED=true.'
    case 'user_channel_ready':
      return 'Start user-channel supervision and wait for fresh connected status.'
    case 'closeout_execution':
      return 'Enable non-dry-run closeout only after closeout action preflight is safe.'
    case 'erc1155_operator_approval':
      return 'Configure live account and exchange spender, then pass approval probe.'
    case 'account_identity':
      return 'Set POLYMARKET_PRIVATE_KEY and matching live signature/funder settings.'
    case 'accounting_snapshot':
      return 'Pass live accounting snapshot with no disallowed retained positions.'
    case 'native_pol_balance':
      return 'Fund enough POL for required gas path or prove gasless proxy mode.'
    case 'authenticated_clob_client':
      return 'Authenticate CLOB SDK client with live key and signature settings.'
    case 'closed_only_status':
      return 'Pass authenticated CLOB closed-only account probe.'
    case 'pusd_balance':
      return 'Fund enough PUSD collateral for configured trade size.'
    case 'exchange_v3_allowance':
      return 'Set exchange v3 address and verify allowance.'
    case 'clean_startup_account':
      return 'Clear open orders and retained positions before live startup.'
    default:
      return key?.startsWith('pusd_allowance')
        ? 'Approve or verify enough PUSD allowance for required exchange.'
        : 'Inspect readiness detail and clear this gate before live submit.'
  }
}

function mentionedEnvs(detail = '') {
  const matches = detail.match(/[A-Z][A-Z0-9_]{2,}/g) ?? []
  return matches
    .map((item) => item.replace(/_+$/, ''))
    .filter((item) => /^(LIVE|COMBO|POLYMARKET|POLYGON|ONCHAIN|SETTLEMENT|CLOB)_/.test(item))
}

function liveNextActions(checks: Array<{ key?: string; state?: string; detail?: string }>) {
  return checks
    .filter((check) => check.state !== 'ready')
    .map((check) => ({
      key: check.key ?? 'check',
      state: check.state ?? 'unknown',
      action: liveAction(check.key),
      mentionedEnvs: mentionedEnvs(check.detail),
    }))
}

function codeBlockerSummary(blocker: { key?: string; detail?: string }) {
  return `${blocker.key ?? 'code'}: ${blocker.detail ?? 'unknown'}`
}

function numberField(row: Record<string, string> | undefined, key: string) {
  const value = Number(row?.[key] ?? 0)
  return Number.isFinite(value) ? value : 0
}

function timestampAgeMs(row: Record<string, string> | undefined) {
  const timestamp = row?.timestamp
  if (!timestamp) return null
  const parsed = Date.parse(timestamp)
  if (!Number.isFinite(parsed)) return null
  return Math.max(0, Date.now() - parsed)
}

type RawEdge = {
  scan_id: number
  type: string | null
  event_id: string | null
  event_title: string | null
  cost: number
  revenue: number
  net_profit: number
  roi_pct: number
  raw_candidate_total: number
  opportunities_found: number
}
type RawEdgeHistory = {
  scan_rows: number
  max_best_raw_edge: RawEdge | null
  positive_best_raw_edge_rows: number
  missed_positive_raw_edge_rows: number
  first_missed_positive_raw_edge: RawEdge | null
  no_missed_positive_raw_edge: boolean
  complete: boolean
  malformed_rows: number
  error: string | null
  sources: string[]
}

type CsvParserState = {
  field: string
  row: string[]
  quoted: boolean
  pendingQuote: boolean
  skipLf: boolean
}
type ScanHistoryAccumulator = {
  headerIndexes: Map<string, number> | null
  headerLength: number
  scanRows: number
  maxBestRawEdge: RawEdge | null
  positiveBestRawEdgeRows: number
  missedPositiveRawEdgeRows: number
  firstMissedPositiveRawEdge: RawEdge | null
  malformedRows: number
}
type ScanHistoryCursor = {
  filePath: string
  dev: number
  ino: number
  position: number
  decoder: StringDecoder
  parser: CsvParserState
}
type ScanHistoryCache = {
  current: ScanHistoryCursor
  previousIdentity: string | null
  accumulator: ScanHistoryAccumulator
  completeFromStart: boolean
  invalidated: string | null
}

function createCsvParserState(): CsvParserState {
  return {
    field: '',
    row: [],
    quoted: false,
    pendingQuote: false,
    skipLf: false,
  }
}

function createScanHistoryCursor(filePath: string, stats: fsSync.Stats): ScanHistoryCursor {
  return {
    filePath,
    dev: stats.dev,
    ino: stats.ino,
    position: 0,
    decoder: new StringDecoder('utf8'),
    parser: createCsvParserState(),
  }
}

function createScanHistoryAccumulator(): ScanHistoryAccumulator {
  return {
    headerIndexes: null,
    headerLength: 0,
    scanRows: 0,
    maxBestRawEdge: null,
    positiveBestRawEdgeRows: 0,
    missedPositiveRawEdgeRows: 0,
    firstMissedPositiveRawEdge: null,
    malformedRows: 0,
  }
}

function fileIdentity(stats: fsSync.Stats) {
  return `${stats.dev}:${stats.ino}`
}

function parserHasPartialRow(parser: CsvParserState) {
  return (
    parser.quoted ||
    parser.pendingQuote ||
    parser.field.length > 0 ||
    parser.row.length > 0
  )
}

function consumeCsvChunk(
  state: CsvParserState,
  text: string,
  onRow: (row: string[]) => void,
) {
  let index = 0
  while (index < text.length) {
    const char = text[index]

    if (state.skipLf) {
      state.skipLf = false
      if (char === '\n') {
        index += 1
        continue
      }
    }
    if (state.pendingQuote) {
      state.pendingQuote = false
      if (char === '"') {
        state.field += '"'
        index += 1
        continue
      }
      state.quoted = false
      continue
    }
    if (state.quoted) {
      if (char === '"') state.pendingQuote = true
      else state.field += char
      index += 1
      continue
    }
    if (char === '"') {
      state.quoted = true
    } else if (char === ',') {
      state.row.push(state.field)
      state.field = ''
    } else if (char === '\n' || char === '\r') {
      state.row.push(state.field)
      if (state.row.some((cell) => cell.length)) onRow(state.row)
      state.field = ''
      state.row = []
      state.skipLf = char === '\r'
    } else {
      state.field += char
    }
    index += 1
  }
}

function consumeScanHistoryRow(accumulator: ScanHistoryAccumulator, row: string[]) {
  if (row[0] === 'timestamp' && row.includes('scan_id')) {
    accumulator.headerIndexes = new Map(row.map((name, index) => [name, index]))
    accumulator.headerLength = row.length
    const requiredFields = [
      'scan_id',
      'raw_yes_candidates',
      'raw_no_candidates',
      'raw_bundle_candidates',
      'raw_ranked_candidates',
      'opportunities_found',
      'best_raw_edge_net_profit',
    ]
    if (requiredFields.some((field) => !accumulator.headerIndexes?.has(field))) {
      accumulator.malformedRows += 1
      accumulator.headerIndexes = null
    }
    return
  }
  if (!accumulator.headerIndexes || row.length !== accumulator.headerLength) {
    accumulator.malformedRows += 1
    return
  }

  const field = (name: string) => row[accumulator.headerIndexes?.get(name) ?? -1] ?? ''
  const number = (name: string) => {
    const parsed = Number(field(name) || 0)
    return Number.isFinite(parsed) ? parsed : 0
  }
  const edge: RawEdge = {
    scan_id: Math.trunc(number('scan_id')),
    type: field('best_raw_edge_type') || null,
    event_id: field('best_raw_edge_event_id') || null,
    event_title: field('best_raw_edge_event_title') || null,
    cost: number('best_raw_edge_cost'),
    revenue: number('best_raw_edge_revenue'),
    net_profit: number('best_raw_edge_net_profit'),
    roi_pct: number('best_raw_edge_roi_pct'),
    raw_candidate_total:
      Math.trunc(number('raw_yes_candidates')) +
      Math.trunc(number('raw_no_candidates')) +
      Math.trunc(number('raw_bundle_candidates')) +
      Math.trunc(number('raw_ranked_candidates')),
    opportunities_found: Math.trunc(number('opportunities_found')),
  }

  accumulator.scanRows += 1
  if (
    accumulator.maxBestRawEdge === null ||
    edge.net_profit > accumulator.maxBestRawEdge.net_profit
  ) {
    accumulator.maxBestRawEdge = edge
  }
  if (edge.net_profit > 0) accumulator.positiveBestRawEdgeRows += 1
  if (edge.net_profit > 0 && edge.raw_candidate_total === 0 && edge.opportunities_found === 0) {
    accumulator.missedPositiveRawEdgeRows += 1
    accumulator.firstMissedPositiveRawEdge ??= edge
  }
}

function scanHistorySnapshot(
  accumulator: ScanHistoryAccumulator,
  complete: boolean,
  error: string | null,
  sources: string[],
): RawEdgeHistory {
  const trustworthy = complete && accumulator.malformedRows === 0 && accumulator.scanRows > 0
  return {
    scan_rows: accumulator.scanRows,
    max_best_raw_edge: accumulator.maxBestRawEdge,
    positive_best_raw_edge_rows: accumulator.positiveBestRawEdgeRows,
    missed_positive_raw_edge_rows: accumulator.missedPositiveRawEdgeRows,
    first_missed_positive_raw_edge: accumulator.firstMissedPositiveRawEdge,
    no_missed_positive_raw_edge: trustworthy && accumulator.missedPositiveRawEdgeRows === 0,
    complete: trustworthy,
    malformed_rows: accumulator.malformedRows,
    error,
    sources,
  }
}

let scanHistoryCache: ScanHistoryCache | null = null
let scanHistoryRefresh: Promise<RawEdgeHistory> | null = null

async function refreshScanHistory(): Promise<RawEdgeHistory> {
  const filePath = path.join(diagnosticsDir, 'scan_summary.csv')
  const previousPath = `${filePath}.1`
  let stats: fsSync.Stats
  let previousStats: fsSync.Stats | null = null
  try {
    stats = await fs.stat(filePath)
    try {
      previousStats = await fs.stat(previousPath)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    }
  } catch (error) {
    const accumulator = scanHistoryCache?.accumulator ?? createScanHistoryAccumulator()
    return scanHistorySnapshot(
      accumulator,
      false,
      `scan history unavailable: ${evidenceError(error)}`,
      scanHistoryCache ? [scanHistoryCache.current.filePath] : [],
    )
  }

  if (!scanHistoryCache) {
    const accumulator = createScanHistoryAccumulator()
    const current = createScanHistoryCursor(filePath, stats)
    scanHistoryCache = {
      current,
      previousIdentity: previousStats ? fileIdentity(previousStats) : null,
      accumulator,
      completeFromStart: previousStats === null,
      invalidated: null,
    }
    if (previousStats) {
      const previous = createScanHistoryCursor(previousPath, previousStats)
      try {
        await streamScanHistory(previous, previousStats, accumulator)
      } catch (error) {
        return scanHistorySnapshot(
          accumulator,
          false,
          `scan history previous generation read failed: ${evidenceError(error)}`,
          [previousPath, filePath],
        )
      }
    }
  }
  const cache = scanHistoryCache
  const currentIdentity = fileIdentity(stats)
  const cachedIdentity = `${cache.current.dev}:${cache.current.ino}`
  const previousIdentity = previousStats ? fileIdentity(previousStats) : null

  if (currentIdentity !== cachedIdentity) {
    if (previousIdentity === cachedIdentity) {
      cache.previousIdentity = previousIdentity
      cache.current = createScanHistoryCursor(filePath, stats)
    } else {
      cache.invalidated = 'scan_summary.csv was replaced without a verifiable .1 rotation'
    }
  } else if (stats.size < cache.current.position) {
    cache.invalidated = 'scan_summary.csv was truncated during this dashboard session'
  } else if (cache.previousIdentity !== previousIdentity) {
    cache.invalidated = 'scan_summary.csv.1 changed without a matching current-file rotation'
  }

  const sources = previousStats ? [previousPath, filePath] : [filePath]
  if (cache.invalidated) {
    return scanHistorySnapshot(cache.accumulator, false, cache.invalidated, sources)
  }

  try {
    await streamScanHistory(cache.current, stats, cache.accumulator)
  } catch (error) {
    return scanHistorySnapshot(
      cache.accumulator,
      false,
      `scan history read failed: ${evidenceError(error)}`,
      sources,
    )
  }
  const partial = parserHasPartialRow(cache.current.parser)
  const complete = cache.completeFromStart && !partial
  const error = !cache.completeFromStart
    ? 'scan history begins with a retained .1 generation; older rotations cannot be excluded'
    : partial
      ? 'scan_summary.csv ends with an incomplete row'
      : cache.accumulator.malformedRows > 0
        ? `scan history contains ${cache.accumulator.malformedRows} malformed row(s)`
        : null
  return scanHistorySnapshot(cache.accumulator, complete, error, sources)
}

async function streamScanHistory(
  cursor: ScanHistoryCursor,
  stats: fsSync.Stats,
  accumulator: ScanHistoryAccumulator,
) {
  await streamVerifiedFileRange(cursor.filePath, stats, cursor.position, (buffer) => {
    cursor.position += buffer.length
    consumeCsvChunk(cursor.parser, cursor.decoder.write(buffer), (row) =>
      consumeScanHistoryRow(accumulator, row),
    )
  })
}

async function readScanHistory() {
  if (scanHistoryRefresh) return scanHistoryRefresh
  const refresh = refreshScanHistory()
  scanHistoryRefresh = refresh
  try {
    return await refresh
  } finally {
    if (scanHistoryRefresh === refresh) scanHistoryRefresh = null
  }
}

type TradeEvidence = EvidenceCount & {
  paperExecutionRows: number
}
type TradeEvidenceCache = {
  dev: number
  ino: number
  position: number
  decoder: StringDecoder
  parser: CsvParserState
  headerIndexes: Map<string, number> | null
  headerLength: number
  liveSubmissionRows: number
  paperExecutionRows: number
  malformedRows: number
  invalidated: string | null
}

let tradeEvidenceCache: TradeEvidenceCache | null = null
let tradeEvidenceRefresh: Promise<TradeEvidence> | null = null

function tradeEvidenceSnapshot(cache: TradeEvidenceCache, error: string | null): TradeEvidence {
  const partial = parserHasPartialRow(cache.parser)
  const complete =
    error === null &&
    cache.headerIndexes !== null &&
    cache.malformedRows === 0 &&
    !partial
  return {
    count: cache.liveSubmissionRows,
    paperExecutionRows: cache.paperExecutionRows,
    complete,
    malformedRows: cache.malformedRows,
    error:
      error ??
      (cache.headerIndexes === null
        ? 'trades.csv header is missing or invalid'
        : cache.malformedRows > 0
          ? `trades.csv contains ${cache.malformedRows} malformed row(s)`
          : partial
            ? 'trades.csv ends with an incomplete row'
            : null),
  }
}

function consumeTradeEvidenceRow(cache: TradeEvidenceCache, row: string[]) {
  if (row[0] === 'timestamp' && row.includes('mode') && row.includes('status')) {
    cache.headerIndexes = new Map(row.map((name, index) => [name, index]))
    cache.headerLength = row.length
    return
  }
  if (!cache.headerIndexes || row.length !== cache.headerLength) {
    cache.malformedRows += 1
    return
  }
  const record = Object.fromEntries(
    Array.from(cache.headerIndexes.entries(), ([name, index]) => [name, row[index] ?? '']),
  )
  if (isLiveSubmissionRow(record)) cache.liveSubmissionRows += 1
  if (isPaperExecutionRow(record)) cache.paperExecutionRows += 1
}

async function refreshTradeEvidence(): Promise<TradeEvidence> {
  const filePath = path.join(diagnosticsDir, 'trades.csv')
  let stats: fsSync.Stats
  try {
    stats = await fs.stat(filePath)
  } catch (error) {
    return {
      count: tradeEvidenceCache?.liveSubmissionRows ?? 0,
      paperExecutionRows: tradeEvidenceCache?.paperExecutionRows ?? 0,
      complete: false,
      malformedRows: tradeEvidenceCache?.malformedRows ?? 0,
      error: `trades.csv unavailable: ${evidenceError(error)}`,
    }
  }

  if (!tradeEvidenceCache) {
    tradeEvidenceCache = {
      dev: stats.dev,
      ino: stats.ino,
      position: 0,
      decoder: new StringDecoder('utf8'),
      parser: createCsvParserState(),
      headerIndexes: null,
      headerLength: 0,
      liveSubmissionRows: 0,
      paperExecutionRows: 0,
      malformedRows: 0,
      invalidated: null,
    }
  } else if (
    tradeEvidenceCache.dev !== stats.dev ||
    tradeEvidenceCache.ino !== stats.ino ||
    stats.size < tradeEvidenceCache.position
  ) {
    tradeEvidenceCache.invalidated = 'trades.csv was replaced or truncated during this dashboard session'
  }
  const cache = tradeEvidenceCache
  if (cache.invalidated) return tradeEvidenceSnapshot(cache, cache.invalidated)

  try {
    if (stats.size > cache.position) {
      await streamVerifiedFileRange(filePath, stats, cache.position, (buffer) => {
        cache.position += buffer.length
        consumeCsvChunk(cache.parser, cache.decoder.write(buffer), (row) =>
          consumeTradeEvidenceRow(cache, row),
        )
      })
    }
  } catch (error) {
    return tradeEvidenceSnapshot(cache, `trades.csv read failed: ${evidenceError(error)}`)
  }
  return tradeEvidenceSnapshot(cache, null)
}

function readTradeEvidence() {
  if (tradeEvidenceRefresh) return tradeEvidenceRefresh
  const refresh = refreshTradeEvidence()
  tradeEvidenceRefresh = refresh
  void refresh.finally(() => {
    if (tradeEvidenceRefresh === refresh) tradeEvidenceRefresh = null
  })
  return refresh
}

function withCurrentScanHistory(
  paperLiveParityAudit: Record<string, unknown>,
  rawEdgeHistory: RawEdgeHistory,
) {
  if (paperLiveParityAudit.unavailable) return paperLiveParityAudit

  const scanner =
    paperLiveParityAudit.scanner && typeof paperLiveParityAudit.scanner === 'object'
      ? paperLiveParityAudit.scanner
      : {}
  const verdict =
    paperLiveParityAudit.verdict && typeof paperLiveParityAudit.verdict === 'object'
      ? paperLiveParityAudit.verdict
      : {}
  const blockers = Array.isArray(paperLiveParityAudit.blockers)
    ? paperLiveParityAudit.blockers.filter((blocker) => {
        return !(
          blocker &&
          typeof blocker === 'object' &&
          'key' in blocker &&
          blocker.key === 'scanner_no_missed_positive_raw_edge'
        )
      })
    : []
  const currentBlockers = [...blockers]
  if (!rawEdgeHistory.complete) {
    currentBlockers.push({
      key: 'scanner_scan_history_complete',
      detail: rawEdgeHistory.error ?? 'current scan history is incomplete',
    })
  } else if (!rawEdgeHistory.no_missed_positive_raw_edge) {
    currentBlockers.push({
      key: 'scanner_no_missed_positive_raw_edge',
      detail: 'current scan history contains positive best raw edge with no candidate',
    })
  }

  return {
    ...paperLiveParityAudit,
    ok: paperLiveParityAudit.ok === true && rawEdgeHistory.no_missed_positive_raw_edge,
    verdict: {
      ...verdict,
      scanner_scan_history_complete: rawEdgeHistory.complete,
      scanner_no_missed_positive_raw_edge: rawEdgeHistory.no_missed_positive_raw_edge,
    },
    scanner: {
      ...scanner,
      raw_edge_history: rawEdgeHistory,
      no_missed_positive_raw_edge: rawEdgeHistory.no_missed_positive_raw_edge,
    },
    blockers: currentBlockers,
    current_scan_summary: {
      path: path.join(diagnosticsDir, 'scan_summary.csv'),
      rows: rawEdgeHistory.scan_rows,
      refreshed_at: new Date().toISOString(),
    },
  }
}

function scannerNoTradeSummary(
  latestScan: Record<string, string> | undefined,
  paperExecutionCount: number,
) {
  if (paperExecutionCount > 0) return `accepted_fills=${paperExecutionCount}`
  if (!latestScan) return 'no scan_summary.csv row yet'

  const rawYes = numberField(latestScan, 'raw_yes_candidates')
  const rawNo = numberField(latestScan, 'raw_no_candidates')
  const rawBundle = numberField(latestScan, 'raw_bundle_candidates')
  const rawRanked = numberField(latestScan, 'raw_ranked_candidates')
  const rawTotal = rawYes + rawNo + rawBundle + rawRanked
  const theoryYes = numberField(latestScan, 'theory_hint_yes')
  const theoryNo = numberField(latestScan, 'theory_hint_no')
  const theoryBundle = numberField(latestScan, 'theory_hint_bundle')
  const hardUnresolved = numberField(latestScan, 'quote_hard_unresolved_tokens')
  const noAsk = numberField(latestScan, 'quote_no_ask_tokens')
  const missingBook = numberField(latestScan, 'quote_missing_book_tokens')
  const targetProjection = numberField(latestScan, 'target_projection_rejections')
  const targetSize = numberField(latestScan, 'target_size_rejections')
  const opportunities = numberField(latestScan, 'opportunities_found')
  const bestEdgeType = latestScan.best_raw_edge_type || ''
  const bestEdgeNet = latestScan.best_raw_edge_net_profit || ''
  const bestEdgeRoi = latestScan.best_raw_edge_roi_pct || ''
  const bestEdgeCost = latestScan.best_raw_edge_cost || ''
  const bestEdgeRevenue = latestScan.best_raw_edge_revenue || ''
  const bestEdge =
    bestEdgeType && bestEdgeNet
      ? ` best_edge=${bestEdgeType} net=${bestEdgeNet} roi=${bestEdgeRoi || '-'} cost=${bestEdgeCost || '-'} revenue=${bestEdgeRevenue || '-'}`
      : ''

  const reason =
    opportunities > 0
      ? 'opportunities_detected_not_filled'
      : rawTotal === 0
        ? 'no_raw_executable_edge'
        : targetProjection + targetSize > 0
          ? 'target_size_or_depth_removed_edge'
          : hardUnresolved > 0
            ? 'quotes_unresolved'
            : 'filters_removed_edge'

  return [
    `reason=${reason}`,
    `raw=${rawYes}/${rawNo}/${rawBundle}/${rawRanked}`,
    `theory=${theoryYes}/${theoryNo}/${theoryBundle}`,
    `hard_unresolved=${hardUnresolved}`,
    `no_ask=${noAsk}`,
    `missing_book=${missingBook}`,
    `target_rejections=${targetProjection + targetSize}`,
    bestEdge.trim(),
  ]
    .filter(Boolean)
    .join(' ')
}

async function readinessReport() {
  const [
    paper,
    live,
    combo,
    codeCeiling,
    unblockPlan,
    paperLiveParityAudit,
    bundleManifest,
    tradeResult,
    operatorPreflightManifest,
    activationPacket,
    latencyCsv,
    scanCsv,
    scanHistory,
    tradeEvidence,
    liveExecutionJournal,
    comboRfqExecutionJournal,
  ] = await Promise.all([
    paperStats(),
    readJsonFile(readinessJsonFiles.live),
    readJsonFile(readinessJsonFiles.combo),
    readJsonFile(readinessJsonFiles.codeCeiling),
    readJsonNearby(readinessJsonFiles.unblockPlan),
    readJsonNearby(readinessJsonFiles.paperLiveParityAudit),
    readJsonNearby(readinessJsonFiles.bundleManifest),
    readJsonNearby(readinessJsonFiles.tradeResult),
    readOperatorPreflightManifest(),
    readActivationPacket(),
    readCsv('latency_budget.csv'),
    readCsv('scan_summary.csv'),
    readScanHistory(),
    readTradeEvidence(),
    countJsonlRows('live_execution_journal.jsonl', isLiveJournalEvidence),
    countJsonlRows('combo_rfq_execution_journal.jsonl', isLiveJournalEvidence),
  ])
  const latencyRows = parseCsv(latencyCsv)
  const latestLatency = latencyRows.at(-1)
  const scanRows = parseCsv(scanCsv)
  const latestScan = scanRows.at(-1)
  const currentPaperLiveParityAudit = withCurrentScanHistory(
    paperLiveParityAudit,
    scanHistory,
  )
  const runtimeMonitorMode = path.basename(diagnosticsDir) === 'runtime_diagnostics'
  const scanner = scannerStatus()
  const autoStartScanner = false
  const scanFreshMaxMs = Math.max(
    30_000,
    Number(process.env.MONITOR_SCAN_FRESH_MAX_MS ?? 0) ||
      (Number(scannerIntervalSeconds) || 1) * 4 * 1000,
  )
  const latestScanAgeMs = timestampAgeMs(latestScan)
  const latestLatencyAgeMs = timestampAgeMs(latestLatency)
  const monitorDataFresh =
    !runtimeMonitorMode ||
    (scanner.running &&
      latestScanAgeMs !== null &&
      latestScanAgeMs <= scanFreshMaxMs &&
      latestLatencyAgeMs !== null &&
      latestLatencyAgeMs <= scanFreshMaxMs)
  const monitorFreshText = runtimeMonitorMode
    ? `scanner_running=${scanner.running} scanner_interval_s=${scannerIntervalSeconds} scan_age_ms=${latestScanAgeMs ?? 'unknown'} latency_age_ms=${latestLatencyAgeMs ?? 'unknown'} max_age_ms=${scanFreshMaxMs}`
    : 'freshness=readiness_artifact'
  const paperExecutionRows = tradeEvidence.paperExecutionRows
  const liveSubmissionEvidenceCount =
    tradeEvidence.count + liveExecutionJournal.count + comboRfqExecutionJournal.count
  const liveSubmissionEvidenceComplete =
    tradeEvidence.complete && liveExecutionJournal.complete && comboRfqExecutionJournal.complete
  const liveSubmissionEvidenceErrors = [
    tradeEvidence.error,
    liveExecutionJournal.error,
    comboRfqExecutionJournal.error,
  ].filter((error): error is string => Boolean(error))
  const liveChecks = Array.isArray(live.checks) ? live.checks : []
  const liveState = live.live_submissions_supported ? readinessStateFromChecks(liveChecks) : 'blocked'
  const liveBlockers = readinessBlockers(liveChecks)
  const nextLiveActions = liveNextActions(liveChecks)
  const comboBlockers = Array.isArray(combo.blockers) ? combo.blockers : []
  const codeBlockers = Array.isArray(codeCeiling.code_blockers)
    ? codeCeiling.code_blockers.slice(0, 4)
    : []
  const requiredLiveEnvs = Array.from(
    new Set([
      ...nextLiveActions.flatMap((item) => item.mentionedEnvs),
      ...comboBlockers.flatMap((blocker: unknown) => mentionedEnvs(String(blocker))),
    ]),
  ).sort()
  const latencyStatus = latestLatency?.status || 'unknown'
  const quoteCacheHits = numberField(latestLatency, 'quote_cache_hits')
  const quoteRestRequested = numberField(latestLatency, 'quote_rest_requested')
  const quoteRestResolved = numberField(latestLatency, 'quote_rest_resolved')
  const wsRecentWindowRows = Math.max(
    1,
    Number(process.env.MONITOR_WS_RECENT_WINDOW_ROWS ?? 50) || 50,
  )
  const recentWsSnapshotSatisfied = latencyRows.slice(-wsRecentWindowRows).some((row) => {
    return (
      row.ws_snapshot_satisfied === 'true' &&
      numberField(row, 'ws_snapshot_total_tokens') > 0 &&
      numberField(row, 'ws_snapshot_ready_tokens') >= numberField(row, 'ws_snapshot_min_ready_tokens')
    )
  })
  const hftFastPathEvidence = quoteCacheHits > 0 && recentWsSnapshotSatisfied
  const hftState =
    hftFastPathEvidence && monitorDataFresh
      ? 'ready'
      : latencyStatus === 'blocked' || latencyStatus === 'ok' || runtimeMonitorMode
        ? 'blocked'
        : 'unknown'
  const quoteHardUnresolved = latestScan?.quote_hard_unresolved_tokens ?? latestLatency?.quote_hard_unresolved_tokens ?? '-'
  const quoteNoAsk = latestScan?.quote_no_ask_tokens ?? '-'
  const quoteMissingBook = latestScan?.quote_missing_book_tokens ?? '-'
  const hftDetail =
    latestLatency
      ? `latency=${latestLatency.scan_duration_ms ?? '-'}ms ws_snapshot=${latestLatency.ws_snapshot_ready_tokens ?? '-'}/${latestLatency.ws_snapshot_total_tokens ?? '-'}>=${latestLatency.ws_snapshot_min_ready_tokens ?? '-'} latest_satisfied=${latestLatency.ws_snapshot_satisfied ?? '-'} recent_satisfied=${recentWsSnapshotSatisfied} recent_window_rows=${wsRecentWindowRows} wait=${latestLatency.ws_snapshot_wait_ms ?? '-'}ms cache_hits=${quoteCacheHits} rest=${quoteRestResolved}/${quoteRestRequested} resolved=${latestLatency.quote_rest_resolution_pct ?? '-'}% hard_unresolved=${quoteHardUnresolved} no_ask=${quoteNoAsk} missing_book=${quoteMissingBook} ${monitorFreshText} blockers=${hftFastPathEvidence && monitorDataFresh ? 'none' : latestLatency.blockers || 'missing_required_recent_ws_snapshot_cache_or_fresh_scanner'}`
      : 'latency_budget.csv unavailable; run scanner once'
  const parityVerdict =
    currentPaperLiveParityAudit?.verdict && typeof currentPaperLiveParityAudit.verdict === 'object'
      ? (currentPaperLiveParityAudit.verdict as Record<string, unknown>)
      : {}
  const parityBlockers = Array.isArray(currentPaperLiveParityAudit?.blockers)
    ? currentPaperLiveParityAudit.blockers.map((blocker: { key?: string; detail?: string }) =>
        `${blocker.key ?? 'blocker'}: ${blocker.detail ?? 'blocked'}`,
      )
    : []
  const parityScanHistory =
    currentPaperLiveParityAudit?.scanner &&
    typeof currentPaperLiveParityAudit.scanner === 'object' &&
    'raw_edge_history' in currentPaperLiveParityAudit.scanner
      ? (currentPaperLiveParityAudit.scanner.raw_edge_history as Record<string, unknown>)
      : null
  const parityScanEvidence =
    parityScanHistory && typeof parityScanHistory === 'object'
      ? [
          `scan_history=${Number(parityScanHistory.scan_rows ?? 0)} rows`,
          `positive_edges=${Number(parityScanHistory.positive_best_raw_edge_rows ?? 0)}`,
          `missed_positive_edges=${Number(parityScanHistory.missed_positive_raw_edge_rows ?? 0)}`,
        ].join(' ')
      : ''
  const parityEvidence =
    currentPaperLiveParityAudit.unavailable
      ? ''
      : [
          `scanner_path=${parityVerdict.scanner_paper_execution_path_proven === true ? 'proven' : 'missing'}`,
          `decision_parity=${parityVerdict.scanner_live_decision_path_parity_proven === true ? 'proven' : 'missing'}`,
          `missed_edge_guard=${parityVerdict.scanner_no_missed_positive_raw_edge === true ? 'ok' : 'blocked'}`,
          `live_no_submit=${parityVerdict.live_no_submit_guard_proven === true ? 'proven' : 'missing'}`,
          `final_rest_guard=${parityVerdict.final_rest_guard_seen === true ? 'seen' : 'missing'}`,
          parityScanEvidence,
        ].join(' ')
  const parityState =
    currentPaperLiveParityAudit.unavailable
      ? 'unknown'
      : currentPaperLiveParityAudit.ok === true
        ? 'ready'
        : 'blocked'
  const parityValue =
    currentPaperLiveParityAudit.unavailable
      ? 'not checked'
      : parityVerdict.paper_live_identical === true
        ? 'identical proven'
        : 'not identical'
  const parityDetail =
    currentPaperLiveParityAudit.unavailable
      ? `paper-live-parity-audit.json unavailable: ${currentPaperLiveParityAudit.error}`
      : parityBlockers.length
        ? `${parityBlockers.join(' | ')} | ${parityEvidence}`
        : `paper/live parity, paper profit, and speed proof passed | ${parityEvidence}`
  const brokerPaperTradeCount = numericValue(paper.stats?.total_trades)
  const scannerTradeSummary = scannerNoTradeSummary(latestScan, paperExecutionRows)
  const paperExecutionCanary = tradeResult?.checks?.paper?.execution_canary
  const paperExecutionCanaryKnown =
    !tradeResult.unavailable && paperExecutionCanary && typeof paperExecutionCanary === 'object'
  const paperExecutionCanaryOk = paperExecutionCanary?.ok === true
  const paperExecutionCanaryText = paperExecutionCanaryKnown
    ? `paper_canary=${paperExecutionCanaryOk ? 'ok' : 'blocked'} canary_trades=${paperExecutionCanary.trade_count ?? 0}`
    : 'paper_canary=not checked'
  const paperAdapterUnitProof = tradeResult?.checks?.paper?.adapter_unit_proof
  const paperAdapterUnitProofKnown =
    !tradeResult.unavailable && paperAdapterUnitProof && typeof paperAdapterUnitProof === 'object'
  const paperAdapterUnitProofOk = paperAdapterUnitProof?.ok === true
  const paperAdapterUnitProofText = paperAdapterUnitProofKnown
    ? `adapter_proof=${paperAdapterUnitProofOk ? 'ok' : 'blocked'}`
    : 'adapter_proof=not checked'
  const paperScannerTradeProof = tradeResult?.checks?.paper?.scanner_trade_proof
  const paperScannerTradeProofKnown =
    !tradeResult.unavailable && paperScannerTradeProof && typeof paperScannerTradeProof === 'object'
  const paperScannerTradeProofOk = paperScannerTradeProof?.ok === true
  const paperScannerTradeProofHash =
    typeof paperScannerTradeProof?.synthetic_plan_hash === 'string'
      ? paperScannerTradeProof.synthetic_plan_hash
      : ''
  const paperScannerDecisionParity =
    paperScannerTradeProof?.decision_path_parity &&
    typeof paperScannerTradeProof.decision_path_parity === 'object'
      ? (paperScannerTradeProof.decision_path_parity as Record<string, unknown>)
      : null
  const paperScannerDecisionParityOk = paperScannerDecisionParity?.ok === true
  const paperScannerTradeProofText = paperScannerTradeProofKnown
    ? `scanner_proof=${paperScannerTradeProofOk ? 'ok' : 'blocked'} decision_parity=${paperScannerDecisionParityOk ? 'ok' : 'blocked'} synthetic_rows=${paperScannerTradeProof.paper_ok_rows ?? 0} plan_hash=${paperScannerTradeProofHash || 'missing'}`
    : 'scanner_proof=not checked'
  const parityPaper =
    currentPaperLiveParityAudit.paper && typeof currentPaperLiveParityAudit.paper === 'object'
      ? (currentPaperLiveParityAudit.paper as Record<string, unknown>)
      : {}
  const profitabilityEvidence =
    parityPaper.profitability_evidence && typeof parityPaper.profitability_evidence === 'object'
      ? (parityPaper.profitability_evidence as Record<string, unknown>)
      : {}
  const profitabilitySample =
    profitabilityEvidence.sample && typeof profitabilityEvidence.sample === 'object'
      ? (profitabilityEvidence.sample as Record<string, unknown>)
      : {}
  const profitabilityMetrics =
    profitabilityEvidence.metrics && typeof profitabilityEvidence.metrics === 'object'
      ? (profitabilityEvidence.metrics as Record<string, unknown>)
      : {}
  const profitabilityBlockers = Array.isArray(profitabilityEvidence.blockers)
    ? profitabilityEvidence.blockers.map(String)
    : []
  const paperProfitProven = parityVerdict.paper_profitable_proven === true
  const paperProfitState = currentPaperLiveParityAudit.unavailable
    ? 'unknown'
    : paperProfitProven
      ? 'ready'
      : 'blocked'
  const paperProfitValue = paperProfitProven
    ? `conservative_pnl=${profitabilityMetrics.total_conservative_pnl_usd ?? '-'} samples=${profitabilitySample.accepted_trades ?? 0}`
    : 'no real profit'
  const paperProfitDetail = currentPaperLiveParityAudit.unavailable
    ? `paper-live-parity-audit.json unavailable: ${currentPaperLiveParityAudit.error}`
    : paperProfitProven
      ? `campaign gate passed; trades=${profitabilitySample.accepted_trades ?? 0} events=${profitabilitySample.unique_events ?? 0} hours=${profitabilitySample.observation_hours ?? 0} weighted_roi_pct=${profitabilityMetrics.weighted_conservative_roi_pct ?? 0} lower_mean_pnl=${profitabilityMetrics.one_sided_95_normal_lower_mean_pnl_usd ?? 0}`
      : `campaign gate blocked; ${profitabilityBlockers.slice(0, 4).join(' | ') || 'no qualifying real scanner fills'}; synthetic/canary proofs excluded`
  const paperState =
    paper.ok &&
    monitorDataFresh &&
    (!paperAdapterUnitProofKnown || paperAdapterUnitProofOk) &&
    (!paperExecutionCanaryKnown || paperExecutionCanaryOk) &&
    (!paperScannerTradeProofKnown || paperScannerTradeProofOk)
      ? 'ready'
      : 'blocked'

  return {
    generatedAt: new Date().toISOString(),
    diagnosticsDir,
    liveUnblockPlan: unblockPlan.unavailable ? null : unblockPlan,
    paperLiveParityAudit: currentPaperLiveParityAudit.unavailable
      ? null
      : currentPaperLiveParityAudit,
    readinessBundleManifest: bundleManifest.unavailable ? null : bundleManifest,
    tradeReadinessResult: tradeResult.unavailable ? null : tradeResult,
    operatorPreflightManifest: operatorPreflightManifest.unavailable
      ? null
      : operatorPreflightManifest,
    liveActivationPacket: activationPacket.unavailable ? null : activationPacket,
    monitorFreshness: {
      runtimeMonitorMode,
      autoStartScanner,
      scannerRunning: scanner.running,
      scannerIntervalSeconds,
      latestScanAgeMs,
      latestLatencyAgeMs,
      maxAgeMs: scanFreshMaxMs,
      wsRecentWindowRows,
      fresh: monitorDataFresh,
    },
    nextLiveActions,
    requiredLiveEnvs,
    items: [
      {
        key: 'paper',
        label: 'Paper ops',
        state: paperState,
        value: paper.ok ? 'operational' : 'blocked',
        detail: paper.ok
          ? `account=${paper.account} value=${paper.balance?.total_value ?? '-'} pnl=${paper.stats?.pnl ?? paper.balance?.pnl ?? '-'} broker_history=${brokerPaperTradeCount ?? '-'} scanner_fills=${paperExecutionRows} ${scannerTradeSummary} ${paperAdapterUnitProofText} ${paperExecutionCanaryText} ${paperScannerTradeProofText} ${monitorFreshText}`
          : `pm-trader unavailable: ${paper.error ?? 'unknown error'}`,
      },
      {
        key: 'paper_profit',
        label: 'Paper profit',
        state: paperProfitState,
        value: paperProfitValue,
        detail: paperProfitDetail,
      },
      {
        key: 'paper_live_parity',
        label: 'Parity',
        state: parityState,
        value: parityValue,
        detail: parityDetail,
      },
      {
        key: 'live',
        label: 'Live',
        state: liveState,
        value: live.live_submissions_supported ? 'supported' : 'blocked',
        detail: live.unavailable
          ? `live_readiness_report.json unavailable: ${live.error}`
          : liveBlockers.length
            ? liveBlockers.join(' | ')
            : 'all live readiness checks passed',
      },
      {
        key: 'live_no_submit',
        label: 'Live submit',
        state:
          liveSubmissionEvidenceComplete && liveSubmissionEvidenceCount === 0
            ? 'ready'
            : 'blocked',
        value:
          liveSubmissionEvidenceComplete && liveSubmissionEvidenceCount === 0
            ? 'no live submit evidence'
            : liveSubmissionEvidenceCount > 0
              ? `${liveSubmissionEvidenceCount} live evidence rows`
              : 'evidence incomplete',
        detail:
          liveSubmissionEvidenceComplete && liveSubmissionEvidenceCount === 0
            ? 'trades.csv and live journals have no submit evidence'
            : [
                `trades=${tradeEvidence.count}`,
                `live_journal=${liveExecutionJournal.count}`,
                `combo_journal=${comboRfqExecutionJournal.count}`,
                ...liveSubmissionEvidenceErrors,
              ].join(' | '),
      },
      {
        key: 'live_code_gates',
        label: 'Live code gates',
        state: codeCeiling.unavailable ? 'unknown' : codeBlockers.length === 0 ? 'ready' : 'blocked',
        value: codeCeiling.unavailable
          ? 'not checked'
          : codeBlockers.length === 0
            ? 'clear'
            : `${codeBlockers.length} blockers`,
        detail: codeCeiling.unavailable
          ? `live_code_ceiling_report.json unavailable: ${codeCeiling.error}`
          : codeBlockers.length
            ? codeBlockers.map(codeBlockerSummary).join(' | ')
            : 'no hard code blockers in code-ceiling diagnostic',
      },
      {
        key: 'hft',
        label: 'HFT',
        state: hftState,
        value: latencyStatus,
        detail: hftDetail,
      },
      {
        key: 'ui',
        label: 'UI',
        state: monitorDataFresh ? 'ready' : 'blocked',
        value: scanner.running ? 'scanner running' : 'dashboard online',
        detail: `local API online; latest_scan=${latestScan?.scan_id ?? '-'}; combo_promoted=${combo.promoted ?? false}; ${monitorFreshText}`,
      },
    ],
  }
}

type JsonResponse = {
  setHeader(name: string, value: string): void
  statusCode: number
  end(body: string): void
}

function sendJson(res: JsonResponse, body: unknown, statusCode = 200) {
  res.statusCode = statusCode
  res.setHeader('content-type', 'application/json')
  res.end(JSON.stringify(body))
}

function isSameOriginMutation(req: IncomingMessage) {
  const origin = req.headers.origin
  const host = req.headers.host
  if (typeof origin !== 'string' || typeof host !== 'string') return false

  try {
    const protocol = 'encrypted' in req.socket && req.socket.encrypted ? 'https' : 'http'
    return new URL(origin).origin === new URL(`${protocol}://${host}`).origin
  } catch {
    return false
  }
}

function requireSameOriginMutation(req: IncomingMessage, res: JsonResponse) {
  if (isSameOriginMutation(req)) return true
  sendJson(res, { ok: false, error: 'same-origin Origin header required' }, 403)
  return false
}

function pushScannerLog(source: string, chunk: Buffer) {
  const lines = chunk
    .toString('utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
  scannerLog = [...scannerLog, ...lines.map((line) => `${source}: ${line}`)].slice(-80)
}

function pidIsRunning(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

class DashboardConflict extends Error {}

function asObject(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new DashboardConflict(`${label} must be a JSON object`)
  }
  return value as Record<string, unknown>
}

function readScannerPidRecord(): ScannerPidRecord | null {
  try {
    const record = JSON.parse(fsSync.readFileSync(scannerPidFile, 'utf8')) as ScannerPidRecord
    return Number.isInteger(record.pid) && record.pid > 0 ? record : null
  } catch {
    return null
  }
}

function writeScannerPidRecord(record: ScannerPidRecord, ownerToken: string) {
  fsSync.mkdirSync(diagnosticsDir, { recursive: true })
  const temporaryPath = `${scannerPidFile}.${ownerToken}.tmp`
  fsSync.writeFileSync(temporaryPath, JSON.stringify(record), { encoding: 'utf8', mode: 0o600 })
  fsSync.renameSync(temporaryPath, scannerPidFile)
}

function clearScannerPidRecord(ownerToken: string) {
  const record = readScannerPidRecord()
  if (record?.ownerToken !== ownerToken) return
  try {
    fsSync.unlinkSync(scannerPidFile)
  } catch {
    // Already gone.
  }
}

function reserveScannerOwnership(ownerToken: string) {
  fsSync.mkdirSync(diagnosticsDir, { recursive: true })
  let descriptor: number
  try {
    descriptor = fsSync.openSync(scannerLockFile, 'wx', 0o600)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'EEXIST') {
      throw new DashboardConflict(
        `scanner ownership lock already exists at ${scannerLockFile}; refusing a second scanner`,
      )
    }
    throw error
  }
  try {
    fsSync.writeFileSync(
      descriptor,
      JSON.stringify({ ownerPid: process.pid, ownerToken, claimedAt: new Date().toISOString() }),
      'utf8',
    )
    fsSync.fsyncSync(descriptor)
  } catch (error) {
    try {
      fsSync.unlinkSync(scannerLockFile)
    } catch {
      // The exclusively-created claim is already gone.
    }
    throw error
  } finally {
    fsSync.closeSync(descriptor)
  }
}

function releaseScannerOwnership(ownerToken: string) {
  try {
    const claim = JSON.parse(fsSync.readFileSync(scannerLockFile, 'utf8')) as {
      ownerToken?: string
    }
    if (claim.ownerToken !== ownerToken) return
    fsSync.unlinkSync(scannerLockFile)
  } catch {
    // A missing or foreign ownership lock is never removed.
  }
}

function persistedScannerState() {
  const pidFilePresent = fsSync.existsSync(scannerPidFile)
  const lockFilePresent = fsSync.existsSync(scannerLockFile)
  const record = readScannerPidRecord()
  return {
    record,
    pidFilePresent,
    lockFilePresent,
    running: record ? pidIsRunning(record.pid) : false,
  }
}

function scannerStatus() {
  const ownedPid = scannerProcess?.pid
  const ownedRunning = Boolean(
    ownedPid && scannerProcess?.exitCode === null && scannerProcess?.signalCode === null,
  )
  const persisted = ownedRunning
    ? { record: null, pidFilePresent: false, lockFilePresent: false, running: false }
    : persistedScannerState()
  const unmanagedRunning = !ownedRunning && persisted.running
  const staleOwnership =
    !ownedRunning && !unmanagedRunning && (persisted.pidFilePresent || persisted.lockFilePresent)
  const launchConfigPresent = Boolean(
    process.env.SCANNER_RELEASE_BINARY &&
      process.env.SCANNER_READINESS_MANIFEST &&
      process.env.SCANNER_BUILD_PROVENANCE,
  )
  return {
    running: ownedRunning || unmanagedRunning,
    controllable: ownedRunning,
    ownership: ownedRunning
      ? 'owned'
      : unmanagedRunning
        ? 'unmanaged'
        : staleOwnership
          ? 'stale'
          : 'idle',
    stopping: ownedRunning && scannerStopping,
    pid: ownedPid ?? persisted.record?.pid,
    startedAt: ownedRunning ? scannerStartedAt : (persisted.record?.startedAt ?? null),
    lastExit: scannerLastExit,
    account: paperAccount,
    dataDir: paperDataDir,
    diagnosticsDir,
    launchEligibility: scannerLaunchContract
      ? 'evidence_eligible'
      : launchConfigPresent
        ? 'requires_validation'
        : 'blocked',
    launchError:
      scannerLaunchError ??
      (launchConfigPresent
        ? null
        : 'SCANNER_RELEASE_BINARY, SCANNER_READINESS_MANIFEST, and SCANNER_BUILD_PROVENANCE are required'),
    binaryPath: scannerLaunchContract?.binaryPath ?? null,
    binarySha256: scannerLaunchContract?.binarySha256 ?? null,
    readinessManifest: scannerLaunchContract?.readinessManifestPath ?? null,
    buildProvenance: scannerLaunchContract?.buildProvenancePath ?? null,
    unmanagedReason: unmanagedRunning
      ? 'persisted PID is live but is not owned by this dashboard; it will not be signalled'
      : staleOwnership
        ? 'stale or malformed ownership files require manual inspection'
        : null,
    log: scannerLog.slice(-30),
  }
}

function withScannerLifecycle<T>(operation: () => Promise<T>) {
  const result = scannerLifecycleQueue.then(operation, operation)
  scannerLifecycleQueue = result.then(
    () => undefined,
    () => undefined,
  )
  return result
}

async function canonicalRequiredPath(name: string) {
  const configured = process.env[name]
  if (!configured || !path.isAbsolute(configured)) {
    throw new DashboardConflict(`${name} must be an absolute canonical path`)
  }
  const canonical = await fs.realpath(configured)
  if (canonical !== configured) {
    throw new DashboardConflict(`${name} must be canonical and may not be a symlink`)
  }
  return canonical
}

async function canonicalEmbeddedPath(value: unknown, label: string) {
  if (typeof value !== 'string' || !path.isAbsolute(value)) {
    throw new DashboardConflict(`${label} must be an absolute path`)
  }
  return fs.realpath(value)
}

async function sha256File(filePath: string) {
  const digest = createHash('sha256')
  const stream = fsSync.createReadStream(filePath)
  for await (const chunk of stream) digest.update(chunk)
  return digest.digest('hex')
}

async function loadScannerLaunchContract(): Promise<ScannerLaunchContract> {
  const binaryPath = await canonicalRequiredPath('SCANNER_RELEASE_BINARY')
  const readinessManifestPath = await canonicalRequiredPath('SCANNER_READINESS_MANIFEST')
  const buildProvenancePath = await canonicalRequiredPath('SCANNER_BUILD_PROVENANCE')
  const binaryStats = await fs.stat(binaryPath)
  if (!binaryStats.isFile() || (binaryStats.mode & 0o111) === 0) {
    throw new DashboardConflict('SCANNER_RELEASE_BINARY must be an executable regular file')
  }

  const verifier = path.join(scannerCwd, 'scripts', 'verify-readiness-bundle.sh')
  try {
    await run(verifier, [readinessManifestPath], {
      cwd: scannerCwd,
      timeout: 120_000,
      maxBuffer: 4 * 1024 * 1024,
    })
  } catch (error) {
    throw new DashboardConflict(`readiness bundle verification failed: ${evidenceError(error)}`)
  }

  const readiness = asObject(
    JSON.parse(await fs.readFile(readinessManifestPath, 'utf8')),
    'readiness manifest',
  )
  const provenance = asObject(
    JSON.parse(await fs.readFile(buildProvenancePath, 'utf8')),
    'build provenance',
  )
  const build = asObject(readiness.build, 'readiness manifest build')
  const provenanceBinary = asObject(provenance.binary, 'build provenance binary')
  const files = Array.isArray(readiness.files) ? readiness.files : []
  const releaseEntry = files.find(
    (entry) => entry && typeof entry === 'object' && (entry as Record<string, unknown>).label === 'release_binary',
  )
  const provenanceEntry = files.find(
    (entry) => entry && typeof entry === 'object' && (entry as Record<string, unknown>).label === 'build_provenance',
  )
  const releaseFile = asObject(releaseEntry, 'readiness release_binary file entry')
  const provenanceFile = asObject(provenanceEntry, 'readiness build_provenance file entry')
  const manifestBinaryPath = await canonicalEmbeddedPath(build.binary_path, 'build.binary_path')
  const manifestProvenancePath = await canonicalEmbeddedPath(
    build.provenance_path,
    'build.provenance_path',
  )
  const fileBinaryPath = await canonicalEmbeddedPath(releaseFile.path, 'release_binary.path')
  const fileProvenancePath = await canonicalEmbeddedPath(
    provenanceFile.path,
    'build_provenance.path',
  )
  const provenanceBinaryPath = await canonicalEmbeddedPath(
    provenanceBinary.path,
    'provenance binary.path',
  )
  if (
    manifestBinaryPath !== binaryPath ||
    fileBinaryPath !== binaryPath ||
    provenanceBinaryPath !== binaryPath ||
    manifestProvenancePath !== buildProvenancePath ||
    fileProvenancePath !== buildProvenancePath
  ) {
    throw new DashboardConflict('release binary/readiness/provenance paths do not bind to one artifact')
  }

  const runRoot = await canonicalEmbeddedPath(readiness.run_root, 'readiness run_root')
  if (path.relative(runRoot, binaryPath) !== path.join('release', 'polymarket-arb-scanner')) {
    throw new DashboardConflict('SCANNER_RELEASE_BINARY is not the copied release inside readiness run_root')
  }
  const sourceRoot = await canonicalEmbeddedPath(provenance.source_root, 'provenance source_root')
  if (sourceRoot !== (await fs.realpath(scannerCwd))) {
    throw new DashboardConflict('build provenance source_root does not match this repository')
  }
  if (provenance.inputs_unchanged_during_build !== true || build.inputs_unchanged_during_build !== true) {
    throw new DashboardConflict('build inputs were not stable during the readiness release build')
  }

  const hashes = [
    build.binary_sha256,
    releaseFile.sha256,
    provenanceBinary.sha256,
  ].map((value) => (typeof value === 'string' ? value.toLowerCase() : ''))
  if (!hashes[0] || hashes.some((hash) => !/^[0-9a-f]{64}$/.test(hash) || hash !== hashes[0])) {
    throw new DashboardConflict('release binary SHA-256 bindings are missing or inconsistent')
  }
  const expectedProvenanceSha =
    typeof provenanceFile.sha256 === 'string' ? provenanceFile.sha256.toLowerCase() : ''
  if (
    !/^[0-9a-f]{64}$/.test(expectedProvenanceSha) ||
    (await sha256File(buildProvenancePath)) !== expectedProvenanceSha
  ) {
    throw new DashboardConflict('build provenance SHA-256 does not match readiness manifest')
  }
  const paperBinding =
    readiness.paper_execution_binding && typeof readiness.paper_execution_binding === 'object'
      ? (readiness.paper_execution_binding as Record<string, unknown>)
      : null
  const expectedProducerSha = paperBinding?.expected_producer_binary_sha256
  if (
    typeof expectedProducerSha === 'string' &&
    expectedProducerSha.length > 0 &&
    expectedProducerSha.toLowerCase() !== hashes[0]
  ) {
    throw new DashboardConflict('paper execution producer SHA-256 differs from release binary')
  }
  const campaignProfitFingerprint =
    typeof paperBinding?.campaign_profit_compatibility_fingerprint === 'string'
      ? paperBinding.campaign_profit_compatibility_fingerprint
      : ''
  if (
    !/^0x[0-9a-f]{64}$/.test(campaignProfitFingerprint)
  ) {
    throw new DashboardConflict(
      'readiness manifest does not bind a campaign profit-compatibility fingerprint',
    )
  }
  const rawProfitFingerprints = paperBinding?.profit_compatibility_fingerprint_values
  if (
    !Array.isArray(rawProfitFingerprints) ||
    rawProfitFingerprints.length > 1 ||
    rawProfitFingerprints.some(
      (value) =>
        typeof value !== 'string' ||
        !/^0x[0-9a-f]{64}$/.test(value) ||
        value !== campaignProfitFingerprint,
    )
  ) {
    throw new DashboardConflict(
      'paper evidence profit-compatibility fingerprints disagree with the campaign fingerprint',
    )
  }

  return {
    binaryPath,
    binarySha256: hashes[0],
    readinessManifestPath,
    buildProvenancePath,
    profitCompatibilityFingerprint: campaignProfitFingerprint,
  }
}

async function verifyScannerBinaryImmediately(contract: ScannerLaunchContract) {
  const before = await fs.stat(contract.binaryPath)
  const actualSha = await sha256File(contract.binaryPath)
  const after = await fs.stat(contract.binaryPath)
  if (
    before.dev !== after.dev ||
    before.ino !== after.ino ||
    before.size !== after.size ||
    before.mtimeMs !== after.mtimeMs ||
    actualSha !== contract.binarySha256
  ) {
    throw new DashboardConflict('release binary changed or failed SHA-256 verification before spawn')
  }
}

function prepareScannerOwnership(ownerToken: string) {
  if (scannerProcess) throw new DashboardConflict('scanner is already owned by this dashboard')
  const existingRecord = readScannerPidRecord()
  if (fsSync.existsSync(scannerPidFile)) {
    if (!existingRecord) {
      throw new DashboardConflict('scanner.pid is malformed; refusing to overwrite unknown ownership')
    }
    if (pidIsRunning(existingRecord.pid)) {
      throw new DashboardConflict(
        `scanner PID ${existingRecord.pid} is live but unowned; refusing to signal or replace it`,
      )
    }
    fsSync.unlinkSync(scannerPidFile)
  }
  reserveScannerOwnership(ownerToken)
}

function scannerEnvironment() {
  return {
    ...process.env,
    POLYMARKET_PRIVATE_KEY: '',
    POLYMARKET_API_KEY: '',
    POLYMARKET_API_SECRET: '',
    POLYMARKET_API_PASSPHRASE: '',
    CLOB_API_KEY: '',
    CLOB_SECRET: '',
    CLOB_PASS_PHRASE: '',
    CLOB_PASSPHRASE: '',
    LIVE_SIGNER_ADDRESS: '',
    LIVE_FUNDER_ADDRESS: '',
    COMBO_RFQ_BEARER_TOKEN: '',
    COMBO_RFQ_PARTICIPANT_ID: '',
    COMBO_RFQ_STREAM_BEARER_TOKEN: '',
    RELAYER_API_KEY: '',
    RELAYER_API_KEY_ADDRESS: '',
    POLYGON_RPC_URL: '',
    WEBHOOK_URL: '',
    BETDEX_AUTH_TOKEN: '',
    LIVE_TRADING_ENABLED: 'false',
    PAPER_TRADING_ENABLED: 'true',
    PAPER_REQUIRE_FULL_CLOB_QUOTES: 'true',
    PAPER_MATCH_LIVE_POSITION_SIZE: 'true',
    EXTERNAL_PAPER_DATA_DIR: paperDataDir,
    EXTERNAL_PAPER_ACCOUNT: paperAccount,
    STRATEGY_LAB_ENABLED: 'false',
    DIAGNOSTICS_CSV_ENABLED: 'true',
    DIAGNOSTICS_DIR: diagnosticsDir,
    USE_CLOB_PRICES: 'true',
    SCAN_INTERVAL_SECONDS: scannerIntervalSeconds,
  }
}

async function verifyScannerProfitCompatibility(
  contract: ScannerLaunchContract,
  env: NodeJS.ProcessEnv,
  ownerToken: string,
) {
  const outputPath = path.join(diagnosticsDir, `.scanner-launch-fingerprint-${ownerToken}.json`)
  try {
    await run(
      contract.binaryPath,
      ['--paper', '--launch-config-fingerprint-output', outputPath],
      {
        cwd: scannerCwd,
        env,
        timeout: 30_000,
        maxBuffer: 1024 * 1024,
      },
    )
    const fingerprint = asObject(
      JSON.parse(await fs.readFile(outputPath, 'utf8')),
      'scanner launch fingerprint',
    )
    if (fingerprint.profit_compatibility_fingerprint !== contract.profitCompatibilityFingerprint) {
      throw new DashboardConflict(
        'effective paper configuration does not match the readiness campaign profit fingerprint',
      )
    }
  } finally {
    try {
      await fs.unlink(outputPath)
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        scannerLog = [
          ...scannerLog,
          `err: could not remove temporary launch fingerprint: ${evidenceError(error)}`,
        ].slice(-80)
      }
    }
  }
}

async function startScannerOwned() {
  if (scannerProcess?.pid) return scannerStatus()

  scannerLaunchError = null
  let contract: ScannerLaunchContract
  try {
    contract = await loadScannerLaunchContract()
  } catch (error) {
    scannerLaunchError = evidenceError(error)
    throw error
  }
  const ownerToken = randomUUID()
  prepareScannerOwnership(ownerToken)
  const env = scannerEnvironment()
  try {
    await verifyScannerProfitCompatibility(contract, env, ownerToken)
    await verifyScannerBinaryImmediately(contract)
  } catch (error) {
    releaseScannerOwnership(ownerToken)
    scannerLaunchError = evidenceError(error)
    throw error
  }

  scannerStartedAt = new Date().toISOString()
  scannerStopping = false
  scannerLastExit = null
  scannerLog = []

  let child: ChildProcessWithoutNullStreams
  try {
    child = spawn(contract.binaryPath, ['--paper'], {
      cwd: scannerCwd,
      env,
      stdio: 'pipe',
    })
  } catch (error) {
    releaseScannerOwnership(ownerToken)
    scannerLaunchError = evidenceError(error)
    throw error
  }
  scannerProcess = child
  scannerLaunchContract = contract
  if (!child.pid) {
    const spawnError = await new Promise<Error>((resolve) => {
      const timer = setTimeout(() => resolve(new Error('scanner spawn did not return a PID')), 1_000)
      child.once('error', (error) => {
        clearTimeout(timer)
        resolve(error)
      })
    })
    scannerProcess = null
    scannerLaunchContract = null
    releaseScannerOwnership(ownerToken)
    scannerLaunchError = evidenceError(spawnError)
    throw spawnError
  }
  child.stdout.on('data', (chunk: Buffer) => pushScannerLog('out', chunk))
  child.stderr.on('data', (chunk: Buffer) => pushScannerLog('err', chunk))
  let finished = false
  const finish = (exit: ScannerExit) => {
    if (finished) return
    finished = true
    clearScannerPidRecord(ownerToken)
    releaseScannerOwnership(ownerToken)
    if (scannerProcess !== child) return
    scannerLastExit = exit
    scannerProcess = null
    scannerLaunchContract = null
    scannerStartedAt = null
    scannerStopping = false
  }
  child.on('error', (error) => {
    scannerLog = [...scannerLog, `err: ${error.message}`].slice(-80)
    finish({ code: null, signal: null, at: new Date().toISOString() })
  })
  child.on('exit', (code, signal) => {
    finish({ code, signal, at: new Date().toISOString() })
  })
  try {
    writeScannerPidRecord(
      {
        pid: child.pid,
        startedAt: scannerStartedAt,
        ownerPid: process.pid,
        ownerToken,
        binaryPath: contract.binaryPath,
        binarySha256: contract.binarySha256,
      },
      ownerToken,
    )
  } catch (error) {
    scannerLaunchError = `could not persist scanner ownership: ${evidenceError(error)}`
    child.kill('SIGTERM')
    if (!(await waitForChildExit(child, scannerDrainTimeoutMs))) {
      scannerLog = [
        ...scannerLog,
        `err: scanner ownership persistence failed; graceful drain remains in progress for pid ${child.pid ?? 'unknown'} (no force-kill sent)`,
      ].slice(-80)
    }
    throw error
  }
  return scannerStatus()
}

function waitForChildExit(child: ChildProcessWithoutNullStreams, timeoutMs: number) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve(true)
  return new Promise<boolean>((resolve) => {
    let settled = false
    const finish = (exited: boolean) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      child.off('exit', exitedListener)
      resolve(exited)
    }
    const timer = setTimeout(() => {
      finish(false)
    }, timeoutMs)
    const exitedListener = () => finish(true)
    child.once('exit', exitedListener)
    if (child.exitCode !== null || child.signalCode !== null) finish(true)
  })
}

async function stopScannerOwned() {
  const child = scannerProcess
  if (!child) {
    const persisted = persistedScannerState()
    if (persisted.running || persisted.lockFilePresent || persisted.pidFilePresent) {
      throw new DashboardConflict(
        'scanner ownership is persisted but unowned; refusing to signal an unknown process',
      )
    }
    return scannerStatus()
  }
  scannerStopping = true
  child.kill('SIGTERM')
  if (!(await waitForChildExit(child, scannerDrainTimeoutMs))) {
    throw new DashboardConflict(
      `owned scanner pid ${child.pid ?? 'unknown'} is still draining after ${Math.round(scannerDrainTimeoutMs / 1_000)}s; it remains owned and no force-kill was sent`,
    )
  }
  return scannerStatus()
}

async function fileContainsEvidence(filePath: string, skipHeader: boolean) {
  try {
    await fs.access(filePath)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false
    throw error
  }
  const decoder = new StringDecoder('utf8')
  let remainder = ''
  let headerSkipped = !skipHeader
  const stream = fsSync.createReadStream(filePath)
  for await (const chunk of stream) {
    const text = `${remainder}${decoder.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))}`
    const lines = text.split(/\r?\n/)
    remainder = lines.pop() ?? ''
    for (const line of lines) {
      if (!headerSkipped) {
        headerSkipped = true
      } else if (line.trim()) {
        return true
      }
    }
  }
  remainder += decoder.end()
  return headerSkipped && remainder.trim().length > 0
}

async function assertNoDiagnosticsEvidence() {
  const diagnosticFiles = csvFiles
    .filter((name) => name !== 'trades.csv')
    .flatMap((name) => [name, `${name}.1`])
  for (const name of diagnosticFiles) {
    if (await fileContainsEvidence(path.join(diagnosticsDir, name), true)) {
      throw new DashboardConflict(
        `diagnostics reset refused: ${name} contains campaign evidence; use a new DIAGNOSTICS_DIR`,
      )
    }
  }
}

async function assertNoPaperCampaignEvidence() {
  const sources = [
    { name: 'trades.csv', skipHeader: true },
    { name: 'paper_execution_attempts.jsonl', skipHeader: false },
  ]
  for (const source of sources) {
    if (await fileContainsEvidence(path.join(diagnosticsDir, source.name), source.skipHeader)) {
      throw new DashboardConflict(
        `paper reset refused: ${source.name} contains campaign evidence; use a new account and data directory`,
      )
    }
  }
  const paper = await loadPaperStats()
  if (!paper.ok) {
    throw new DashboardConflict(`paper reset refused: existing broker history could not be verified`)
  }
  const brokerTrades = numericValue(paper.stats?.total_trades)
  if (brokerTrades === undefined || brokerTrades > 0) {
    throw new DashboardConflict(
      `paper reset refused: broker trade history is ${brokerTrades ?? 'unknown'}; use a new account and data directory`,
    )
  }
}

async function clearDiagnosticsFiles() {
  await fs.mkdir(diagnosticsDir, { recursive: true })
  const diagnosticFiles = csvFiles
    .filter((name) => name !== 'trades.csv')
    .flatMap((name) => [name, `${name}.1`])
  await Promise.all(
    diagnosticFiles.map(async (name) => {
      try {
        await fs.unlink(path.join(diagnosticsDir, name))
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
      }
    }),
  )
  scanHistoryCache = null
}

async function resetPaperAccount() {
  await run(paperCommand, [
    '--data-dir',
    paperDataDir,
    '--account',
    paperAccount,
    'reset',
    '--confirm',
  ])
}

type ResetKind = 'diagnostics' | 'paper' | 'all'

async function resetState(kind: ResetKind) {
  const persisted = persistedScannerState()
  if (!scannerProcess && (persisted.running || persisted.pidFilePresent || persisted.lockFilePresent)) {
    throw new DashboardConflict('reset refused while scanner ownership is unverified')
  }
  if (kind === 'diagnostics' || kind === 'all') await assertNoDiagnosticsEvidence()
  if (kind === 'paper' || kind === 'all') await assertNoPaperCampaignEvidence()

  const wasRunning = scannerProcess !== null
  if (wasRunning) await stopScannerOwned()
  try {
    if (kind === 'diagnostics' || kind === 'all') await clearDiagnosticsFiles()
    if (kind === 'paper' || kind === 'all') await resetPaperAccount()
  } catch (error) {
    if (wasRunning) await startScannerOwned()
    throw error
  }
  const scanner = wasRunning ? await startScannerOwned() : scannerStatus()
  return {
    ok: true,
    reset: kind,
    restarted: wasRunning,
    account: paperAccount,
    dataDir: paperDataDir,
    diagnosticsDir,
    scanner,
  }
}

function installLocalApi(server: ViteDevServer | PreviewServer) {
  server.middlewares.use('/api/diagnostics', async (_req, res) => {
    const entries = await Promise.all(
      csvFiles.map(async (name) => [name, await readCsv(name)] as const),
    )
    sendJson(res, Object.fromEntries(entries))
  })
  server.middlewares.use('/api/paper-stats', async (_req, res) => {
    sendJson(res, await paperStats())
  })
  server.middlewares.use('/api/readiness', async (_req, res) => {
    sendJson(res, await readinessReport())
  })
  server.middlewares.use('/api/scanner/status', (_req, res) => {
    sendJson(res, scannerStatus())
  })
  server.middlewares.use('/api/scanner/start', async (req, res) => {
    if (req.method !== 'POST') {
      sendJson(res, { error: 'POST required' }, 405)
      return
    }
    if (!requireSameOriginMutation(req, res)) return
    try {
      sendJson(res, await withScannerLifecycle(startScannerOwned))
    } catch (error) {
      sendJson(
        res,
        { ok: false, error: evidenceError(error), scanner: scannerStatus() },
        error instanceof DashboardConflict ? 409 : 500,
      )
    }
  })
  server.middlewares.use('/api/scanner/stop', async (req, res) => {
    if (req.method !== 'POST') {
      sendJson(res, { error: 'POST required' }, 405)
      return
    }
    if (!requireSameOriginMutation(req, res)) return
    try {
      sendJson(res, await withScannerLifecycle(stopScannerOwned))
    } catch (error) {
      sendJson(
        res,
        { ok: false, error: evidenceError(error), scanner: scannerStatus() },
        error instanceof DashboardConflict ? 409 : 500,
      )
    }
  })
  const resetHandler = (kind: ResetKind) => async (req: IncomingMessage, res: JsonResponse) => {
    if (req.method !== 'POST') {
      sendJson(res, { ok: false, error: 'POST required' }, 405)
      return
    }
    if (!requireSameOriginMutation(req, res)) return
    try {
      sendJson(res, await withScannerLifecycle(() => resetState(kind)))
    } catch (error) {
      sendJson(
        res,
        { ok: false, error: evidenceError(error), scanner: scannerStatus() },
        error instanceof DashboardConflict ? 409 : 500,
      )
    }
  }
  server.middlewares.use('/api/reset/diagnostics', resetHandler('diagnostics'))
  server.middlewares.use('/api/reset/paper', resetHandler('paper'))
  server.middlewares.use('/api/reset/all', resetHandler('all'))
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    {
      name: 'diagnostics-api',
      configureServer(server) {
        installLocalApi(server)
      },
      configurePreviewServer(server) {
        installLocalApi(server)
      },
    },
  ],
  resolve: {
    alias: {
      '@': path.resolve(process.cwd(), '@'),
    },
  },
})
