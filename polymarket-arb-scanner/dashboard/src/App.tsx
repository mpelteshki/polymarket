import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Activity,
  ArrowDownRight,
  ArrowUpRight,
  ChevronRight,
  CirclePause,
  Play,
  RefreshCw,
  RotateCcw,
  Square,
  X,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Separator } from '@/components/ui/separator'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { TooltipProvider } from '@/components/ui/tooltip'

type CsvRow = Record<string, string>
type DiagnosticsPayload = Record<string, string>
type PaperStatsPayload = {
  ok: boolean
  account?: string
  dataDir?: string
  error?: string
  balance?: {
    total_value?: number
    pnl?: number
  }
  stats?: {
    pnl?: number
    roi_pct?: number
    total_trades?: number
    win_rate?: number
    max_drawdown?: number
  }
}
type ScannerStatusPayload = {
  running: boolean
  controllable?: boolean
  ownership?: 'owned' | 'unmanaged' | 'stale' | 'idle'
  stopping?: boolean
  pid?: number
  startedAt?: string | null
  lastExit?: {
    code: number | null
    signal: string | null
    at: string
  } | null
  account?: string
  dataDir?: string
  diagnosticsDir?: string
  launchEligibility?: 'evidence_eligible' | 'requires_validation' | 'blocked'
  launchError?: string | null
  unmanagedReason?: string | null
  binaryPath?: string | null
  binarySha256?: string | null
  log?: string[]
}
type ReadinessState = 'ready' | 'blocked' | 'unknown'
type ReadinessItem = {
  key: string
  label: string
  state: ReadinessState
  value: string
  detail: string
}
type LiveAction = {
  key: string
  state: ReadinessState
  action: string
  mentionedEnvs?: string[]
}
type LiveUnblockEnv = {
  name: string
  credential?: boolean
  value_recorded?: boolean
  note?: string
}
type LiveUnblockStep = {
  step: number
  name: string
  goal: string
  evidence_needed?: string[]
  blockers?: unknown[]
}
type LiveUnblockPlan = {
  credential_values_recorded?: boolean
  required_envs?: LiveUnblockEnv[]
  operator_sequence?: LiveUnblockStep[]
}
type ReadinessBundleFile = {
  label: string
  exists?: boolean
  size_bytes?: number
  sha256?: string | null
}
type ReadinessBundleManifest = {
  overall_state?: string
  dashboard_url?: string
  pass_summary?: Record<string, boolean | number | null>
  no_live_policy?: {
    live_trade_attempted?: boolean
    account_created?: boolean
    credential_values_recorded?: boolean
  }
  live_unblock?: {
    required_env_count?: number
    operator_step_count?: number
    raw_blocker_count?: number
    credential_values_recorded?: boolean | null
  }
  files?: ReadinessBundleFile[]
}
type LiveEnvAuditSummary = {
  total_count?: number
  required_count?: number
  credential_count?: number
  present_required_count?: number
  missing_required_count?: number
  invalid_required_count?: number
  blocking_count?: number
  warning_count?: number
  ready?: boolean
}
type LiveEnvAuditBlocker = {
  name: string
  group: string
  issue?: string | null
  expected?: string
}
type LiveEnvAudit = {
  summary?: LiveEnvAuditSummary
  blocking?: LiveEnvAuditBlocker[]
  missing_required?: string[]
}
type OperatorPreflightManifest = {
  live_ready?: boolean
  run_root?: string
  result_json?: string
  manifest_json?: string
  pass_summary?: Record<string, boolean | number | null>
  no_submit_policy?: {
    live_trading_enabled_forced?: boolean
    account_created?: boolean
  }
  env_audit?: {
    path?: string | null
    template?: string | null
    ready?: boolean
    blocking_count?: number | null
    missing_required_count?: number | null
    warning_count?: number | null
  }
  liveEnvAudit?: LiveEnvAudit | null
  files?: ReadinessBundleFile[]
}
type LiveActivationPacket = {
  generated_at?: string
  status?: string
  can_enable_live?: boolean
  no_live_trade_attempted?: boolean
  output_dir?: string
  packet_file?: string
  verification?: {
    ok?: boolean
    error?: string
    verified_at?: string
  }
  artifacts?: Record<string, string>
  gate?: {
    rc?: number
    ok?: boolean
    readiness_state?: string
    readiness_blockers?: number
    operator_live_ready?: boolean
    operator_env_ready?: boolean
    operator_env_blockers?: number
    live_trading_enabled?: string
  }
  pass_summary?: {
    readiness?: Record<string, boolean | number | null>
    operator_preflight?: Record<string, boolean | number | null>
    selftest_ok?: boolean
  }
  protocol_drift?: {
    status?: string
    source_urls?: string[]
    blockers?: unknown[]
  }
}
type ReadinessPayload = {
  generatedAt?: string
  diagnosticsDir?: string
  items?: ReadinessItem[]
  liveUnblockPlan?: LiveUnblockPlan | null
  readinessBundleManifest?: ReadinessBundleManifest | null
  operatorPreflightManifest?: OperatorPreflightManifest | null
  liveActivationPacket?: LiveActivationPacket | null
  nextLiveActions?: LiveAction[]
  requiredLiveEnvs?: string[]
}
type PaperSample = { at: string; totalValue: number; pnl: number }
type ResetKind = 'diagnostics' | 'paper' | 'all'

const files = {
  scans: 'scan_summary.csv',
  trades: 'trades.csv',
  decisions: 'candidate_evaluations.csv',
  rejections: 'candidate_rejections.csv',
} as const

const navItems = ['Overview', 'Decisions', 'Trades', 'Rejections', 'Sources']

function parseCsv(text = ''): CsvRow[] {
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

function numeric(row: CsvRow | undefined, key: string) {
  const value = Number(row?.[key] ?? 0)
  return Number.isFinite(value) ? value : 0
}

function finite(value: unknown, fallback = 0) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function money(value: number) {
  const abs = Math.abs(value).toLocaleString(undefined, {
    maximumFractionDigits: 2,
    minimumFractionDigits: 2,
  })
  return `${value < 0 ? '-' : ''}$${abs}`
}

function pct(value: number) {
  return `${value.toFixed(2)}%`
}

function short(value: string, max = 72) {
  return value.length > max ? `${value.slice(0, max - 3)}...` : value
}

function titleText(value: string) {
  return value.replace(/_/g, ' ')
}

function latestRows(rows: CsvRow[], count: number) {
  return rows.slice(Math.max(0, rows.length - count)).reverse()
}

function rowKey(row: CsvRow, index = 0) {
  return [
    row.timestamp,
    row.scan_id,
    row.mode,
    row.stage,
    row.pool,
    row.event_id,
    index,
  ].join('|')
}

function sourceOf(row: CsvRow) {
  const eventId = row.event_id || ''
  return eventId.startsWith('external:') ? eventId.split(':')[1] || 'external' : 'polymarket'
}

function statusVariant(status: string) {
  const lower = status.toLowerCase()
  if (lower.includes('reject') || lower.includes('fail')) return 'destructive'
  if (lower.includes('paper') || lower.includes('submitted')) return 'secondary'
  if (lower.includes('raw') || lower.includes('scan')) return 'outline'
  return 'default'
}

function readinessVariant(state: ReadinessState) {
  if (state === 'blocked') return 'destructive'
  if (state === 'unknown') return 'outline'
  return 'secondary'
}

function readinessTone(state: ReadinessState): 'neutral' | 'good' | 'bad' {
  if (state === 'ready') return 'good'
  if (state === 'blocked') return 'bad'
  return 'neutral'
}

function numericCell(row: CsvRow, key: string) {
  return row[key] === undefined || row[key] === '' ? '-' : money(numeric(row, key))
}

function signedMoney(value: number) {
  return `${value >= 0 ? '+' : ''}${money(value)}`
}

function textCell(row: CsvRow, key: string) {
  if (key.includes('profit') || key.includes('pnl')) return numericCell(row, key)
  if (key.includes('score')) return Number(row[key] || 0).toFixed(3)
  return short(row[key] || '-')
}

function isExecution(row: CsvRow) {
  const mode = (row.mode || '').toLowerCase()
  const status = (row.status || '').toLowerCase()
  const parityOk = (row.parity_ok || '').toLowerCase()
  if (parityOk === 'false') return false
  if (mode === 'paper') return status === 'ok'
  if (mode === 'live') return status === 'ok' || status === 'settlement_confirmed_unrealized'
  if (mode === 'live_combo_rfq') return status === 'accepted_pending_finality'
  return false
}

function isPaperExecution(row: CsvRow) {
  return (row.mode || '').toLowerCase() === 'paper' && isExecution(row)
}

function SeriesChart({
  values,
  format = money,
  tone = 'neutral',
}: {
  values: number[]
  format?: (value: number) => string
  tone?: 'neutral' | 'good' | 'bad'
}) {
  const series = values.length ? values : [0]
  const min = Math.min(0, ...series)
  const max = Math.max(0, ...series)
  const span = max - min || 1
  const w = 720
  const h = 260
  const left = 64
  const right = 16
  const top = 18
  const bottom = 34
  const plotW = w - left - right
  const plotH = h - top - bottom
  const lineClass =
    tone === 'good' ? 'stroke-emerald-600' : tone === 'bad' ? 'stroke-red-600' : 'stroke-foreground'
  const areaClass =
    tone === 'good' ? 'fill-emerald-600/10' : tone === 'bad' ? 'fill-red-600/10' : 'fill-foreground/10'
  const points = series.map((value, index) => {
    const x = left + (series.length === 1 ? 0 : (index / (series.length - 1)) * plotW)
    const y = top + plotH - ((value - min) / span) * plotH
    return [x, y] as const
  })
  const line = points.map(([x, y], index) => `${index === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ')
  const area = `${line} L${points.at(-1)?.[0].toFixed(1)},${top + plotH} L${left},${top + plotH} Z`
  const yTicks = [0, 0.5, 1].map((ratio) => max - ratio * span)
  const zeroY = top + plotH - ((0 - min) / span) * plotH

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="h-[260px] w-full">
      {yTicks.map((tick) => {
        const y = top + plotH - ((tick - min) / span) * plotH
        return (
          <g key={tick}>
            <path d={`M${left} ${y.toFixed(1)}H${w - right}`} className="stroke-border" strokeWidth="1" />
            <text x="8" y={y + 4} className="fill-muted-foreground text-[11px]">
              {format(tick)}
            </text>
          </g>
        )
      })}
      <path d={`M${left} ${zeroY.toFixed(1)}H${w - right}`} className="stroke-foreground/40" strokeWidth="1" />
      <path d={area} className={areaClass} strokeWidth="0" />
      <path d={line} className={lineClass} strokeWidth="3" strokeLinecap="round" fill="none" />
      <circle
        cx={points.at(-1)?.[0] ?? left}
        cy={points.at(-1)?.[1] ?? top + plotH}
        r="5"
        className={tone === 'good' ? 'fill-emerald-600' : tone === 'bad' ? 'fill-red-600' : 'fill-foreground'}
      />
      <text x={left} y={h - 8} className="fill-muted-foreground text-[11px]">
        oldest
      </text>
      <text x={w - right - 34} y={h - 8} className="fill-muted-foreground text-[11px]">
        latest
      </text>
    </svg>
  )
}

function Metric({
  label,
  value,
  detail,
  tone = 'neutral',
}: {
  label: string
  value: string
  detail: string
  tone?: 'neutral' | 'good' | 'bad'
}) {
  const Icon = tone === 'bad' ? ArrowDownRight : tone === 'good' ? ArrowUpRight : Activity
  return (
    <Card size="sm" className="rounded-lg">
      <CardHeader>
        <CardDescription>{label}</CardDescription>
        <CardTitle className="flex items-center justify-between text-xl">
          {value}
          <Icon data-icon="inline-end" className="text-muted-foreground" />
        </CardTitle>
      </CardHeader>
      <CardContent className="text-xs text-muted-foreground">{detail}</CardContent>
    </Card>
  )
}

function ReadinessCard({ item }: { item: ReadinessItem }) {
  const tone = readinessTone(item.state)
  const Icon = tone === 'bad' ? ArrowDownRight : tone === 'good' ? ArrowUpRight : Activity
  return (
    <Card
      size="sm"
      className={
        item.state === 'ready'
          ? 'rounded-lg border-emerald-600/30 bg-emerald-600/5'
          : item.state === 'blocked'
            ? 'rounded-lg border-red-600/30 bg-red-600/5'
            : 'rounded-lg'
      }
    >
      <CardHeader className="gap-2">
        <div className="flex items-center justify-between gap-3">
          <CardDescription>{item.label}</CardDescription>
          <Badge variant={readinessVariant(item.state)}>{item.state}</Badge>
        </div>
        <CardTitle className="flex items-center justify-between gap-3 text-xl">
          <span className="truncate">{item.value}</span>
          <Icon data-icon="inline-end" className="shrink-0 text-muted-foreground" />
        </CardTitle>
      </CardHeader>
      <CardContent className="line-clamp-2 break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
        {item.detail}
      </CardContent>
    </Card>
  )
}

function LiveActionsPanel({
  actions,
  envs,
  plan,
}: {
  actions: LiveAction[]
  envs: string[]
  plan?: LiveUnblockPlan | null
}) {
  const planSteps = plan?.operator_sequence ?? []
  const planEnvs = plan?.required_envs ?? []
  if (actions.length === 0 && envs.length === 0 && planSteps.length === 0) return null
  const visibleActions = actions.slice(0, 5)
  const visibleEnvs = planEnvs.length
    ? planEnvs.slice(0, 24)
    : envs.slice(0, 18).map((name) => ({ name, credential: false, value_recorded: false }))
  return (
    <Card className="mb-4 rounded-lg border-red-600/30 bg-red-600/5">
      <CardHeader>
        <CardDescription>Live unblock path</CardDescription>
        <CardTitle className="flex flex-wrap items-center gap-2 text-lg">
          Operator gate map
          {plan ? (
            <Badge variant={plan.credential_values_recorded ? 'destructive' : 'outline'}>
              {plan.credential_values_recorded ? 'values recorded' : 'no credential values'}
            </Badge>
          ) : null}
        </CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3">
        {planSteps.length ? (
          <div className="grid gap-2 xl:grid-cols-5">
            {planSteps.map((step) => (
              <div key={step.name} className="grid min-w-0 gap-2 rounded-md border bg-background/70 p-3">
                <div className="flex items-center justify-between gap-2">
                  <span className="font-mono text-xs">step {step.step}</span>
                  <Badge variant={(step.blockers?.length ?? 0) ? 'destructive' : 'secondary'}>
                    {step.blockers?.length ?? 0} blockers
                  </Badge>
                </div>
                <div className="text-sm font-medium capitalize">{titleText(step.name)}</div>
                <div className="line-clamp-2 break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
                  {step.goal}
                </div>
                <div className="line-clamp-2 break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
                  {(step.evidence_needed ?? []).slice(0, 3).join(' / ')}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="grid gap-2">
            {visibleActions.map((item) => (
              <div key={item.key} className="grid gap-1 rounded-md border bg-background/70 p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={readinessVariant(item.state)}>{item.state}</Badge>
                  <span className="font-mono text-xs">{item.key}</span>
                </div>
                <div className="text-sm">{item.action}</div>
              </div>
            ))}
          </div>
        )}
        {visibleEnvs.length ? (
          <div className="flex flex-wrap gap-2">
            {visibleEnvs.map((env) => (
              <Badge
                key={env.name}
                variant={env.credential ? 'secondary' : 'outline'}
                className="h-auto max-w-full justify-start whitespace-normal text-left font-mono break-all"
              >
                {env.name}
              </Badge>
            ))}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}

function BundleManifestPanel({ manifest }: { manifest?: ReadinessBundleManifest | null }) {
  if (!manifest) return null
  const files = manifest.files ?? []
  const missing = files.filter((file) => !file.exists).length
  const unhashed = files.filter((file) => file.exists && !file.sha256).length
  const passEntries = Object.entries(manifest.pass_summary ?? {})
  const allProofsReady = missing === 0 && unhashed === 0
  return (
    <Card className="mb-4 rounded-lg border-emerald-600/30 bg-emerald-600/5">
      <CardHeader>
        <CardDescription>Proof bundle</CardDescription>
        <CardTitle className="flex flex-wrap items-center gap-2 text-lg">
          Readiness bundle manifest
          <Badge variant={allProofsReady ? 'secondary' : 'destructive'}>
            {allProofsReady ? 'all hashed' : 'check files'}
          </Badge>
          <Badge variant="outline">{manifest.overall_state ?? 'unknown'}</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3 text-sm">
        <div className="grid gap-2 md:grid-cols-4">
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Files</div>
            <div className="font-mono text-lg">{files.length}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Missing</div>
            <div className="font-mono text-lg">{missing}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Unhashed</div>
            <div className="font-mono text-lg">{unhashed}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Live policy</div>
            <div className="font-mono text-lg">
              {manifest.no_live_policy?.live_trade_attempted ? 'live' : 'no-live'}
            </div>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          {passEntries.map(([key, value]) => (
            <Badge
              key={key}
              variant={value === true || value === 0 ? 'secondary' : 'outline'}
              className="h-auto max-w-full justify-start whitespace-normal text-left font-mono break-all"
            >
              {key}: {String(value)};
            </Badge>
          ))}
        </div>
        <div className="flex flex-wrap gap-2 text-xs text-muted-foreground">
          <span>{manifest.live_unblock?.operator_step_count ?? 0} operator steps</span>
          <span>{manifest.live_unblock?.required_env_count ?? 0} env names</span>
          <span>{manifest.live_unblock?.raw_blocker_count ?? 0} live blockers</span>
          <span className="break-all">{manifest.dashboard_url ?? '-'}</span>
        </div>
      </CardContent>
    </Card>
  )
}

function OperatorPreflightPanel({ manifest }: { manifest?: OperatorPreflightManifest | null }) {
  if (!manifest) return null
  const files = manifest.files ?? []
  const missing = files.filter((file) => !file.exists).length
  const unhashed = files.filter((file) => file.exists && !file.sha256).length
  const passEntries = Object.entries(manifest.pass_summary ?? {})
  const envAudit = manifest.liveEnvAudit ?? null
  const envBlockers = envAudit?.blocking ?? []
  const envGroups = Object.entries(
    envBlockers.reduce<Record<string, LiveEnvAuditBlocker[]>>((groups, blocker) => {
      const key = blocker.group || 'live_env'
      groups[key] = [...(groups[key] ?? []), blocker]
      return groups
    }, {}),
  )
  const allProofsReady = missing === 0 && unhashed === 0
  const liveForcedOff = manifest.no_submit_policy?.live_trading_enabled_forced === false
  const envReady = manifest.env_audit?.ready ?? envAudit?.summary?.ready
  return (
    <Card className="mb-4 rounded-lg border-sky-600/30 bg-sky-600/5">
      <CardHeader>
        <CardDescription>Operator preflight proof</CardDescription>
        <CardTitle className="flex flex-wrap items-center gap-2 text-lg">
          Live no-submit preflight
          <Badge variant={allProofsReady ? 'secondary' : 'destructive'}>
            {allProofsReady ? 'all hashed' : 'check files'}
          </Badge>
          <Badge variant={liveForcedOff ? 'secondary' : 'destructive'}>
            {liveForcedOff ? 'forced off' : 'live allowed'}
          </Badge>
          <Badge variant={manifest.live_ready ? 'secondary' : 'outline'}>
            {manifest.live_ready ? 'live ready' : 'live blocked'}
          </Badge>
          <Badge variant={envReady ? 'secondary' : 'outline'}>
            env {envReady ? 'ready' : 'blocked'}
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3 text-sm">
        <div className="grid gap-2 md:grid-cols-4">
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Files</div>
            <div className="font-mono text-lg">{files.length}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Missing</div>
            <div className="font-mono text-lg">{missing}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Unhashed</div>
            <div className="font-mono text-lg">{unhashed}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Live ready</div>
            <div className="font-mono text-lg">{manifest.live_ready ? 'true' : 'false'}</div>
          </div>
        </div>
        {envAudit ? (
          <div className="grid gap-3">
            <div className="grid gap-2 md:grid-cols-4">
              <div className="rounded-md border bg-background/70 p-3">
                <div className="text-xs text-muted-foreground">Env blockers</div>
                <div className="font-mono text-lg">{envAudit.summary?.blocking_count ?? 0}</div>
              </div>
              <div className="rounded-md border bg-background/70 p-3">
                <div className="text-xs text-muted-foreground">Missing envs</div>
                <div className="font-mono text-lg">
                  {envAudit.summary?.missing_required_count ?? 0}
                </div>
              </div>
              <div className="rounded-md border bg-background/70 p-3">
                <div className="text-xs text-muted-foreground">Invalid envs</div>
                <div className="font-mono text-lg">
                  {envAudit.summary?.invalid_required_count ?? 0}
                </div>
              </div>
              <div className="rounded-md border bg-background/70 p-3">
                <div className="text-xs text-muted-foreground">Required envs</div>
                <div className="font-mono text-lg">{envAudit.summary?.required_count ?? 0}</div>
              </div>
            </div>
            {envGroups.length ? (
              <div className="grid gap-2 md:grid-cols-2">
                {envGroups.map(([group, blockers]) => (
                  <div key={group} className="rounded-md border bg-background/70 p-3">
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <div className="text-xs font-medium text-muted-foreground">{group}</div>
                      <Badge variant="outline">{blockers.length}</Badge>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {blockers.map((blocker) => (
                        <Badge
                          key={`${group}-${blocker.name}`}
                          variant="outline"
                          className="h-auto max-w-full justify-start whitespace-normal text-left font-mono break-all"
                        >
                          {blocker.name}:{blocker.issue ?? 'blocked'}:{blocker.expected ?? '-'}
                        </Badge>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
        <div className="flex flex-wrap gap-2">
          {passEntries.map(([key, value]) => (
            <Badge
              key={key}
              variant={value === true || value === 0 ? 'secondary' : 'outline'}
              className="h-auto max-w-full justify-start whitespace-normal text-left font-mono break-all"
            >
              {key}: {String(value)};
            </Badge>
          ))}
        </div>
        <div className="grid gap-1 text-xs text-muted-foreground [overflow-wrap:anywhere]">
          <span>run={manifest.run_root ?? '-'}</span>
          <span>result={manifest.result_json ?? '-'}</span>
          <span>manifest={manifest.manifest_json ?? '-'}</span>
          <span>env_template={manifest.env_audit?.template ?? '-'}</span>
        </div>
      </CardContent>
    </Card>
  )
}

function ActivationPacketPanel({ packet }: { packet?: LiveActivationPacket | null }) {
  if (!packet) return null
  const gate = packet.gate ?? {}
  const verified = packet.verification?.ok === true
  const canEnable = verified && packet.can_enable_live === true
  const gateOk = verified && gate.ok === true
  const selftestOk = packet.pass_summary?.selftest_ok === true
  const noLive = packet.no_live_trade_attempted !== false
  const protocolStatus = packet.protocol_drift?.status ?? 'unknown'
  const protocolBlockers = packet.protocol_drift?.blockers?.length ?? 0
  const protocolSources = packet.protocol_drift?.source_urls ?? []
  const readinessPasses = Object.entries(packet.pass_summary?.readiness ?? {})
  const operatorPasses = Object.entries(packet.pass_summary?.operator_preflight ?? {})
  const artifactEntries = Object.entries(packet.artifacts ?? {}).filter(([key]) =>
    [
      'packet_json',
      'packet_markdown',
      'readiness_manifest',
      'operator_preflight_manifest',
      'live_ready_gate_report',
      'activation_packet_verification',
      'live_env_template',
    ].includes(key),
  )
  const passVariant = (value: boolean | number | null) =>
    value === true || value === 0 ? 'secondary' : 'outline'

  return (
    <Card
      className={
        canEnable
          ? 'mb-4 rounded-lg border-emerald-600/30 bg-emerald-600/5'
          : 'mb-4 rounded-lg border-red-600/30 bg-red-600/5'
      }
    >
      <CardHeader>
        <CardDescription>Activation packet</CardDescription>
        <CardTitle className="flex flex-wrap items-center gap-2 text-lg">
          Live enable decision
          <Badge variant={canEnable ? 'secondary' : 'destructive'}>
            {canEnable ? 'enable allowed' : 'enable blocked'}
          </Badge>
          <Badge variant={verified ? 'secondary' : 'destructive'}>
            {verified ? 'packet verified' : 'packet unverified'}
          </Badge>
          <Badge variant={gateOk ? 'secondary' : 'destructive'}>
            gate {gateOk ? 'ok' : 'blocked'}
          </Badge>
          <Badge variant={selftestOk ? 'secondary' : 'outline'}>
            selftest {selftestOk ? 'ok' : 'missing'}
          </Badge>
          <Badge variant={noLive ? 'secondary' : 'destructive'}>
            {noLive ? 'no live trade' : 'live trade seen'}
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3 text-sm">
        <div className="grid gap-2 md:grid-cols-4">
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Status</div>
            <div className="font-mono text-lg">{packet.status ?? 'unknown'}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Gate rc</div>
            <div className="font-mono text-lg">{gate.rc ?? '-'}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Readiness blockers</div>
            <div className="font-mono text-lg">{gate.readiness_blockers ?? 0}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Env blockers</div>
            <div className="font-mono text-lg">{gate.operator_env_blockers ?? 0}</div>
          </div>
        </div>

        <div className="grid gap-2 md:grid-cols-4">
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Protocol drift</div>
            <div className="font-mono text-lg">{protocolStatus}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Protocol blockers</div>
            <div className="font-mono text-lg">{protocolBlockers}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Source URLs</div>
            <div className="font-mono text-lg">{protocolSources.length}</div>
          </div>
          <div className="rounded-md border bg-background/70 p-3">
            <div className="text-xs text-muted-foreground">Live enabled</div>
            <div className="font-mono text-lg">{gate.live_trading_enabled ?? 'false'}</div>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          {[...readinessPasses, ...operatorPasses].map(([key, value]) => (
            <Badge
              key={key}
              variant={passVariant(value)}
              className="h-auto max-w-full justify-start whitespace-normal text-left font-mono break-all"
            >
              {key}: {String(value)};
            </Badge>
          ))}
        </div>

        <div className="grid gap-1 text-xs text-muted-foreground [overflow-wrap:anywhere]">
          <span>packet={packet.packet_file ?? packet.artifacts?.packet_json ?? '-'}</span>
          <span>output={packet.output_dir ?? '-'}</span>
          {artifactEntries.map(([key, value]) => (
            <span key={key}>
              {key}={value}
            </span>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}

export default function App() {
  const [payload, setPayload] = useState<DiagnosticsPayload>({})
  const [paperStats, setPaperStats] = useState<PaperStatsPayload>({ ok: false })
  const [readiness, setReadiness] = useState<ReadinessPayload>({})
  const [paperSeries, setPaperSeries] = useState<PaperSample[]>([])
  const [scanner, setScanner] = useState<ScannerStatusPayload>({ running: false })
  const [scannerBusy, setScannerBusy] = useState(false)
  const [resetBusy, setResetBusy] = useState<ResetKind | null>(null)
  const [paused, setPaused] = useState(false)
  const [range, setRange] = useState('100')
  const [source, setSource] = useState('all')
  const [activeNav, setActiveNav] = useState('Overview')
  const [tableView, setTableView] = useState<'decisions' | 'trades' | 'rejections' | 'sources'>('trades')
  const [selectedRow, setSelectedRow] = useState<CsvRow | null>(null)
  const [updatedAt, setUpdatedAt] = useState('')

  const load = useCallback(async () => {
    const [diagnostics, paper, scannerStatus, readinessStatus] = await Promise.all([
      fetch('/api/diagnostics').then((response) => response.json()),
      fetch('/api/paper-stats')
        .then((response) => response.json())
        .catch((error) => ({ ok: false, error: String(error) })),
      fetch('/api/scanner/status')
        .then((response) => response.json())
        .catch(() => ({ running: false })),
      fetch('/api/readiness')
        .then((response) => response.json())
        .catch((error) => ({
          items: [
            {
              key: 'ui',
              label: 'UI',
              state: 'blocked',
              value: 'API error',
              detail: String(error),
            },
          ],
        })),
    ])
    setPayload(diagnostics)
    setPaperStats(paper)
    setReadiness(readinessStatus)
    if (paper.ok) {
      const now = new Date().toLocaleTimeString()
      setPaperSeries((samples) =>
        [
          ...samples,
          {
            at: now,
            totalValue: finite(paper.balance?.total_value),
            pnl: finite(paper.stats?.pnl ?? paper.balance?.pnl),
          },
        ].slice(-Number(range)),
      )
    }
    setScanner(scannerStatus)
    setUpdatedAt(new Date().toLocaleTimeString())
  }, [range])

  async function scannerAction(action: 'start' | 'stop') {
    setScannerBusy(true)
    try {
      const response = await fetch(`/api/scanner/${action}`, { method: 'POST' })
      const body = await response.json()
      if (!response.ok || body.ok === false) {
        setScanner(body.scanner ?? scanner)
        throw new Error(body.error || `Scanner ${action} failed`)
      }
      setScanner(body)
      void load()
    } catch (error) {
      window.alert(`Scanner action failed: ${error instanceof Error ? error.message : String(error)}`)
    } finally {
      setScannerBusy(false)
    }
  }

  async function resetAction(kind: ResetKind) {
    const label =
      kind === 'diagnostics'
        ? 'scan log and visible diagnostics'
        : kind === 'paper'
          ? 'paper account balance, trades, and positions'
          : 'scan log, diagnostics, paper balance, trades, and positions'
    const confirmed = window.confirm(
      `Reset ${label}? The scanner will stop briefly and restart if it is running.`,
    )
    if (!confirmed) return

    setResetBusy(kind)
    try {
      const response = await fetch(`/api/reset/${kind}`, { method: 'POST' })
      const body = await response.json()
      if (!response.ok || body.ok === false) {
        throw new Error(body.error || 'Reset failed')
      }
      setPayload({})
      setPaperSeries([])
      setSelectedRow(null)
      setScanner(body.scanner ?? { running: false })
      await load()
    } catch (error) {
      window.alert(`Reset failed: ${error instanceof Error ? error.message : String(error)}`)
    } finally {
      setResetBusy(null)
    }
  }

  function goNav(item: string) {
    setActiveNav(item)
    setSelectedRow(null)
    if (item === 'Decisions') setTableView('decisions')
    if (item === 'Trades') setTableView('trades')
    if (item === 'Rejections') setTableView('rejections')
    if (item === 'Sources') setTableView('sources')
    const id = item === 'Overview' ? 'overview' : item === 'Sources' ? 'sources' : 'activity'
    window.requestAnimationFrame(() => document.getElementById(id)?.scrollIntoView({ behavior: 'smooth' }))
  }

  function changeSource(value: string) {
    setSource(value)
    setSelectedRow(null)
  }

  function changeTableView(value: string) {
    const next = value as typeof tableView
    setTableView(next)
    setSelectedRow(null)
    setActiveNav(
      next === 'decisions'
        ? 'Decisions'
        : next === 'rejections'
          ? 'Rejections'
          : next === 'sources'
            ? 'Sources'
            : 'Trades',
    )
  }

  useEffect(() => {
    if (paused) return
    let cancelled = false
    let timerId = 0
    const poll = async () => {
      await load().catch(() => undefined)
      if (!cancelled) timerId = window.setTimeout(poll, 5000)
    }
    void poll()
    return () => {
      cancelled = true
      window.clearTimeout(timerId)
    }
  }, [load, paused])

  const data = useMemo(() => {
    const scans = parseCsv(payload[files.scans])
    const trades = parseCsv(payload[files.trades])
    const decisions = parseCsv(payload[files.decisions])
    const rejections = parseCsv(payload[files.rejections])
    const windowSize = Number(range)
    const scannerStartedAt = Date.parse(scanner.startedAt ?? '')
    const useSession = Number.isFinite(scannerStartedAt)
    const inSession = (row: CsvRow) => {
      const timestamp = Date.parse(row.timestamp ?? '')
      return !useSession || !Number.isFinite(timestamp) || timestamp >= scannerStartedAt - 1000
    }
    const sessionScans = scans.filter(inSession)
    const sessionTrades = trades.filter(inSession)
    const sessionDecisions = decisions.filter(inSession)
    const sessionRejections = rejections.filter(inSession)
    const scopedScans = sessionScans.slice(Math.max(0, sessionScans.length - windowSize))
    const latest = scopedScans.at(-1)
    const sources = Array.from(
      new Set(
        [...sessionDecisions, ...sessionRejections, ...sessionTrades]
          .map(sourceOf)
          .filter(Boolean),
      ),
    ).sort()
    const filterSource = (row: CsvRow) => source === 'all' || sourceOf(row) === source

    return {
      scans: scopedScans,
      trades: sessionTrades.filter(filterSource),
      decisions: sessionDecisions.filter(filterSource),
      rejections: sessionRejections.filter(filterSource),
      latest,
      sources,
    }
  }, [payload, range, scanner.startedAt, source])

  const latest = data.latest
  const projectedPnl = numeric(latest, 'cumulative_pnl_usd')
  const projectedRoi = numeric(latest, 'cumulative_pnl_pct')
  const paperPnl = finite(paperStats.stats?.pnl ?? paperStats.balance?.pnl)
  const paperRoi = finite(paperStats.stats?.roi_pct)
  const paperValue = finite(paperStats.balance?.total_value, 10_000 + paperPnl)
  const quoteReady =
    numeric(latest, 'quote_ready_yes_events') +
    numeric(latest, 'quote_ready_no_events') +
    numeric(latest, 'quote_ready_bundle_markets')
  const unresolved = numeric(latest, 'quote_hard_unresolved_tokens')
  const opportunities = numeric(latest, 'opportunities_found')
  const trades = numeric(latest, 'cumulative_trades_executed')
  const executions = data.trades.filter(isExecution)
  const paperExecutions = data.trades.filter(isPaperExecution)
  const tradeRows = latestRows(executions, 12)
  const rejectionRows = latestRows(data.rejections, 12)
  const decisionRows = latestRows(data.decisions, 12)
  const sourceRows = data.sources.map((name) => {
    const rows = [...data.decisions, ...data.rejections, ...data.trades].filter((row) => sourceOf(row) === name)
    return {
      source: name,
      rows: String(rows.length),
      decisions: String(data.decisions.filter((row) => sourceOf(row) === name).length),
      rejections: String(data.rejections.filter((row) => sourceOf(row) === name).length),
      submissions: String(data.trades.filter((row) => sourceOf(row) === name && isExecution(row)).length),
    }
  })
  const winRate = paperStats.ok ? finite(paperStats.stats?.win_rate) : null
  const brokerPaperTrades = paperStats.ok ? finite(paperStats.stats?.total_trades) : 0
  const paperTrades = paperExecutions.length
  const lastExecution = executions.at(-1)
  const detailRow = selectedRow ?? lastExecution ?? null
  const paperPnlSeries = paperSeries.map((sample) => sample.pnl)
  const readinessItems = readiness.items ?? []
  const liveActions = readiness.nextLiveActions ?? []
  const liveEnvs = readiness.requiredLiveEnvs ?? []
  const liveUnblockPlan = readiness.liveUnblockPlan ?? null
  const readinessBundleManifest = readiness.readinessBundleManifest ?? null
  const operatorPreflightManifest = readiness.operatorPreflightManifest ?? null
  const liveActivationPacket = readiness.liveActivationPacket ?? null
  const paperProfitProven = paperStats.ok && paperTrades > 0 && paperPnl > 0
  const profitHeadline =
    !paperStats.ok
      ? 'Waiting for paper stats.'
      : paperProfitProven
        ? 'Profitable on current marks.'
        : 'No paper profit proof.'
  const profitSummary =
    !paperStats.ok
      ? 'Paper account stats unavailable.'
      : paperTrades <= 0
        ? `Scanner has no accepted paper fills in current diagnostics. Broker history has ${brokerPaperTrades} rows.`
      : paperPnl < 0 && projectedPnl >= 0
        ? `Current marks are ${signedMoney(paperPnl)}; settlement projection is ${signedMoney(projectedPnl)}.`
        : projectedPnl >= 0
          ? `Current marks are ${signedMoney(paperPnl)}; settlement projection is ${signedMoney(projectedPnl)}.`
          : `Current marks are ${signedMoney(paperPnl)}; settlement projection is not positive.`
  const tableConfig =
    tableView === 'decisions'
      ? {
          title: 'Recent decisions',
          description: 'Selection scores from candidate_evaluations.csv',
          rows: decisionRows,
          columns: [
            ['pool', 'Pool'],
            ['event_title', 'Event'],
            ['selection_state', 'State'],
            ['candidate_score', 'Score'],
          ] as [string, string][],
        }
      : tableView === 'rejections'
        ? {
            title: 'Recent rejections',
            description: 'Filtered candidates from candidate_rejections.csv',
            rows: rejectionRows,
            columns: [
              ['stage', 'Stage'],
              ['event_title', 'Event'],
              ['reason', 'Reason'],
              ['projected_net_profit', 'Projected PnL'],
            ] as [string, string][],
          }
        : tableView === 'sources'
          ? {
              title: 'Sources',
              description: 'Rows currently loaded by venue',
              rows: sourceRows,
              columns: [
                ['source', 'Source'],
                ['decisions', 'Decisions'],
                ['rejections', 'Rejections'],
                ['submissions', 'Submissions'],
              ] as [string, string][],
            }
          : {
              title: 'Executions',
              description: 'Accepted hedged paper/live submissions from trades.csv',
              rows: tradeRows,
              columns: [
                ['status', 'Status'],
                ['event_title', 'Event'],
                ['arb_type', 'Type'],
                ['conservative_pnl_usd', 'Conservative PnL'],
              ] as [string, string][],
            }
  const scannerLabel = scanner.running
    ? scanner.stopping
      ? 'stopping'
      : 'running'
    : 'stopped'
  const scannerDetail = scanner.running
    ? `scanner pid ${scanner.pid ?? '-'} ownership=${scanner.ownership ?? 'unknown'}${scanner.unmanagedReason ? ` ${scanner.unmanagedReason}` : ''}`
    : scanner.lastExit
      ? `last exit ${scanner.lastExit.code ?? scanner.lastExit.signal ?? 'ok'}`
      : `scanner idle; launch=${scanner.launchEligibility ?? 'unknown'}${scanner.launchError ? ` ${scanner.launchError}` : ''}`

  return (
    <TooltipProvider>
      <div className="min-h-screen bg-background text-foreground">
        <div className="grid min-h-screen grid-cols-1 lg:grid-cols-[220px_1fr]">
          <aside className="border-b bg-muted/30 p-4 lg:border-r lg:border-b-0">
            <div className="mb-6 flex items-center gap-2">
              <div className="flex size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
                A
              </div>
              <div>
                <div className="text-sm font-semibold">Arb Scanner</div>
                <div className="text-xs text-muted-foreground">Trading operations</div>
              </div>
            </div>
            <nav className="flex flex-wrap gap-1 lg:flex-col">
              {navItems.map((item) => (
                <Button
                  key={item}
                  variant={item === activeNav ? 'secondary' : 'ghost'}
                  size="sm"
                  className="justify-start"
                  onClick={() => goNav(item)}
                >
                  {item}
                </Button>
              ))}
            </nav>
          </aside>

          <main id="overview" className="min-w-0 scroll-mt-4 p-4 lg:p-6">
            <header className="mb-4 flex flex-col gap-3 border-b pb-4 xl:flex-row xl:items-center xl:justify-between">
              <div>
                <h1 className="text-2xl font-semibold">Trade readiness monitor</h1>
                <p className="text-sm text-muted-foreground">
                  runtime_diagnostics / {updatedAt || 'waiting for data'} / paper, live, UI, HFT
                </p>
                <p className="text-xs text-muted-foreground">
                  Paper account: {paperStats.account || 'unavailable'}{' '}
                  {paperStats.ok ? 'live stats loaded' : 'stats unavailable'}
                </p>
                <p className="text-xs text-muted-foreground">{scannerDetail}</p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant={scanner.running ? 'secondary' : 'outline'}>{scannerLabel}</Badge>
                <Button
                  size="sm"
                  variant={scanner.running ? 'outline' : 'default'}
                  disabled={
                    scannerBusy ||
                    resetBusy !== null ||
                    scanner.stopping ||
                    (scanner.running && scanner.controllable !== true)
                  }
                  onClick={() => void scannerAction(scanner.running ? 'stop' : 'start')}
                >
                  {scanner.running ? (
                    <Square data-icon="inline-start" />
                  ) : (
                    <Play data-icon="inline-start" />
                  )}
                  {scanner.running ? 'Stop scanner' : 'Start scanner'}
                </Button>
                <div className="flex flex-wrap items-center gap-1 rounded-lg border p-1">
                  <span className="flex h-7 items-center gap-1 px-2 text-xs text-muted-foreground">
                    <RotateCcw data-icon="inline-start" />
                    Reset
                  </span>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={resetBusy !== null}
                    onClick={() => void resetAction('diagnostics')}
                  >
                    {resetBusy === 'diagnostics' ? 'Resetting' : 'Scan log'}
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={resetBusy !== null}
                    onClick={() => void resetAction('paper')}
                  >
                    {resetBusy === 'paper' ? 'Resetting' : 'Paper'}
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={resetBusy !== null}
                    onClick={() => void resetAction('all')}
                  >
                    {resetBusy === 'all' ? 'Resetting' : 'All'}
                  </Button>
                </div>
                <Tabs value={range} onValueChange={setRange}>
                  <TabsList>
                    <TabsTrigger value="25">25 scans</TabsTrigger>
                    <TabsTrigger value="100">100 scans</TabsTrigger>
                    <TabsTrigger value="500">500 scans</TabsTrigger>
                  </TabsList>
                </Tabs>
                <Select value={source} onValueChange={changeSource}>
                  <SelectTrigger size="sm" aria-label="Source filter">
                    <SelectValue placeholder="Source" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      <SelectItem value="all">All sources</SelectItem>
                      {data.sources.map((item) => (
                        <SelectItem key={item} value={item}>
                          {item}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  </SelectContent>
                </Select>
                <div className="flex h-7 items-center gap-2 rounded-lg border px-2 text-xs">
                  <CirclePause data-icon="inline-start" />
                  <span>Pause</span>
                  <Switch
                    aria-label="Pause auto refresh"
                    checked={paused}
                    onCheckedChange={setPaused}
                  />
                </div>
                <Button size="sm" variant="outline" onClick={() => void load()}>
                  <RefreshCw data-icon="inline-start" />
                  Refresh
                </Button>
              </div>
            </header>

            <section className="mb-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {readinessItems.length ? (
                readinessItems.map((item) => <ReadinessCard key={item.key} item={item} />)
              ) : (
                <Card className="rounded-lg">
                  <CardHeader>
                    <CardDescription>Readiness</CardDescription>
                    <CardTitle>waiting for data</CardTitle>
                  </CardHeader>
                  <CardContent className="text-sm text-muted-foreground">
                    Local readiness API has not responded yet.
                  </CardContent>
                </Card>
              )}
            </section>

            <LiveActionsPanel actions={liveActions} envs={liveEnvs} plan={liveUnblockPlan} />
            <ActivationPacketPanel packet={liveActivationPacket} />
            <BundleManifestPanel manifest={readinessBundleManifest} />
            <OperatorPreflightPanel manifest={operatorPreflightManifest} />

            <section className="mb-4 grid gap-3 xl:grid-cols-[1fr_1fr_1.1fr]">
              <Card className="rounded-lg border-emerald-600/30 bg-emerald-600/5">
                <CardHeader>
                  <CardDescription>Settlement projection</CardDescription>
                  <CardTitle className="text-3xl text-emerald-700">{signedMoney(projectedPnl)}</CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  {pct(projectedRoi)} ROI if accepted hedged baskets settle as modeled
                </CardContent>
              </Card>
              <Card className="rounded-lg border-red-600/30 bg-red-600/5">
                <CardHeader>
                  <CardDescription>Open mark-to-market</CardDescription>
                  <CardTitle className="text-3xl text-red-700">
                    {paperStats.ok ? signedMoney(paperPnl) : '-'}
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm text-muted-foreground">
                  {paperStats.ok ? `${pct(paperRoi)} account ROI from pm-trader marks` : 'pm-trader stats unavailable'}
                </CardContent>
              </Card>
              <Card className="rounded-lg">
                <CardHeader>
                  <CardDescription>Profitability read</CardDescription>
                  <CardTitle className={paperStats.ok && paperPnl < 0 ? 'text-red-700' : 'text-emerald-700'}>
                    {profitHeadline}
                  </CardTitle>
                </CardHeader>
                <CardContent className="grid gap-2 text-sm text-muted-foreground">
                  <div>{profitSummary}</div>
                  <div className="flex items-center justify-between gap-3">
                    <span>Accepted hedged trades</span>
                    <span className="font-mono text-foreground">{executions.length}</span>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span>Latest accepted trade</span>
                    <span className="font-mono text-foreground">
                      {lastExecution ? signedMoney(numeric(lastExecution, 'conservative_pnl_usd')) : '-'}
                    </span>
                  </div>
                </CardContent>
              </Card>
            </section>

            <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
              <Metric
                label="Paper value"
                value={paperStats.ok ? money(paperValue) : '-'}
                detail={paperStats.ok ? 'pm-trader account value' : 'pm-trader stats unavailable'}
              />
              <Metric
                label="Paper PnL"
                value={paperStats.ok ? money(paperPnl) : '-'}
                detail={paperStats.ok ? `${pct(paperRoi)} account ROI` : 'pm-trader stats unavailable'}
                tone={paperPnl >= 0 ? 'good' : 'bad'}
              />
              <Metric
                label="Submitted PnL"
                value={money(projectedPnl)}
                detail={`${pct(projectedRoi)} projected ROI`}
                tone={projectedPnl >= 0 ? 'good' : 'bad'}
              />
              <Metric
                label="Win rate"
                value={winRate === null ? '-' : pct(winRate)}
                detail={paperStats.ok ? `${brokerPaperTrades} broker rows; ${paperTrades} scanner fills` : 'pm-trader stats unavailable'}
              />
              <Metric label="Submissions" value={String(trades)} detail={`${opportunities} opps latest scan`} />
              <Metric label="Quote ready" value={String(quoteReady)} detail={`${unresolved} unresolved tokens`} />
            </section>

            <section className="mt-4 grid gap-4 xl:grid-cols-[1.35fr_0.65fr]">
              <Card className="rounded-lg">
                <CardHeader>
                  <CardTitle className="flex items-center justify-between gap-3">
                    <span>Submitted settlement PnL</span>
                    <span className={projectedPnl >= 0 ? 'text-emerald-700' : 'text-red-700'}>
                      {signedMoney(projectedPnl)}
                    </span>
                  </CardTitle>
                  <CardDescription>Accepted hedged projection across selected scan window</CardDescription>
                </CardHeader>
                <CardContent>
                  <SeriesChart
                    values={data.scans.map((row) => numeric(row, 'cumulative_pnl_usd'))}
                    tone={projectedPnl >= 0 ? 'good' : 'bad'}
                  />
                </CardContent>
              </Card>
              <div className="grid gap-4">
                <Card className="rounded-lg">
                  <CardHeader>
                    <CardTitle className="flex items-center justify-between gap-3">
                      <span>Open mark PnL</span>
                      <span className={paperPnl >= 0 ? 'text-emerald-700' : 'text-red-700'}>
                        {paperStats.ok ? signedMoney(paperPnl) : '-'}
                      </span>
                    </CardTitle>
                  <CardDescription>
                    pm-trader account marks, {paperPnlSeries.length || 1} UI samples
                  </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <SeriesChart
                      values={paperPnlSeries.length ? paperPnlSeries : [paperPnl]}
                      tone={paperPnl >= 0 ? 'good' : 'bad'}
                    />
                  </CardContent>
                </Card>
                <Card className="rounded-lg">
                  <CardHeader>
                    <CardTitle>Scan health</CardTitle>
                    <CardDescription>Latest throughput and quote state</CardDescription>
                  </CardHeader>
                  <CardContent className="grid gap-3 text-sm">
                    {[
                      ['Raw candidates', 'raw_yes_candidates', 'raw_no_candidates', 'raw_bundle_candidates'],
                      ['Target rejects', 'target_projection_rejections', 'target_size_rejections'],
                      ['Quote misses', 'quote_no_ask_tokens', 'quote_missing_book_tokens'],
                    ].map(([label, ...keys]) => {
                      const count = keys.reduce((sum, key) => sum + numeric(latest, key), 0)
                      return (
                        <div key={label}>
                          <div className="mb-1 flex justify-between">
                            <span>{label}</span>
                            <span className="font-mono">{count}</span>
                          </div>
                          <div className="h-2 rounded-sm bg-muted">
                            <div
                              className={label === 'Quote misses' && count ? 'h-2 rounded-sm bg-red-600' : 'h-2 rounded-sm bg-primary'}
                              style={{ width: `${Math.min(100, count * 4)}%` }}
                            />
                          </div>
                        </div>
                      )
                    })}
                  </CardContent>
                </Card>
              </div>
            </section>

            <section id="activity" className="mt-4 grid scroll-mt-4 gap-4 2xl:grid-cols-[1fr_420px]">
              <Card className="rounded-lg">
                <CardHeader className="gap-3">
                  <div>
                    <CardTitle>Activity</CardTitle>
                    <CardDescription>Click a row to inspect scanner evidence</CardDescription>
                  </div>
                  <Tabs value={tableView} onValueChange={changeTableView}>
                    <TabsList>
                      <TabsTrigger value="trades">Trades</TabsTrigger>
                      <TabsTrigger value="decisions">Decisions</TabsTrigger>
                      <TabsTrigger value="rejections">Rejections</TabsTrigger>
                      <TabsTrigger value="sources">Sources</TabsTrigger>
                    </TabsList>
                  </Tabs>
                </CardHeader>
                <CardContent>
                  <DataTable
                    title={tableConfig.title}
                    description={tableConfig.description}
                    rows={tableConfig.rows}
                    columns={tableConfig.columns}
                    selectedKey={selectedRow ? rowKey(selectedRow) : ''}
                    onSelect={setSelectedRow}
                  />
                </CardContent>
              </Card>
              <DetailPanel row={detailRow} onClose={selectedRow ? () => setSelectedRow(null) : undefined} />
            </section>
          </main>
        </div>
      </div>
    </TooltipProvider>
  )
}

function DataTable({
  title,
  description,
  rows,
  columns,
  selectedKey,
  onSelect,
}: {
  title: string
  description: string
  rows: CsvRow[]
  columns: [string, string][]
  selectedKey?: string
  onSelect?: (row: CsvRow) => void
}) {
  const mobileTitleKey = columns.find(([key]) => key.includes('title'))?.[0] ?? columns[1]?.[0] ?? columns[0]?.[0]
  const mobileValueKey = columns.at(-1)?.[0] ?? columns[0]?.[0]

  return (
    <div>
      <div className="mb-3">
        <div className="font-medium">{title}</div>
        <div className="text-sm text-muted-foreground">{description}</div>
      </div>
      <div className="grid gap-2 md:hidden">
        {rows.length === 0 ? (
          <div className="rounded-md border p-3 text-sm text-muted-foreground">No diagnostics yet.</div>
        ) : (
          rows.map((row, index) => {
            const key = rowKey(row)
            const status = row.status || row.stage || row.selection_state || row.mode || 'recorded'
            return (
              <button
                key={`${key}-mobile-${index}`}
                type="button"
                aria-selected={selectedKey === key}
                onClick={() => onSelect?.(row)}
                className="grid gap-2 rounded-md border p-3 text-left text-sm aria-selected:bg-muted"
              >
                <span className="flex items-center justify-between gap-3">
                  <Badge variant={statusVariant(status)}>{status}</Badge>
                  <span className="font-mono">{textCell(row, mobileValueKey)}</span>
                </span>
                <span className="line-clamp-2 font-medium">{textCell(row, mobileTitleKey)}</span>
              </button>
            )
          })
        )}
      </div>
      <div className="hidden md:block">
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map(([, label]) => (
                <TableHead key={label}>{label}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={columns.length} className="text-muted-foreground">
                  No diagnostics yet.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row, index) => {
                const key = rowKey(row)
                return (
                  <TableRow
                    key={`${key}-${index}`}
                    role="button"
                    tabIndex={0}
                    aria-selected={selectedKey === key}
                    onClick={() => onSelect?.(row)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' || event.key === ' ') onSelect?.(row)
                    }}
                    className="cursor-pointer aria-selected:bg-muted"
                  >
                    {columns.map(([key]) => (
                      <TableCell key={key} className={key.includes('title') || key === 'reason' ? 'max-w-[420px] truncate' : ''}>
                        {key === 'status' || key === 'selection_state' || key === 'stage' ? (
                          <Badge variant={statusVariant(row[key] || row.stage || '')}>
                            {row[key] || row.stage || 'recorded'}
                          </Badge>
                        ) : (
                          textCell(row, key)
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      </div>
      <Separator className="mt-3" />
      <div className="mt-2 text-xs text-muted-foreground">{rows.length} rows shown</div>
    </div>
  )
}

function DetailPanel({ row, onClose }: { row: CsvRow | null; onClose?: () => void }) {
  const title = row?.event_title || row?.source || 'No row selected'
  const status = row ? row.status || row.stage || row.selection_state || row.mode || 'recorded' : 'idle'
  const rows = row
    ? [
        ['Source', row.source || sourceOf(row)],
        ['Scan', row.scan_id],
        ['Pool', row.pool],
        ['Mode', row.mode],
        ['Type', row.arb_type || row.outcome_side],
        ['Projected PnL', row.projected_net_profit ? money(numeric(row, 'projected_net_profit')) : ''],
        ['Projected ROI', row.projected_roi_pct ? pct(numeric(row, 'projected_roi_pct')) : ''],
        ['Conservative PnL', row.conservative_pnl_usd ? money(numeric(row, 'conservative_pnl_usd')) : ''],
        ['Filled cost', row.filled_cost_usd ? money(numeric(row, 'filled_cost_usd')) : ''],
        ['Fill count', row.fill_count],
        ['Parity ok', row.parity_ok],
        ['Unhedged', row.unhedged_notional_usd ? money(numeric(row, 'unhedged_notional_usd')) : ''],
        ['Rows loaded', row.rows],
        ['Submissions', row.submissions],
      ].filter(([, value]) => value)
    : []

  return (
    <Card className="rounded-lg">
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle className="flex items-center gap-2">
              <ChevronRight data-icon="inline-start" />
              Detail
            </CardTitle>
            <CardDescription>{short(title, 96)}</CardDescription>
          </div>
          {onClose ? (
            <Button size="icon-sm" variant="ghost" aria-label="Close detail" onClick={onClose}>
              <X />
            </Button>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="grid gap-4">
        <Badge variant={statusVariant(status)} className="w-fit">
          {status}
        </Badge>
        {row ? (
          <>
            <div className="grid gap-2 text-sm">
              {rows.map(([label, value]) => (
                <div key={label} className="grid grid-cols-[120px_1fr] gap-3">
                  <span className="text-muted-foreground">{label}</span>
                  <span className="min-w-0 truncate font-mono">{value}</span>
                </div>
              ))}
            </div>
            {(row.reason || row.note) && (
              <div>
                <div className="mb-1 text-sm font-medium">{row.reason ? 'Reason' : 'Note'}</div>
                <div className="rounded-md border bg-muted/30 p-3 text-sm text-muted-foreground">
                  {row.reason || row.note}
                </div>
              </div>
            )}
            {row.legs_summary && (
              <div>
                <div className="mb-1 text-sm font-medium">Legs</div>
                <div className="max-h-44 overflow-auto rounded-md border bg-muted/30 p-3 text-xs leading-5 text-muted-foreground">
                  {row.legs_summary}
                </div>
              </div>
            )}
          </>
        ) : (
          <div className="text-sm text-muted-foreground">Select any activity row.</div>
        )}
      </CardContent>
    </Card>
  )
}
