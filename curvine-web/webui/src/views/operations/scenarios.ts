import type {
  DiagnosticCheck,
  DiagnosticStatus,
  MissingTarget,
  OperationsAlert,
  OperationsPayload
} from '@/api/client'

export interface OperationsScenarioDefinition {
  id: string
  label: string
  description: string
  expected: string
}

export const OPERATIONS_SCENARIOS: OperationsScenarioDefinition[] = [
  { id: 'healthy-builtin', label: 'Healthy · Builtin', description: 'Builtin collection, discovery, and all required targets are healthy.', expected: 'PASS, 100% coverage, no alerts' },
  { id: 'healthy-prometheus', label: 'Healthy · Prometheus', description: 'Prometheus is reachable and covers every discovered target.', expected: 'PASS, 100% coverage, no alerts' },
  { id: 'discovery-pending', label: 'Discovery pending', description: 'No master or worker has been discovered yet.', expected: 'FAIL with master discovery critical and worker warning' },
  { id: 'observability-disabled', label: 'Observability disabled', description: 'Observability is disabled while discovery remains available.', expected: 'FAIL with one configuration alert' },
  { id: 'builtin-master-down', label: 'Builtin master down', description: 'The active builtin provider cannot scrape the master target.', expected: 'FAIL with one critical target alert; no duplicate coverage alert' },
  { id: 'builtin-worker-down', label: 'Builtin worker down', description: 'One live worker is discovered but its builtin scrape is down.', expected: 'WARNING with one target alert; no duplicate coverage alert' },
  { id: 'prometheus-not-configured', label: 'Prometheus not configured', description: 'Prometheus mode is selected without a provider URL.', expected: 'FAIL with one critical configuration alert' },
  { id: 'prometheus-unreachable', label: 'Prometheus unreachable', description: 'The configured Prometheus health endpoint cannot be reached.', expected: 'FAIL with one critical reachability alert' },
  { id: 'prometheus-targets-missing', label: 'Prometheus targets missing', description: 'Prometheus is reachable but master and worker scrape targets are down.', expected: 'FAIL with target-level alerts and no duplicate coverage alert' },
  { id: 'prometheus-worker-down', label: 'Prometheus worker down', description: 'Prometheus covers the master but one required worker target is down.', expected: 'WARNING with one worker target alert and no duplicate coverage alert' },
  { id: 'prometheus-api-error', label: 'Prometheus target API error', description: 'Prometheus is reachable, but target coverage cannot be evaluated.', expected: 'FAIL with the coverage diagnostic alert retained' },
  { id: 'auto-fallback', label: 'Auto fallback', description: 'Builtin is active after Prometheus becomes unavailable.', expected: 'WARNING showing Prometheus degradation while builtin remains covered' },
  { id: 'operations-api-error', label: 'Platform health API error', description: 'The aggregate platform health request fails completely.', expected: 'Error banner with empty page data' }
]

const fix = {
  discovery: 'Verify master RPC connectivity and worker registration, then refresh operations.',
  builtin: 'Verify the target endpoint and metrics web port, then inspect Observability / Data Sources.',
  prometheus: 'Verify the Prometheus URL, network policy, monitor selectors, and /api/v1/targets access.'
}

function check(id: string, title: string, status: DiagnosticStatus, summary: string, suggested_fix = '', detail = ''): DiagnosticCheck {
  return { id, title, status, summary, suggested_fix, detail }
}

function missing(source: string, component: string, instance: string, reason: string): MissingTarget {
  return { id: `${component}:${instance}`, source, component, instance, url: `http://${instance}:9000/metrics`, reason }
}

function alert(
  id: string,
  severity: 'critical' | 'warning',
  title: string,
  summary: string,
  source = 'diagnostics',
  target_id?: string,
  suggested_fix = fix.discovery
): OperationsAlert {
  return { id, severity, title, summary, source, target_id, suggested_fix }
}

function payload(
  active_provider: string,
  overall_status: DiagnosticStatus,
  checks: DiagnosticCheck[],
  alerts: OperationsAlert[],
  options: {
    masters?: number
    workers?: number
    covered?: number
    expected?: number
    transfers?: number
    prometheusCovered?: number
    missing?: MissingTarget[]
  } = {}
): OperationsPayload {
  const generated_ms = 1_750_000_000_000
  return {
    generated_ms,
    diagnostics: {
      generated_ms,
      active_provider,
      overall_status,
      checks,
      discovered_master_targets: options.masters ?? 1,
      discovered_live_worker_targets: options.workers ?? 1,
      discovered_transfer_targets: options.transfers ?? 0,
      prometheus_covered_targets: options.prometheusCovered ?? 0,
      active_covered_targets: options.covered ?? 2,
      expected_targets: options.expected ?? 2,
      missing_targets: options.missing ?? []
    },
    alerts
  }
}

const commonDiscovery = [
  check('observability-enabled', 'Observability enabled', 'pass', 'Observability APIs and collection are enabled.'),
  check('master-targets', 'Curvine master targets discovered', 'pass', 'Discovered 1 master metrics target.'),
  check('worker-targets', 'Curvine worker targets discovered', 'pass', 'Discovered 1 live worker metrics target.')
]

const SCENARIO_PAYLOADS: Record<string, OperationsPayload | Error> = {
  'healthy-builtin': payload('builtin', 'pass', [
    ...commonDiscovery,
    check('builtin-coverage', 'Builtin scrape coverage', 'pass', 'Builtin collector covers 2 of 2 required targets.'),
    check('prometheus-reachable', 'Prometheus reachable', 'skipped', 'Prometheus URL is not configured.'),
    check('prometheus-coverage', 'Prometheus target coverage', 'skipped', 'Coverage is unavailable without a Prometheus URL.')
  ], []),
  'healthy-prometheus': payload('prometheus', 'pass', [
    ...commonDiscovery,
    check('builtin-coverage', 'Builtin scrape coverage', 'skipped', 'Builtin coverage is not authoritative because Prometheus is active.'),
    check('prometheus-reachable', 'Prometheus reachable', 'pass', 'Prometheus accepted the health request.'),
    check('prometheus-coverage', 'Prometheus target coverage', 'pass', 'Prometheus covers 2 of 2 required targets.')
  ], [], { prometheusCovered: 2 }),
  'discovery-pending': payload('builtin', 'fail', [
    check('observability-enabled', 'Observability enabled', 'pass', 'Observability APIs and collection are enabled.'),
    check('master-targets', 'Curvine master targets discovered', 'fail', 'Discovered 0 master metrics targets.', fix.discovery),
    check('worker-targets', 'Curvine worker targets discovered', 'warning', 'Discovered 0 live worker metrics targets.', fix.discovery),
    check('builtin-coverage', 'Builtin scrape coverage', 'skipped', 'Coverage is pending until target discovery completes.'),
    check('prometheus-coverage', 'Prometheus target coverage', 'skipped', 'Coverage is unavailable without a Prometheus URL.')
  ], [
    alert('diagnostic:master-targets', 'critical', 'Curvine master targets discovered', 'Discovered 0 master metrics targets.'),
    alert('diagnostic:worker-targets', 'warning', 'Curvine worker targets discovered', 'Discovered 0 live worker metrics targets.')
  ], { masters: 0, workers: 0, covered: 0, expected: 0 }),
  'observability-disabled': payload('builtin', 'fail', [
    check('observability-enabled', 'Observability enabled', 'fail', 'Observability is disabled by configuration.', 'Set web.observability.enabled=true and restart curvine-web.'),
    ...commonDiscovery.slice(1),
    check('builtin-coverage', 'Builtin scrape coverage', 'pass', 'Builtin collector covers 2 of 2 required targets.')
  ], [alert('diagnostic:observability-enabled', 'critical', 'Observability enabled', 'Observability is disabled by configuration.', 'diagnostics', undefined, 'Set web.observability.enabled=true and restart curvine-web.')]),
  'builtin-master-down': (() => {
    const target = missing('builtin-target', 'master', 'master-1', 'connection refused')
    return payload('builtin', 'fail', [
      ...commonDiscovery,
      check('builtin-coverage', 'Builtin scrape coverage', 'fail', 'Builtin collector covers 1 of 2 required targets.', fix.builtin)
    ], [alert('builtin-target:master:master-1', 'critical', 'master metrics target unavailable', 'master-1: connection refused', 'builtin-target', target.id, fix.builtin)], { covered: 1, missing: [target] })
  })(),
  'builtin-worker-down': (() => {
    const target = missing('builtin-target', 'worker', 'worker-1', 'request timed out')
    return payload('builtin', 'warning', [
      ...commonDiscovery,
      check('builtin-coverage', 'Builtin scrape coverage', 'warning', 'Builtin collector covers 1 of 2 required targets.', fix.builtin)
    ], [alert('builtin-target:worker:worker-1', 'warning', 'worker metrics target unavailable', 'worker-1: request timed out', 'builtin-target', target.id, fix.builtin)], { covered: 1, missing: [target] })
  })(),
  'prometheus-not-configured': payload('prometheus', 'fail', [
    ...commonDiscovery,
    check('builtin-coverage', 'Builtin scrape coverage', 'skipped', 'Builtin coverage is not authoritative because Prometheus is active.'),
    check('prometheus-reachable', 'Prometheus reachable', 'fail', 'Prometheus URL is not configured.', 'Set web.observability.prometheus.url or switch provider mode to builtin.'),
    check('prometheus-coverage', 'Prometheus target coverage', 'skipped', 'Coverage is unavailable without a Prometheus URL.')
  ], [alert('diagnostic:prometheus-reachable', 'critical', 'Prometheus reachable', 'Prometheus URL is not configured.', 'diagnostics', undefined, fix.prometheus)], { covered: 0 }),
  'prometheus-unreachable': payload('prometheus', 'fail', [
    ...commonDiscovery,
    check('builtin-coverage', 'Builtin scrape coverage', 'skipped', 'Builtin coverage is not authoritative because Prometheus is active.'),
    check('prometheus-reachable', 'Prometheus reachable', 'fail', 'Prometheus cannot be reached.', fix.prometheus, 'connection refused'),
    check('prometheus-coverage', 'Prometheus target coverage', 'skipped', 'Coverage was skipped because Prometheus is unreachable.')
  ], [alert('diagnostic:prometheus-reachable', 'critical', 'Prometheus reachable', 'Prometheus cannot be reached.', 'diagnostics', undefined, fix.prometheus)], { covered: 0 }),
  'prometheus-targets-missing': (() => {
    const master = missing('prometheus-target', 'master', 'master-1', 'target health is down')
    const worker = missing('prometheus-target', 'worker', 'worker-1', 'no matching scrape target')
    return payload('prometheus', 'fail', [
      ...commonDiscovery,
      check('prometheus-reachable', 'Prometheus reachable', 'pass', 'Prometheus accepted the health request.'),
      check('prometheus-coverage', 'Prometheus target coverage', 'fail', 'Prometheus covers 0 of 2 required targets.', fix.prometheus)
    ], [
      alert('prometheus-target:master:master-1', 'critical', 'master metrics target unavailable', 'master-1: target health is down', 'prometheus-target', master.id, fix.prometheus),
      alert('prometheus-target:worker:worker-1', 'warning', 'worker metrics target unavailable', 'worker-1: no matching scrape target', 'prometheus-target', worker.id, fix.prometheus)
    ], { covered: 0, missing: [master, worker] })
  })(),
  'prometheus-worker-down': (() => {
    const worker = missing('prometheus-target', 'worker', 'worker-1', 'target health is down')
    return payload('prometheus', 'warning', [
      ...commonDiscovery,
      check('prometheus-reachable', 'Prometheus reachable', 'pass', 'Prometheus accepted the health request.'),
      check('prometheus-coverage', 'Prometheus target coverage', 'warning', 'Prometheus covers 1 of 2 required targets.', fix.prometheus)
    ], [alert('prometheus-target:worker:worker-1', 'warning', 'worker metrics target unavailable', 'worker-1: target health is down', 'prometheus-target', worker.id, fix.prometheus)], { covered: 1, prometheusCovered: 1, missing: [worker] })
  })(),
  'prometheus-api-error': payload('prometheus', 'fail', [
    ...commonDiscovery,
    check('prometheus-reachable', 'Prometheus reachable', 'pass', 'Prometheus accepted the health request.'),
    check('prometheus-coverage', 'Prometheus target coverage', 'fail', 'Prometheus target coverage could not be evaluated.', fix.prometheus, 'HTTP 403 from /api/v1/targets')
  ], [alert('diagnostic:prometheus-coverage', 'critical', 'Prometheus target coverage', 'Prometheus target coverage could not be evaluated.', 'diagnostics', undefined, fix.prometheus)], { covered: 0 }),
  'auto-fallback': payload('builtin', 'warning', [
    ...commonDiscovery,
    check('builtin-coverage', 'Builtin scrape coverage', 'pass', 'Builtin collector covers 2 of 2 required targets.'),
    check('prometheus-reachable', 'Prometheus reachable', 'warning', 'Prometheus cannot be reached; builtin remains active.', fix.prometheus),
    check('prometheus-coverage', 'Prometheus target coverage', 'skipped', 'Coverage was skipped because Prometheus is unreachable.')
  ], [alert('diagnostic:prometheus-reachable', 'warning', 'Prometheus reachable', 'Prometheus cannot be reached; builtin remains active.', 'diagnostics', undefined, fix.prometheus)]),
  'operations-api-error': new Error('Simulated GET /v1/observability/operations failure (HTTP 503)')
}

export function getOperationsScenario(id: string): OperationsPayload {
  const value = SCENARIO_PAYLOADS[id] ?? SCENARIO_PAYLOADS['healthy-builtin']
  if (value instanceof Error) throw value
  const cloned = JSON.parse(JSON.stringify(value)) as OperationsPayload
  cloned.generated_ms = Date.now()
  cloned.diagnostics.generated_ms = cloned.generated_ms
  return cloned
}

export function getOperationsScenarioDefinition(id: string): OperationsScenarioDefinition {
  return OPERATIONS_SCENARIOS.find((scenario) => scenario.id === id) ?? OPERATIONS_SCENARIOS[0]
}
