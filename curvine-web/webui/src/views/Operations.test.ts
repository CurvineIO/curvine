import { describe, expect, it } from 'vitest'
import Operations from './Operations.vue'
import {
  getOperationsScenario,
  OPERATIONS_SCENARIOS
} from './operations/scenarios'

type OperationsOptions = {
  name: string
  computed: Record<string, (this: Record<string, unknown>) => unknown>
  methods: Record<string, (...args: unknown[]) => unknown>
}

const component = Operations as unknown as OperationsOptions

describe('Platform Health page', () => {
  it('loads as the canonical platform health component', () => {
    expect(component.name).toBe('OperationsPage')
    expect(typeof component.methods.fetchData).toBe('function')
    expect(typeof component.computed.orderedChecks).toBe('function')
  })

  it('formats coverage for populated and pending discovery states', () => {
    expect(component.computed.coverageLabel.call({ expectedTargets: 4, coveragePercent: 75 })).toBe('75%')
    expect(component.computed.coverageLabel.call({ expectedTargets: 0, coveragePercent: 0 })).toBe('—')
  })

  it('defines a unique, renderable matrix for every operations state', () => {
    expect(OPERATIONS_SCENARIOS.length).toBe(13)
    expect(new Set(OPERATIONS_SCENARIOS.map((scenario) => scenario.id)).size).toBe(13)

    for (const scenario of OPERATIONS_SCENARIOS) {
      if (scenario.id === 'operations-api-error') continue
      const payload = getOperationsScenario(scenario.id)
      expect(payload.generated_ms).toBe(payload.diagnostics.generated_ms)
      expect(payload.diagnostics.checks.length).toBeGreaterThan(0)
      expect(new Set(payload.alerts.map((alert) => alert.id)).size).toBe(payload.alerts.length)
    }
  })

  it('shows target-level alerts without duplicate coverage summaries', () => {
    for (const id of ['builtin-master-down', 'builtin-worker-down', 'prometheus-targets-missing', 'prometheus-worker-down']) {
      const payload = getOperationsScenario(id)
      expect(payload.alerts.some((alert) => alert.target_id)).toBe(true)
      expect(payload.alerts.some((alert) => alert.id.includes('coverage'))).toBe(false)
    }
  })

  it('covers critical, warning, empty, pending, and request-error UI states', () => {
    expect(getOperationsScenario('healthy-builtin').alerts).toHaveLength(0)
    expect(getOperationsScenario('discovery-pending').diagnostics.expected_targets).toBe(0)
    expect(getOperationsScenario('builtin-master-down').alerts[0].severity).toBe('critical')
    expect(getOperationsScenario('builtin-worker-down').alerts[0].severity).toBe('warning')
    expect(getOperationsScenario('builtin-worker-down').diagnostics.overall_status).toBe('warning')
    expect(getOperationsScenario('prometheus-worker-down').diagnostics.overall_status).toBe('warning')
    expect(getOperationsScenario('auto-fallback').diagnostics.overall_status).toBe('warning')
    expect(() => getOperationsScenario('operations-api-error')).toThrow('HTTP 503')
  })
})
