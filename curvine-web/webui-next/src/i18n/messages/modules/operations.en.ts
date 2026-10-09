export default {
  operations: {
    summary: {
      coverage: 'Coverage',
      status: 'Status',
      checksPassed: 'Checks passed',
      needAttention: '{count} need attention',
      targetsCovered: 'Targets covered',
      masterWorker: '{master} master · {worker} worker',
      masterWorkerTransfer: '{master} master · {worker} worker · {transfer} transfer',
      activeAlerts: 'Active alerts',
      criticalWarning: '{critical} critical · {warning} warning',
      providerMode: 'Provider mode',
      discoveredTargets: '{count} discovered targets',
      providerPending: 'Provider pending'
    },
    status: {
      healthy: 'Healthy',
      critical: 'Critical',
      degraded: 'Degraded',
      pending: 'Pending',
      headlineFail: 'Immediate attention required',
      headlineWarning: 'Platform health is degraded',
      headlinePass: 'All systems operational',
      headlinePending: 'Waiting for operational data',
      descFail: '{critical} critical alert(s) and {actionable} diagnostic check(s) need attention.',
      descWarning: 'The cluster remains available, but one or more checks require investigation.',
      descPass: 'All {count} checks passed or were safely skipped.',
      descPending: 'Target discovery or provider evaluation has not completed yet.'
    },
    checks: {
      title: 'Diagnostic checks',
      subtitle: 'Provider readiness, discovery and scrape coverage validation.',
      passed: '{passed}/{total} passed',
      empty: 'No diagnostic checks returned.',
      howToFix: 'How to fix'
    },
    alerts: {
      title: 'Active alerts',
      subtitle: 'Actionable issues derived from diagnostics and missing targets.',
      active: '{count} active',
      allClear: 'All clear',
      allClearDesc: 'No active alerts. Provider and scrape targets look healthy.',
      recommendedAction: 'Recommended action',
      reviewChecks: 'Review diagnostic checks on the left.',
      suggestedFix: {
        builtin: 'Verify the target endpoint and metrics web port, then inspect Observability / Data Sources.',
        prometheus: 'Verify the target endpoint, component label and Prometheus monitor selector.',
        discovery: 'Verify master RPC connectivity and worker registration, then refresh operations.',
        prometheusUrl: 'Verify the Prometheus URL, network policy, monitor selectors, and /api/v1/targets access.'
      },
      targetTitle: {
        master: 'Curvine master metrics target unavailable',
        worker: 'Curvine worker metrics target unavailable',
        fuse: 'Curvine FUSE metrics target unavailable',
        transfer: 'Curvine Transfer metrics target unavailable'
      }
    },
    targets: {
      title: 'Unavailable targets',
      subtitle: 'Required endpoints without a healthy matching scrape.',
      missing: '{count} missing',
      source: 'Source',
      component: 'Component',
      instance: 'Instance',
      endpoint: 'Endpoint',
      reason: 'Reason',
      reasons: {
        'connection refused': 'connection refused',
        'request timed out': 'request timed out',
        'scrape failed': 'scrape failed',
        'target health is down': 'target health is down',
        'no matching scrape target': 'no matching scrape target',
        noHealthyScrape: '{provider} has no healthy matching scrape target'
      }
    },
    severity: {
      critical: 'critical',
      warning: 'warning'
    },
    checkStatus: {
      pass: 'pass',
      fail: 'fail',
      warning: 'warning',
      skipped: 'skipped'
    },
    sources: {
      diagnostics: 'diagnostics',
      builtin: 'builtin',
      prometheus: 'prometheus'
    },
    time: {
      notAvailable: 'not available'
    }
  }
}
