import { describe, expect, it } from 'vitest'
import {
  fuseLabelDetailParts,
  matchesFuseFilter,
  matchesTargetFilter,
  matchesWorkerFilter,
  panelValueFormatter,
  resolveInstanceComponent,
  fuseFilterOptionLabel,
  fuseSeriesLabel,
  matchesTransferFilter,
  computeSeriesSummaryFromPoints,
  resolveSeriesSummary,
  seriesHasFinitePoints,
  seriesDisplayName,
  shouldShowDashboardPanel,
  stepForRange,
  uniqueSeriesChartNames,
  transferFilterOptionLabel,
  transferLabelDetailParts,
  transferSeriesLabel,
  workerTransferLabelDetailParts,
  workerFilterOptionLabel,
  workerSeriesLabel
} from './metrics'

describe('stepForRange', () => {
  it('matches backend defaults', () => {
    expect(stepForRange('15m')).toBe('15s')
    expect(stepForRange('1h')).toBe('1m')
    expect(stepForRange('6h')).toBe('5m')
    expect(stepForRange('24h')).toBe('15m')
    expect(stepForRange('3d')).toBe('1h')
  })
})

describe('seriesDisplayName', () => {
  it('labels operation panels with instance', () => {
    const name = seriesDisplayName(
      {
        name: 'operation_count',
        labels: {
          component: 'worker',
          operation: 'read',
          instance: 'worker-1:9001'
        }
      },
      'operation-rate'
    )
    expect(name).toBe('read · worker-1')
  })

  it('includes worker host for metric series panels', () => {
    const name = seriesDisplayName(
      {
        name: 'eviction_trigger_count',
        labels: {
          component: 'worker',
          metric: 'eviction_trigger_count',
          instance: 'worker-2:9001',
          worker_id: '2'
        }
      },
      'eviction-events-rate'
    )
    expect(name).toBe('Eviction triggers · worker-2')
  })

  it('uses cluster suffix for aggregated panels', () => {
    const name = seriesDisplayName(
      {
        name: 'read_bytes (cluster)',
        labels: { component: 'cluster', metric: 'read_bytes' }
      },
      'cluster-read-throughput'
    )
    expect(name).toBe('read_bytes (cluster)')
  })

  it('labels transfer metric panels with state detail and instance', () => {
    const name = seriesDisplayName(
      {
        name: 'transfer_completed_bytes_total',
        labels: {
          component: 'transfer',
          metric: 'transfer_completed_bytes_total',
          state: 'completed',
          instance: 'cv-transfer-0:9011'
        }
      },
      'transfer-completed-bytes-rate'
    )
    expect(name).toBe('Completed bytes · completed · cv-transfer-0:9011')
  })

  it('labels transfer worker in-flight panels with the worker id', () => {
    const name = seriesDisplayName(
      {
        name: 'transfer_worker_inflight_count',
        labels: {
          component: 'transfer',
          metric: 'transfer_worker_inflight_count',
          worker_id: '3',
          instance: 'cv-transfer-0:9011'
        }
      },
      'transfer-worker-inflight-count'
    )
    expect(name).toBe('3 · cv-transfer-0:9011')
  })

  it('labels transfer reject panels with the rejection class', () => {
    const name = seriesDisplayName(
      {
        name: 'transfer_worker_reject_total',
        labels: {
          component: 'transfer',
          metric: 'transfer_worker_reject_total',
          class: 'capacity',
          instance: 'cv-transfer-0:9011'
        }
      },
      'transfer-worker-reject-rate'
    )
    expect(name).toBe('Worker rejects · capacity · cv-transfer-0:9011')
  })
})

describe('uniqueSeriesChartNames', () => {
  it('disambiguates duplicate worker labels on metric panels', () => {
    const series = [
      {
        name: 'eviction_trigger_count (w1)',
        labels: {
          component: 'worker',
          metric: 'eviction_trigger_count',
          instance: '10.0.0.1:9001',
          worker_id: '1'
        }
      },
      {
        name: 'eviction_trigger_count (w2)',
        labels: {
          component: 'worker',
          metric: 'eviction_trigger_count',
          instance: '10.0.0.2:9001',
          worker_id: '2'
        }
      }
    ]
    const names = uniqueSeriesChartNames(series, 'eviction-events-rate')
    expect(new Set(names).size).toBe(2)
    expect(names[0]).toContain('Eviction triggers')
    expect(names[1]).toContain('Eviction triggers')
  })
})

describe('matchesWorkerFilter', () => {
  it('keeps master series when worker filter is set', () => {
    expect(matchesWorkerFilter('worker-1', 'master', 'active-master', {})).toBe(true)
  })

  it('matches worker by instance or id', () => {
    expect(
      matchesWorkerFilter('worker-1:9001', 'worker', 'worker-1:9001', {
        instance: 'worker-1:9001'
      })
    ).toBe(true)
    expect(
      matchesWorkerFilter('7', 'worker', 'worker-7:9001', { worker_id: '7' })
    ).toBe(true)
    expect(
      matchesWorkerFilter('worker-2:9001', 'worker', 'worker-1:9001', {
        instance: 'worker-1:9001'
      })
    ).toBe(false)
  })
})

describe('matchesTargetFilter', () => {
  it('hides worker groups when filtering master', () => {
    expect(matchesTargetFilter('master', '', 'worker', 'w1:9001')).toBe(false)
    expect(matchesTargetFilter('master', '', 'master', 'active-master')).toBe(true)
  })

  it('resolves instance filter using target rows', () => {
    const rows = [
      { component: 'worker', instance: 'w1:9001' },
      { component: 'master', instance: 'active-master' }
    ]
    expect(resolveInstanceComponent(rows, 'w1:9001')).toBe('worker')
    expect(
      matchesTargetFilter('instance', 'w1:9001', 'worker', 'w1:9001', 'worker')
    ).toBe(true)
  })
})

describe('workerSeriesLabel', () => {
  it('prefers shortened hostname over worker id', () => {
    expect(
      workerSeriesLabel({ instance: 'curvine-worker-1:9001', worker_id: '1' })
    ).toBe('curvine-worker-1')
  })

  it('formats dropdown labels', () => {
    expect(workerFilterOptionLabel('curvine-worker-1:9001', 1)).toBe(
      'curvine-worker-1 (1)'
    )
  })
})

describe('fuseSeriesLabel', () => {
  it('keeps host and port for fuse legends', () => {
    expect(fuseSeriesLabel({ instance: '10.209.9.229:9012' })).toBe('10.209.9.229:9012')
    expect(fuseFilterOptionLabel('10.209.9.229:9002')).toBe('10.209.9.229:9002')
  })

  it('labels fuse request panels with port', () => {
    const name = seriesDisplayName(
      {
        name: 'curvine_fuse_requests_total',
        labels: {
          component: 'fuse',
          opcode: 'Read',
          kind: 'stream',
          instance: '10.209.9.229:9012'
        }
      },
      'fuse-request-qps'
    )
    expect(name).toBe('Read · stream · 10.209.9.229:9012')
  })

  it('includes cache and reason labels for meta cache invalidations', () => {
    const series = [
      {
        name: 'invalidations_status',
        labels: {
          component: 'fuse',
          cache: 'status',
          reason: 'unlink',
          instance: '10.209.9.229:9002'
        }
      },
      {
        name: 'invalidations_list',
        labels: {
          component: 'fuse',
          cache: 'list',
          reason: 'unlink',
          instance: '10.209.9.229:9002'
        }
      }
    ]
    const names = uniqueSeriesChartNames(series, 'fuse-meta-cache-invalidations')
    expect(new Set(names).size).toBe(2)
    expect(names[0]).toContain('status')
    expect(names[0]).toContain('unlink')
    expect(names[1]).toContain('list')
    expect(names[1]).toContain('unlink')
  })

  it('collects fuse label detail parts in stable order', () => {
    expect(
      fuseLabelDetailParts({
        reason: 'rename',
        cache: 'blocks',
        status: 'hit'
      })
    ).toEqual(['blocks', 'hit', 'rename'])
  })

  it('disambiguates fuse mounts on the same host', () => {
    const series = [
      {
        name: 'inode_num',
        labels: { component: 'fuse', metric: 'inode_num', instance: '10.209.9.229:9002' }
      },
      {
        name: 'inode_num',
        labels: { component: 'fuse', metric: 'inode_num', instance: '10.209.9.229:9012' }
      }
    ]
    const names = uniqueSeriesChartNames(series, 'fuse-runtime-handles')
    expect(new Set(names).size).toBe(2)
    expect(names[0]).toContain(':9002')
    expect(names[1]).toContain(':9012')
  })
})

describe('matchesFuseFilter', () => {
  it('filters fuse series by instance', () => {
    expect(
      matchesFuseFilter('10.209.9.229:9002', 'fuse', '10.209.9.229:9002', {
        instance: '10.209.9.229:9002'
      })
    ).toBe(true)
    expect(
      matchesFuseFilter('10.209.9.229:9002', 'fuse', '10.209.9.229:9012', {
        instance: '10.209.9.229:9012'
      })
    ).toBe(false)
    expect(matchesFuseFilter('10.209.9.229:9002', 'master', 'active-master', {})).toBe(true)
  })
})

describe('matchesTransferFilter', () => {
  it('filters transfer series by instance and ignores other components', () => {
    expect(
      matchesTransferFilter('cv-transfer-0:9011', 'transfer', 'cv-transfer-0:9011', {
        instance: 'cv-transfer-0:9011'
      })
    ).toBe(true)
    expect(
      matchesTransferFilter('cv-transfer-0:9011', 'transfer', 'cv-transfer-1:9011', {
        instance: 'cv-transfer-1:9011'
      })
    ).toBe(false)
    expect(
      matchesTransferFilter('cv-transfer-0:9011', 'master', 'active-master', {})
    ).toBe(true)
  })
})

describe('transferSeriesLabel', () => {
  it('keeps host and port for transfer legends', () => {
    expect(transferSeriesLabel({ instance: 'cv-transfer-0:9011' })).toBe('cv-transfer-0:9011')
    expect(transferFilterOptionLabel('cv-transfer-0:9011')).toBe('cv-transfer-0:9011')
  })

  it('collects transfer label detail parts in stable order', () => {
    expect(
      transferLabelDetailParts({
        reason: 'task_failed',
        state: 'failed',
        kind: 'copy',
        result: 'error'
      })
    ).toEqual(['copy', 'error', 'failed', 'task_failed'])
  })
})

describe('workerTransferSeriesLabel', () => {
  it('labels worker transfer terminal panels with state, kind and worker', () => {
    const name = seriesDisplayName(
      {
        name: 'worker_transfer_task_terminal_total',
        labels: {
          component: 'worker',
          metric: 'worker_transfer_task_terminal_total',
          state: 'completed',
          kind: 'load',
          instance: 'cv-worker-0:9002'
        }
      },
      'worker-transfer-terminal-rate'
    )
    expect(name).toBe('Terminal · completed · load · cv-worker-0')
  })

  it('collects worker transfer label detail parts in stable order', () => {
    expect(
      workerTransferLabelDetailParts({
        reason: 'timeout',
        state: 'failed',
        kind: 'load',
        result: 'rejected'
      })
    ).toEqual(['rejected', 'timeout', 'failed', 'load'])
  })
})

describe('matchesTargetFilter fuse', () => {
  it('respects fuse filter when target filter is fuse', () => {
    expect(
      matchesTargetFilter('fuse', '', 'fuse', '10.209.9.229:9002', undefined, '10.209.9.229:9002')
    ).toBe(true)
    expect(
      matchesTargetFilter('fuse', '', 'fuse', '10.209.9.229:9012', undefined, '10.209.9.229:9002')
    ).toBe(false)
  })
})

describe('panelValueFormatter', () => {
  it('formats fuse-io-size as bytes, not milliseconds', () => {
    const format = panelValueFormatter('fuse-io-size')
    expect(format(1024)).toBe('1.00 KB')
    expect(format(1024)).not.toContain('ms')
  })

  it('formats explicit FUSE latency panels as milliseconds', () => {
    const format = panelValueFormatter('fuse-request-p99')
    expect(format(12.5)).toBe('12.5 ms')
  })

  it('formats fuse-io-bytes-rate as bytes per second', () => {
    const format = panelValueFormatter('fuse-io-bytes-rate')
    expect(format(2048)).toBe('2.00 KB/s')
  })

  it('formats counter-rate panels without rate suffix as per second', () => {
    const format = panelValueFormatter('fuse-user-meta-cache')
    expect(format(1.4)).toBe('1.4/s')
    expect(panelValueFormatter('fuse-meta-cache-invalidations')(0)).toBe('0/s')
    expect(panelValueFormatter('fuse-decode-errors')(2)).toBe('2/s')
  })

  it('formats fuse-readdir-entries as a plain count', () => {
    const format = panelValueFormatter('fuse-readdir-entries')
    expect(format(128)).toMatch(/128/)
    expect(format(128)).not.toContain('ms')
    expect(format(128)).not.toContain('KB')
  })

  it('formats worker latency panels as microseconds', () => {
    const format = panelValueFormatter('worker-read-latency')
    expect(format(42)).toBe('42 us')
  })

  it('formats transfer latency panels as microseconds and rates per second', () => {
    expect(panelValueFormatter('transfer-store-latency-p99')(982)).toBe('982 us')
    expect(panelValueFormatter('transfer-completed-bytes-rate')(3.5)).toBe('3.5 B/s')
    expect(panelValueFormatter('transfer-report-rate')(1.4)).toBe('1.4/s')
    expect(panelValueFormatter('transfer-job-summary-flush-p99')(1200)).toBe('1,200 us')
    expect(panelValueFormatter('worker-transfer-execution-p99')(2500)).toBe('2,500 us')
    expect(panelValueFormatter('worker-transfer-copy-bytes-rate')(3.5)).toBe('3.5 B/s')
  })

  it('formats the transfer phase latency panels as microseconds, not milliseconds', () => {
    for (const panelId of [
      'worker-transfer-read-p99',
      'worker-transfer-write-p99',
      'worker-transfer-queue-p99'
    ]) {
      const format = panelValueFormatter(panelId)
      expect(format(2500)).toBe('2,500 us')
      expect(format(2500)).not.toContain('ms')
    }
  })

  it('formats the new transfer backpressure panels as rates', () => {
    expect(panelValueFormatter('transfer-report-envelope-rate')(1.4)).toBe('1.4/s')
    expect(panelValueFormatter('transfer-requeue-rate')(0)).toBe('0/s')
    expect(panelValueFormatter('transfer-worker-reject-rate')(2)).toBe('2/s')
  })
})

describe('shouldShowDashboardPanel', () => {
  const emptyPanel = { id: 'fuse-decode-errors', series: [], error: '' }
  const dataPanel = {
    id: 'fuse-request-qps',
    series: [{ points: [[1, 2]] }],
    error: ''
  }

  it('hides empty panels by default', () => {
    expect(shouldShowDashboardPanel(emptyPanel)).toBe(false)
    expect(shouldShowDashboardPanel(dataPanel)).toBe(true)
  })

  it('shows empty panels when the toggle is enabled', () => {
    expect(shouldShowDashboardPanel(emptyPanel, { showEmptyPanels: true })).toBe(true)
  })

  it('keeps key panels visible only while loading', () => {
    expect(shouldShowDashboardPanel(
      { id: 'fuse-request-qps', series: [], error: '' },
      { dashboardLoading: true }
    )).toBe(true)
    expect(shouldShowDashboardPanel(
      { id: 'fuse-decode-errors', series: [], error: '' },
      { dashboardLoading: true }
    )).toBe(false)
  })

  it('always shows panels with errors', () => {
    expect(shouldShowDashboardPanel(
      { id: 'fuse-decode-errors', series: [], error: 'query failed' }
    )).toBe(true)
  })

  it('hides panels whose points are all null', () => {
    const nullOnlyPanel = {
      id: 'fuse-request-qps',
      series: [{ points: [[1, null], [2, null]] }],
      error: ''
    }
    expect(shouldShowDashboardPanel(nullOnlyPanel)).toBe(false)
    expect(shouldShowDashboardPanel(nullOnlyPanel, { showEmptyPanels: true })).toBe(true)
  })
})

describe('series summary helpers', () => {
  it('detects finite points and ignores null placeholders', () => {
    expect(seriesHasFinitePoints([[1, null], [2, null]])).toBe(false)
    expect(seriesHasFinitePoints([[1, 0], [2, null]])).toBe(true)
  })

  it('computes summary from points when backend summary is empty', () => {
    const summary = resolveSeriesSummary(
      { current: null, min: null, max: null, avg: null },
      [[1, 10], [2, 20], [3, 30]]
    )
    expect(summary.current).toBe(30)
    expect(summary.min).toBe(10)
    expect(summary.max).toBe(30)
    expect(summary.avg).toBe(20)
  })

  it('prefers backend summary when present', () => {
    const summary = resolveSeriesSummary(
      { current: 5, min: 1, max: 9, avg: 4 },
      [[1, 10], [2, 20]]
    )
    expect(summary.current).toBe(5)
    expect(summary.avg).toBe(4)
  })
})
