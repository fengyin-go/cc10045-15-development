import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 迟到数据恢复：告警再开启待办、交班领取、调查采样查看
// ---------------------------------------------------------------------------

export type ReopenRecord = {
  id: number
  alert_id: number
  alert_rule_name: string
  metric_name: string
  interval_key: string
  metric_value_id: number
  metric_value: number
  sample_time: string
  status: string
  claimed_by: string | null
  claimed_at: string | null
  analysis_id: number | null
  closure_evidence: {
    analysis_id?: number
    conclusion?: string
    evidence_sample_ids?: number[]
    closed_at?: string
  }
}

export type MetricSample = {
  id: number
  metric_id: number
  snapshot_value: number
  snapshot_time: string
  comparison_status: string | null
}

export type InvestigationSamples = {
  entry_id: number
  status: string
  interval_key: string | null
  reopen_id: number | null
  claimed_by: string | null
  evidence_sample_ids: number[]
  samples: MetricSample[]
  predecessor: {
    entry_id: number
    status: string
    conclusion: string | null
    evidence_sample_ids: number[]
    samples: MetricSample[]
    closure: Record<string, unknown>
  } | null
}

export async function listReopens(status?: string): Promise<{ items: ReopenRecord[]; total: number }> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const response = await request(`/api/pipeline/alerts/reopens${query}`)
  if (!response.ok) throw new Error('告警再开启待办读取失败')
  return await response.json()
}

export async function claimReopen(
  reopenId: number,
  claimedBy: string,
): Promise<{ ok: boolean; message: string; investigation: EntryRow | null }> {
  const response = await request(`/api/pipeline/anomaly/reopens/${reopenId}/claim`, {
    method: 'POST',
    body: JSON.stringify({ claimed_by: claimedBy }),
  })
  return await response.json()
}

export async function loadInvestigationSamples(entryId: number): Promise<InvestigationSamples> {
  const response = await request(`/api/pipeline/anomaly/${entryId}/samples`)
  if (!response.ok) throw new Error('调查采样读取失败')
  return await response.json()
}
