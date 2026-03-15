export type KpiFamily = 'prb' | 'bler' | 'rrc';
export type KpiGrain = 'entity-time' | 'site-time' | 'region-time';

export interface RowsResponse {
  count: number;
  rows: Array<Record<string, unknown>>;
}

export interface OperationResponse {
  operation: string;
  status: string;
  result: Record<string, unknown>;
}

export interface IngestionStatusResponse {
  status_counts: Array<Record<string, unknown>>;
  summary: Record<string, unknown>;
  latest_scan_at: string | null;
  recent_failures: Array<Record<string, unknown>>;
}

declare global {
  interface Window {
    __LTE_PM_API_BASE__?: string;
  }
}

const apiBase = window.__LTE_PM_API_BASE__ ?? 'http://localhost:8000/api/v1';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const isFormData = init?.body instanceof FormData;
  const response = await fetch(`${apiBase}${path}`, {
    headers: {
      ...(isFormData ? {} : { 'Content-Type': 'application/json' }),
      ...(init?.headers ?? {})
    },
    ...init
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with status ${response.status}`);
  }

  return (await response.json()) as T;
}

function buildQuery(params: Record<string, string | number | undefined | null>): string {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      query.set(key, String(value));
    }
  });
  const encoded = query.toString();
  return encoded ? `?${encoded}` : '';
}

export const api = {
  getHealth: () => request<{ service: string; status: string }>('/health'),
  getReady: () => request<{ service: string; status: string; database: string }>('/ready'),
  getIngestionStatus: (limitRecentFailures = 10) =>
    request<IngestionStatusResponse>(`/ingestion/status${buildQuery({ limit_recent_failures: limitRecentFailures })}`),
  getIngestionFailures: (limit = 20) => request<RowsResponse>(`/ingestion/failures${buildQuery({ limit })}`),
  getFailureDetail: (id: number) => request<{ row: Record<string, unknown> | null }>(`/ingestion/failures/${id}`),
  getReconciliationPreview: (limit = 20) =>
    request<RowsResponse>(`/ingestion/reconciliation-preview${buildQuery({ limit })}`),
  getUnmappedEntities: (limit = 20) => request<RowsResponse>(`/topology/unmapped-entities${buildQuery({ limit })}`),
  getSiteCoverage: (limit = 20) => request<RowsResponse>(`/topology/site-coverage${buildQuery({ limit })}`),
  getRegionCoverage: (limit = 20) => request<RowsResponse>(`/topology/region-coverage${buildQuery({ limit })}`),
  uploadTopologyWorkbook: (file: File) => {
    const form = new FormData();
    form.append('file', file);
    return request<{ snapshot: Record<string, unknown> }>('/topology/workbook-preview', {
      method: 'POST',
      body: form
    });
  },
  getTopologySnapshots: () => request<RowsResponse>('/topology/snapshots'),
  getTopologySnapshot: (snapshotId: number) => request<{ snapshot: Record<string, unknown> }>(`/topology/snapshots/${snapshotId}`),
  getActiveTopologySnapshot: () => request<{ snapshot: Record<string, unknown> }>('/topology/active-snapshot'),
  reconcileTopologySnapshot: (snapshotId: number) =>
    request<{ snapshot: Record<string, unknown> }>(`/topology/snapshots/${snapshotId}/reconcile`, { method: 'POST' }),
  getTopologyReconciliationDetails: (reconciliationId: number, params: Record<string, string | number | undefined | null>) =>
    request<RowsResponse>(`/topology/reconciliations/${reconciliationId}/details${buildQuery(params)}`),
  getTopologyDrift: (snapshotId: number) => request<{ snapshot: Record<string, unknown> }>(`/topology/snapshots/${snapshotId}/drift`),
  applyTopologySnapshot: (snapshotId: number) =>
    request<OperationResponse>(`/topology/snapshots/${snapshotId}/apply`, { method: 'POST' }),
  syncTopologyViaTopologyApi: () => request<OperationResponse>('/topology/sync', { method: 'POST' }),
  getKpiResults: (grain: KpiGrain, params: Record<string, string | number | undefined | null>) =>
    request<RowsResponse>(`/kpi-results/${grain}${buildQuery(params)}`),
  getKpiValidation: (grain: KpiGrain, params: Record<string, string | number | undefined | null>) =>
    request<RowsResponse>(`/kpi-validation/${grain}${buildQuery(params)}`),
  runFtpCycle: (payload: Record<string, unknown>) =>
    request<OperationResponse>('/operations/ftp-run-cycle', { method: 'POST', body: JSON.stringify(payload) }),
  retryDownload: (ids: number[]) =>
    request<OperationResponse>('/operations/ftp-retry-download', { method: 'POST', body: JSON.stringify({ ids }) }),
  retryIngest: (ids: number[]) =>
    request<OperationResponse>('/operations/ftp-retry-ingest', { method: 'POST', body: JSON.stringify({ ids }) }),
  syncEntities: () => request<OperationResponse>('/operations/sync-entities', { method: 'POST', body: '{}' }),
  syncTopology: () => request<OperationResponse>('/operations/sync-topology', { method: 'POST', body: '{}' })
};
