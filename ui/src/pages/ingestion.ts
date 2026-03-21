import { api } from '../api/client';
import { escapeHtml, renderKeyValue, renderTable, setMessage } from '../components/render';
import type { PageModule } from '../router';

function parseIds(raw: string): number[] {
  return raw
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0);
}

export const page: PageModule = {
  title: 'Ingestion',
  subtitle: 'Manual FTP operations and registry-backed ingestion visibility.',
  async render(container) {
    container.innerHTML = `
      <div class="grid cols-2">
        <section class="panel">
          <h3>Status summary</h3>
          <div id="status-summary"></div>
        </section>
        <section class="panel">
          <h3>Run FTP cycle</h3>
          <form id="ftp-cycle-form" class="grid">
            <div class="form-grid">
              <label><div class="small">Limit</div><input name="limit" type="number" value="20" min="1" /></label>
              <label><div class="small">Start</div><input name="start" type="date" /></label>
              <label><div class="small">End</div><input name="end" type="date" /></label>
              <label><div class="small">Revision policy</div>
                <select name="revision_policy">
                  <option value="additive">additive</option>
                  <option value="base-only">base-only</option>
                  <option value="revisions-only">revisions-only</option>
                  <option value="latest-only">latest-only</option>
                </select>
              </label>
              <label><div class="small">Families (comma separated)</div><input name="families" placeholder="PM/itbbu/ltefdd" /></label>
            </div>
            <div class="actions">
              <label><input name="dry_run" type="checkbox" /> dry run</label>
              <label><input name="retry_failed" type="checkbox" /> retry failed</label>
              <button class="primary" type="submit">Run FTP cycle</button>
            </div>
          </form>
          <div id="ftp-cycle-result"></div>
        </section>
      </div>
      <div class="grid cols-1">
        <section class="panel">
          <h3>Source intervals</h3>
          <div class="small">Discovered 15-minute source intervals from the FTP registry.</div>
          <div id="source-intervals-table"></div>
        </section>
      </div>
      <div class="grid cols-2">
        <section class="panel">
          <h3>Retry failed download</h3>
          <form id="retry-download-form" class="grid">
            <label><div class="small">Registry ids (comma separated)</div><input name="ids" placeholder="101,102" /></label>
            <div class="actions"><button type="submit">Retry download</button></div>
          </form>
          <div id="retry-download-result"></div>
        </section>
        <section class="panel">
          <h3>Retry failed ingest</h3>
          <form id="retry-ingest-form" class="grid">
            <label><div class="small">Registry ids (comma separated)</div><input name="ids" placeholder="205,206" /></label>
            <div class="actions"><button type="submit">Retry ingest</button></div>
          </form>
          <div id="retry-ingest-result"></div>
        </section>
      </div>
      <div class="grid cols-2">
        <section class="panel">
          <h3>FTP Runs</h3>
          <div class="small">Running runs</div>
          <div id="ftp-runs-running"></div>
          <div class="small" style="margin-top:0.75rem;">Recent runs</div>
          <div id="ftp-runs-recent"></div>
        </section>
        <section class="panel">
          <h3>Latest run events</h3>
          <div id="ftp-run-events"></div>
        </section>
      </div>
      <div class="grid cols-2">
        <section class="panel"><h3>Failures</h3><div id="failures-table"></div></section>
        <section class="panel"><h3>Reconciliation preview</h3><div id="reconciliation-table"></div></section>
      </div>
    `;

    const summaryEl = container.querySelector<HTMLElement>('#status-summary');
    const failuresEl = container.querySelector<HTMLElement>('#failures-table');
    const reconciliationEl = container.querySelector<HTMLElement>('#reconciliation-table');
    const sourceIntervalsEl = container.querySelector<HTMLElement>('#source-intervals-table');
    const ftpCycleResult = container.querySelector<HTMLElement>('#ftp-cycle-result');
    const retryDownloadResult = container.querySelector<HTMLElement>('#retry-download-result');
    const retryIngestResult = container.querySelector<HTMLElement>('#retry-ingest-result');
    const ftpRunsRunning = container.querySelector<HTMLElement>('#ftp-runs-running');
    const ftpRunsRecent = container.querySelector<HTMLElement>('#ftp-runs-recent');
    const ftpRunEvents = container.querySelector<HTMLElement>('#ftp-run-events');
    if (!summaryEl || !failuresEl || !reconciliationEl || !sourceIntervalsEl || !ftpCycleResult || !retryDownloadResult || !retryIngestResult || !ftpRunsRunning || !ftpRunsRecent || !ftpRunEvents) return;

    const load = async () => {
      try {
        const [status, sourceIntervals, failures, reconciliation, runningRuns, recentRuns] = await Promise.all([
          api.getIngestionStatus(10),
          api.getSourceIntervals(50),
          api.getIngestionFailures(20),
          api.getReconciliationPreview(20),
          api.getFtpRuns(10, 'running'),
          api.getFtpRuns(20)
        ]);
        summaryEl.innerHTML = renderKeyValue(status.summary);
        sourceIntervalsEl.innerHTML = renderSourceIntervalsTable(sourceIntervals.rows);
        failuresEl.innerHTML = renderTable(failures.rows);
        reconciliationEl.innerHTML = renderTable(reconciliation.rows);
        ftpRunsRunning.innerHTML = renderTable(
          runningRuns.rows.map(normalizeRunRow)
        );
        ftpRunsRecent.innerHTML = renderTable(
          recentRuns.rows.map(normalizeRunRow)
        );
        const latestRun = recentRuns.rows[0];
        if (latestRun && typeof latestRun.id === 'number') {
          const events = await api.getFtpRunEvents(latestRun.id, 20);
          ftpRunEvents.innerHTML = renderTable(events.rows.map((row) => ({ ...row, metrics_json: formatMetrics(row.metrics_json) })));
        } else {
          ftpRunEvents.innerHTML = '<div class="status">No runs yet.</div>';
        }
      } catch (error) {
        setMessage(container, `Ingestion page load failed: ${String(error)}`, 'error');
      }
    };

    const cycleForm = container.querySelector<HTMLFormElement>('#ftp-cycle-form');
    const retryDownloadForm = container.querySelector<HTMLFormElement>('#retry-download-form');
    const retryIngestForm = container.querySelector<HTMLFormElement>('#retry-ingest-form');
    const revisionPolicySelect = cycleForm?.querySelector<HTMLSelectElement>('select[name="revision_policy"]') ?? null;

    if (revisionPolicySelect) {
      revisionPolicySelect.addEventListener('change', () => {
        revisionPolicySelect.dataset.userSelected = 'true';
      });
    }

    cycleForm?.addEventListener('submit', async (event) => {
      event.preventDefault();
      setMessage(ftpCycleResult, 'Queueing FTP cycle…');
      const formData = new FormData(cycleForm);
      const familiesRaw = String(formData.get('families') ?? '').trim();
      try {
        const response = await api.runFtpCycle({
          limit: Number(formData.get('limit') ?? 20),
          start: formData.get('start') || null,
          end: formData.get('end') || null,
          revision_policy: formData.get('revision_policy') || 'additive',
          families: familiesRaw ? familiesRaw.split(',').map((value) => value.trim()).filter(Boolean) : undefined,
          dry_run: formData.get('dry_run') === 'on',
          retry_failed: formData.get('retry_failed') === 'on'
        });
        ftpCycleResult.innerHTML = renderKeyValue({
          run_id: response.run_id,
          status: response.status,
          requested_at: response.run.requested_at
        });
        await load();
      } catch (error) {
        setMessage(ftpCycleResult, `FTP cycle failed: ${String(error)}`, 'error');
      }
    });

    retryDownloadForm?.addEventListener('submit', async (event) => {
      event.preventDefault();
      const ids = parseIds(String(new FormData(retryDownloadForm).get('ids') ?? ''));
      setMessage(retryDownloadResult, 'Retrying download…');
      try {
        const response = await api.retryDownload(ids);
        retryDownloadResult.innerHTML = renderTable((response.result.results as Array<Record<string, unknown>>) ?? []);
        await load();
      } catch (error) {
        setMessage(retryDownloadResult, `Retry download failed: ${String(error)}`, 'error');
      }
    });

    retryIngestForm?.addEventListener('submit', async (event) => {
      event.preventDefault();
      const ids = parseIds(String(new FormData(retryIngestForm).get('ids') ?? ''));
      setMessage(retryIngestResult, 'Retrying ingest…');
      try {
        const response = await api.retryIngest(ids);
        retryIngestResult.innerHTML = renderTable((response.result.results as Array<Record<string, unknown>>) ?? []);
        await load();
      } catch (error) {
        setMessage(retryIngestResult, `Retry ingest failed: ${String(error)}`, 'error');
      }
    });

    sourceIntervalsEl.addEventListener('click', async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement) || target.dataset.action !== 'run-interval') {
        return;
      }
      const intervalStart = target.dataset.intervalStart;
      if (!intervalStart) {
        return;
      }
      setMessage(ftpCycleResult, `Queueing interval run for ${intervalStart}…`);
      const formData = cycleForm ? new FormData(cycleForm) : null;
      const familiesRaw = String(formData?.get('families') ?? '').trim();
      const selectedRevisionPolicy = revisionPolicySelect?.value;
      const revisionPolicy =
        revisionPolicySelect?.dataset.userSelected === 'true' && selectedRevisionPolicy
          ? selectedRevisionPolicy
          : 'latest-only';
      try {
        const response = await api.runFtpCycle({
          interval_start: intervalStart,
          limit: Number(formData?.get('limit') ?? 20),
          revision_policy: revisionPolicy,
          families: familiesRaw ? familiesRaw.split(',').map((value) => value.trim()).filter(Boolean) : undefined,
          dry_run: formData?.get('dry_run') === 'on',
          retry_failed: formData?.get('retry_failed') === 'on'
        });
        ftpCycleResult.innerHTML = renderKeyValue({
          run_id: response.run_id,
          status: response.status,
          interval_start: intervalStart,
          requested_at: response.run.requested_at
        });
        await load();
      } catch (error) {
        setMessage(ftpCycleResult, `Interval run failed: ${String(error)}`, 'error');
      }
    });

    await load();
    const pollHandle = window.setInterval(() => {
      if (!container.isConnected || window.location.hash !== '#/ingestion') {
        window.clearInterval(pollHandle);
        return;
      }
      if (document.hidden) {
        return;
      }
      void load();
    }, 5000);
  }
};

function normalizeRunRow(row: Record<string, unknown>): Record<string, unknown> {
  const summary = (row.summary_json as Record<string, unknown> | undefined) ?? {};
  const parameters = (row.parameters_json as Record<string, unknown> | undefined) ?? {};
  return {
    id: row.id,
    status: row.status,
    interval_start: parameters.interval_start ?? '',
    requested_at: row.requested_at,
    started_at: row.started_at,
    finished_at: row.finished_at,
    scanned: summary.scanned ?? 0,
    downloaded: summary.downloaded ?? 0,
    ingested: summary.ingested ?? 0,
    failed: Number(summary.failed_downloads ?? 0) + Number(summary.failed_ingests ?? 0),
    trigger_source: row.trigger_source
  };
}

function formatMetrics(value: unknown): string {
  if (!value || typeof value !== 'object') return '';
  return JSON.stringify(value);
}

function renderSourceIntervalsTable(rows: Array<Record<string, unknown>>): string {
  if (!rows.length) {
    return '<div class="status">No discovered intervals yet.</div>';
  }
  const header = `
    <tr>
      <th>interval_start</th>
      <th>total_files</th>
      <th>families</th>
      <th>missing</th>
      <th>quality</th>
      <th>topology</th>
      <th>notes</th>
      <th>statuses</th>
      <th>max_revision</th>
      <th>last_seen_at</th>
      <th>last_scan_at</th>
      <th>action</th>
    </tr>
  `;
  const body = rows
    .map((row) => {
      const intervalStart = String(row.interval_start ?? '');
      const families = Array.isArray(row.families_present) ? row.families_present.join(', ') : String(row.families_present ?? '');
      const missingFamilies = Array.isArray(row.missing_families) ? row.missing_families.join(', ') : String(row.missing_families ?? '');
      const statuses = Array.isArray(row.statuses_present) ? row.statuses_present.join(', ') : String(row.statuses_present ?? '');
      const topology = formatTopologyCoverage(row);
      return `
        <tr>
          <td>${escapeHtml(intervalStart)}</td>
          <td>${escapeHtml(row.total_files)}</td>
          <td>${escapeHtml(families)}</td>
          <td>${escapeHtml(missingFamilies)}</td>
          <td>${escapeHtml(row.quality_status)}</td>
          <td>${escapeHtml(topology)}</td>
          <td>${escapeHtml(row.quality_notes)}</td>
          <td>${escapeHtml(statuses)}</td>
          <td>${escapeHtml(row.max_revision)}</td>
          <td>${escapeHtml(row.last_seen_at)}</td>
          <td>${escapeHtml(row.last_scan_at)}</td>
          <td><button type="button" data-action="run-interval" data-interval-start="${escapeHtml(intervalStart)}">Run</button></td>
        </tr>
      `;
    })
    .join('');
  return `<div class="table-wrap"><table class="table"><thead>${header}</thead><tbody>${body}</tbody></table></div>`;
}

function formatTopologyCoverage(row: Record<string, unknown>): string {
  const mapped = Number(row.topology_mapped_count ?? 0);
  const unmapped = Number(row.topology_unmapped_count ?? 0);
  const pct = row.topology_coverage_pct;
  if (!Number.isFinite(mapped) || !Number.isFinite(unmapped)) {
    return 'n/a';
  }
  if (mapped === 0 && unmapped === 0) {
    return 'no topology rows';
  }
  if (pct === null || pct === undefined) {
    return `${mapped}/${unmapped} mapped/unmapped`;
  }
  return `${mapped}/${unmapped} mapped/unmapped (${pct}%)`;
}
