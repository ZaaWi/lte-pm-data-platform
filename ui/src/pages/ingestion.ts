import { api } from '../api/client';
import { renderKeyValue, renderTable, setMessage } from '../components/render';
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
        <section class="panel"><h3>Failures</h3><div id="failures-table"></div></section>
        <section class="panel"><h3>Reconciliation preview</h3><div id="reconciliation-table"></div></section>
      </div>
    `;

    const summaryEl = container.querySelector<HTMLElement>('#status-summary');
    const failuresEl = container.querySelector<HTMLElement>('#failures-table');
    const reconciliationEl = container.querySelector<HTMLElement>('#reconciliation-table');
    const ftpCycleResult = container.querySelector<HTMLElement>('#ftp-cycle-result');
    const retryDownloadResult = container.querySelector<HTMLElement>('#retry-download-result');
    const retryIngestResult = container.querySelector<HTMLElement>('#retry-ingest-result');
    if (!summaryEl || !failuresEl || !reconciliationEl || !ftpCycleResult || !retryDownloadResult || !retryIngestResult) return;

    const load = async () => {
      try {
        const [status, failures, reconciliation] = await Promise.all([
          api.getIngestionStatus(10),
          api.getIngestionFailures(20),
          api.getReconciliationPreview(20)
        ]);
        summaryEl.innerHTML = renderKeyValue(status.summary);
        failuresEl.innerHTML = renderTable(failures.rows);
        reconciliationEl.innerHTML = renderTable(reconciliation.rows);
      } catch (error) {
        setMessage(container, `Ingestion page load failed: ${String(error)}`, 'error');
      }
    };

    const cycleForm = container.querySelector<HTMLFormElement>('#ftp-cycle-form');
    const retryDownloadForm = container.querySelector<HTMLFormElement>('#retry-download-form');
    const retryIngestForm = container.querySelector<HTMLFormElement>('#retry-ingest-form');

    cycleForm?.addEventListener('submit', async (event) => {
      event.preventDefault();
      setMessage(ftpCycleResult, 'Running FTP cycle…');
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
        ftpCycleResult.innerHTML = renderKeyValue(response.result);
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

    await load();
  }
};
