import { api } from '../api/client';
import { escapeHtml, renderKeyValue, renderTable, setMessage } from '../components/render';
import type { PageModule } from '../router';

function renderSnapshotHistory(rows: Array<Record<string, unknown>>): string {
  if (!rows.length) {
    return '<div class="status">No snapshots uploaded yet.</div>';
  }
  const body = rows
    .map((row) => {
      const snapshotId = Number(row.snapshot_id);
      const canApply = Number(row.parser_error_count ?? 0) === 0 && Number(row.blocking_error_count ?? 0) === 0;
      return `
        <tr>
          <td>${escapeHtml(row.snapshot_id)}</td>
          <td>${escapeHtml(row.source_file_name)}</td>
          <td>${escapeHtml(row.topology_release_date)}</td>
          <td>${escapeHtml(row.status)}</td>
          <td>${escapeHtml(row.is_active_snapshot)}</td>
          <td>${escapeHtml(row.blocking_error_count)}</td>
          <td>${escapeHtml(row.warning_count)}</td>
          <td class="actions">
            <button type="button" data-action="summary" data-snapshot-id="${snapshotId}">Summary</button>
            <button type="button" data-action="reconcile" data-snapshot-id="${snapshotId}">Reconcile</button>
            <button type="button" data-action="drift" data-snapshot-id="${snapshotId}">Drift</button>
            <button type="button" data-action="apply" data-snapshot-id="${snapshotId}" ${canApply ? '' : 'disabled'}>Apply</button>
          </td>
        </tr>
      `;
    })
    .join('');

  return `
    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>snapshot_id</th>
            <th>source_file_name</th>
            <th>release_date</th>
            <th>status</th>
            <th>active</th>
            <th>blocking_errors</th>
            <th>warnings</th>
            <th>actions</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

export const page: PageModule = {
  title: 'Topology Management',
  subtitle: 'Upload workbook snapshots, reconcile drift, apply a candidate snapshot, and run sync-topology.',
  async render(container) {
    let selectedSnapshotId: number | null = null;
    let selectedReconciliationId: number | null = null;

    container.innerHTML = `
      <div class="grid cols-2">
        <section class="panel">
          <h3>Upload workbook</h3>
          <form id="topology-upload-form" class="grid">
            <label>
              <div class="small">Workbook file</div>
              <input id="topology-workbook-file" name="file" type="file" accept=".xlsx" />
            </label>
            <div class="actions">
              <button class="primary" type="submit">Preview</button>
            </div>
          </form>
          <div id="topology-upload-result"></div>
        </section>
        <section class="panel">
          <h3>Current active snapshot</h3>
          <div id="topology-active-snapshot"></div>
        </section>
      </div>

      <div class="grid cols-2">
        <section class="panel">
          <h3>Snapshot summary</h3>
          <div id="topology-snapshot-summary"></div>
        </section>
        <section class="panel">
          <h3>Reconciliation summary</h3>
          <div id="topology-reconciliation-summary"></div>
          <div class="actions">
            <button id="topology-sync-btn" type="button">Run sync-topology</button>
          </div>
          <div id="topology-action-result"></div>
        </section>
      </div>

      <section class="panel">
        <h3>Drift / reconciliation details</h3>
        <form id="topology-detail-form" class="form-grid">
          <label><div class="small">Issue type</div>
            <select name="issue_type">
              <option value="">all</option>
              <option value="DUPLICATE_ENTITY_MULTIPLE_SITES">duplicate entity multiple sites</option>
              <option value="CONFLICTING_SITE_REGION">conflicting site region</option>
              <option value="PM_MISSING_FROM_WORKBOOK">PM missing from workbook</option>
              <option value="WORKBOOK_MISSING_FROM_PM">workbook missing from PM</option>
              <option value="WORKBOOK_SITE_NO_PM_ACTIVITY">workbook site no PM</option>
              <option value="ENTITY_ADDED">entity added</option>
              <option value="ENTITY_REMOVED">entity removed</option>
              <option value="ENTITY_MOVED_SITE">entity moved site</option>
              <option value="SITE_MOVED_REGION">site moved region</option>
            </select>
          </label>
          <label class="compact-field"><div class="small">Rows</div><input name="limit" type="number" value="100" min="1" /></label>
          <div class="actions">
            <button id="topology-load-details-btn" type="submit">Load details</button>
          </div>
        </form>
        <div id="topology-reconciliation-details"></div>
      </section>

      <section class="panel">
        <h3>Snapshot history</h3>
        <div id="topology-snapshot-history"></div>
      </section>

      <div class="grid cols-2">
        <section class="panel">
          <h3>Unmapped entities</h3>
          <div id="unmapped-table"></div>
        </section>
        <section class="panel">
          <h3>Site coverage</h3>
          <div class="actions"><button id="topology-load-coverage-btn" type="button">Load detailed coverage</button></div>
          <div id="site-coverage-table"></div>
        </section>
      </div>
      <section class="panel">
        <h3>Region coverage</h3>
        <div id="region-coverage-table"></div>
      </section>
    `;

    const uploadForm = container.querySelector<HTMLFormElement>('#topology-upload-form');
    const uploadResult = container.querySelector<HTMLElement>('#topology-upload-result');
    const activeSnapshot = container.querySelector<HTMLElement>('#topology-active-snapshot');
    const snapshotSummary = container.querySelector<HTMLElement>('#topology-snapshot-summary');
    const reconciliationSummary = container.querySelector<HTMLElement>('#topology-reconciliation-summary');
    const snapshotHistory = container.querySelector<HTMLElement>('#topology-snapshot-history');
    const detailsForm = container.querySelector<HTMLFormElement>('#topology-detail-form');
    const details = container.querySelector<HTMLElement>('#topology-reconciliation-details');
    const actionResult = container.querySelector<HTMLElement>('#topology-action-result');
    const unmapped = container.querySelector<HTMLElement>('#unmapped-table');
    const siteCoverage = container.querySelector<HTMLElement>('#site-coverage-table');
    const regionCoverage = container.querySelector<HTMLElement>('#region-coverage-table');
    const syncTopologyBtn = container.querySelector<HTMLButtonElement>('#topology-sync-btn');
    const loadCoverageBtn = container.querySelector<HTMLButtonElement>('#topology-load-coverage-btn');
    if (!uploadForm || !uploadResult || !activeSnapshot || !snapshotSummary || !reconciliationSummary || !snapshotHistory || !detailsForm || !details || !actionResult || !unmapped || !siteCoverage || !regionCoverage || !syncTopologyBtn || !loadCoverageBtn) {
      return;
    }

    const loadCoverage = async () => {
      const [unmappedEntities, siteRows, regionRows] = await Promise.all([
        api.getUnmappedEntities(20),
        api.getSiteCoverage(20),
        api.getRegionCoverage(20)
      ]);
      unmapped.innerHTML = renderTable(unmappedEntities.rows);
      siteCoverage.innerHTML = renderTable(siteRows.rows);
      regionCoverage.innerHTML = renderTable(regionRows.rows);
    };

    const loadSnapshotIndex = async () => {
      const [active, snapshots] = await Promise.all([
        api.getActiveTopologySnapshot(),
        api.getTopologySnapshots()
      ]);
      activeSnapshot.innerHTML = Object.keys(active.snapshot ?? {}).length
        ? renderKeyValue(active.snapshot)
        : '<div class="status">No active snapshot.</div>';
      snapshotHistory.innerHTML = renderSnapshotHistory(snapshots.rows);
    };

    const loadSnapshotSummary = async (snapshotId: number) => {
      const response = await api.getTopologySnapshot(snapshotId);
      selectedSnapshotId = snapshotId;
      selectedReconciliationId = Number(response.snapshot.reconciliation_id ?? 0) || null;
      snapshotSummary.innerHTML = renderKeyValue(response.snapshot);
      reconciliationSummary.innerHTML = renderKeyValue({
        reconciliation_id: response.snapshot.reconciliation_id,
        parser_error_count: response.snapshot.parser_error_count,
        parser_warning_count: response.snapshot.parser_warning_count,
        blocking_error_count: response.snapshot.blocking_error_count,
        warning_count: response.snapshot.warning_count,
        pm_missing_from_workbook_count: response.snapshot.pm_missing_from_workbook_count,
        workbook_missing_from_pm_count: response.snapshot.workbook_missing_from_pm_count,
        workbook_sites_no_pm_count: response.snapshot.workbook_sites_no_pm_count,
        duplicate_entity_mapping_count: response.snapshot.duplicate_entity_mapping_count,
        conflicting_site_region_count: response.snapshot.conflicting_site_region_count,
        entities_added_count: response.snapshot.entities_added_count,
        entities_removed_count: response.snapshot.entities_removed_count,
        entities_moved_site_count: response.snapshot.entities_moved_site_count,
        sites_moved_region_count: response.snapshot.sites_moved_region_count
      });
    };

    const loadDetails = async () => {
      if (!selectedReconciliationId) {
        setMessage(details, 'Run reconciliation first.', 'neutral');
        return;
      }
      const formData = new FormData(detailsForm);
      const issueType = String(formData.get('issue_type') ?? '').trim();
      const limit = Number(formData.get('limit') ?? 100);
      const response = await api.getTopologyReconciliationDetails(selectedReconciliationId, {
        issue_type: issueType || undefined,
        limit
      });
      details.innerHTML = renderTable(response.rows);
    };

    const refresh = async () => {
      const unmappedEntities = await api.getUnmappedEntities(20);
      unmapped.innerHTML = renderTable(unmappedEntities.rows);
      siteCoverage.innerHTML = '<div class="status">Detailed site/region coverage is loaded on demand.</div>';
      regionCoverage.innerHTML = '<div class="status">Detailed site/region coverage is loaded on demand.</div>';
      await loadSnapshotIndex();
      if (selectedSnapshotId) {
        await loadSnapshotSummary(selectedSnapshotId);
      }
    };

    uploadForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      const fileInput = container.querySelector<HTMLInputElement>('#topology-workbook-file');
      const file = fileInput?.files?.[0];
      if (!file) {
        setMessage(uploadResult, 'Choose a workbook file first.', 'error');
        return;
      }
      setMessage(uploadResult, 'Uploading workbook and creating preview snapshot…');
      try {
        const response = await api.uploadTopologyWorkbook(file);
        selectedSnapshotId = Number(response.snapshot.snapshot_id);
        selectedReconciliationId = null;
        uploadResult.innerHTML = renderKeyValue({
          snapshot_id: response.snapshot.snapshot_id,
          source_file_name: response.snapshot.source_file_name,
          topology_release_date: response.snapshot.topology_release_date,
          parser_error_count: response.snapshot.parser_error_count,
          parser_warning_count: response.snapshot.parser_warning_count,
          normalized_row_count: response.snapshot.normalized_row_count
        });
        await refresh();
      } catch (error) {
        setMessage(uploadResult, `Workbook preview failed: ${String(error)}`, 'error');
      }
    });

    snapshotHistory.addEventListener('click', async (event) => {
      const target = event.target as HTMLElement;
      const button = target.closest<HTMLButtonElement>('button[data-snapshot-id]');
      if (!button) return;
      const snapshotId = Number(button.dataset.snapshotId);
      const action = button.dataset.action;
      try {
        if (action === 'summary' || action === 'drift') {
          await loadSnapshotSummary(snapshotId);
        } else if (action === 'reconcile') {
          setMessage(actionResult, 'Running reconciliation…');
          const response = await api.reconcileTopologySnapshot(snapshotId);
          selectedSnapshotId = snapshotId;
          selectedReconciliationId = Number(response.snapshot.reconciliation_id ?? 0) || null;
          await refresh();
          setMessage(actionResult, 'Reconciliation completed.', 'success');
        } else if (action === 'apply') {
          setMessage(actionResult, 'Applying snapshot to live topology references…');
          const response = await api.applyTopologySnapshot(snapshotId);
          actionResult.innerHTML = renderTable([response.result]);
          await refresh();
        }
      } catch (error) {
        setMessage(actionResult, `Topology action failed: ${String(error)}`, 'error');
      }
    });

    detailsForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      try {
        await loadDetails();
      } catch (error) {
        setMessage(details, `Loading reconciliation details failed: ${String(error)}`, 'error');
      }
    });

    syncTopologyBtn.addEventListener('click', async () => {
      setMessage(actionResult, 'Running sync-topology…');
      try {
        const response = await api.syncTopologyViaTopologyApi();
        actionResult.innerHTML = renderTable([response.result]);
        const unmappedEntities = await api.getUnmappedEntities(20);
        unmapped.innerHTML = renderTable(unmappedEntities.rows);
      } catch (error) {
        setMessage(actionResult, `sync-topology failed: ${String(error)}`, 'error');
      }
    });

    loadCoverageBtn.addEventListener('click', async () => {
      setMessage(siteCoverage, 'Loading detailed site coverage…');
      setMessage(regionCoverage, 'Loading detailed region coverage…');
      try {
        await loadCoverage();
      } catch (error) {
        setMessage(siteCoverage, `Loading site coverage failed: ${String(error)}`, 'error');
        setMessage(regionCoverage, `Loading region coverage failed: ${String(error)}`, 'error');
      }
    });

    await refresh();
  }
};
