import { api } from '../api/client';
import { renderKeyValue, renderTable, setMessage } from '../components/render';
import type { PageModule } from '../router';

export const page: PageModule = {
  title: 'Overview',
  subtitle: 'Current platform status across ingestion, topology, and verified KPI validation.',
  async render(container) {
    container.innerHTML = `
      <div class="grid">
        <div class="kpi-cards" id="summary-cards"></div>
        <div class="grid cols-2">
          <section class="panel"><h3>Recent failures</h3><div id="failures"></div></section>
          <section class="panel"><h3>Topology coverage snapshot</h3><div id="topology"></div></section>
        </div>
        <div class="grid cols-2">
          <section class="panel"><h3>Site/region coverage sample</h3><div id="coverage"></div></section>
          <section class="panel"><h3>Validation snapshot</h3><div id="validation"></div></section>
        </div>
      </div>
    `;

    const cards = container.querySelector<HTMLElement>('#summary-cards');
    const failures = container.querySelector<HTMLElement>('#failures');
    const topology = container.querySelector<HTMLElement>('#topology');
    const coverage = container.querySelector<HTMLElement>('#coverage');
    const validation = container.querySelector<HTMLElement>('#validation');
    if (!cards || !failures || !topology || !coverage || !validation) return;

    try {
      const [ingestionStatus, unmapped, siteCoverage, regionCoverage, prbValidation, blerValidation, rrcValidation] =
        await Promise.all([
          api.getIngestionStatus(5),
          api.getUnmappedEntities(5),
          api.getSiteCoverage(5),
          api.getRegionCoverage(5),
          api.getKpiValidation('site-time', { family: 'prb' }),
          api.getKpiValidation('site-time', { family: 'bler' }),
          api.getKpiValidation('entity-time', { family: 'rrc' })
        ]);

      const summary = ingestionStatus.summary;
      cards.innerHTML = [
        ['Pending downloads', summary.pending_downloads],
        ['Pending ingests', summary.pending_ingests],
        ['Failed ingests', summary.failed_ingests],
        ['Reconciliation needed', summary.reconciliation_needed]
      ]
        .map(
          ([label, value]) =>
            `<div class="kpi-card"><div class="label">${label}</div><div class="value">${value ?? 0}</div></div>`
        )
        .join('');

      failures.innerHTML = renderTable(ingestionStatus.recent_failures.slice(0, 5));
      topology.innerHTML = renderKeyValue({
        unmapped_entities_sample: unmapped.count,
        latest_scan_at: ingestionStatus.latest_scan_at ?? 'n/a'
      });
      coverage.innerHTML = renderTable([
        ...(siteCoverage.rows.slice(0, 3) as Array<Record<string, unknown>>),
        ...(regionCoverage.rows.slice(0, 2) as Array<Record<string, unknown>>)
      ]);
      validation.innerHTML = renderTable([
        ...(prbValidation.rows as Array<Record<string, unknown>>),
        ...(blerValidation.rows as Array<Record<string, unknown>>),
        ...(rrcValidation.rows as Array<Record<string, unknown>>)
      ]);
    } catch (error) {
      setMessage(container, `Overview load failed: ${String(error)}`, 'error');
    }
  }
};
