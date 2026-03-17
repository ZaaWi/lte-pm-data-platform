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
      const [ingestionStatus, topologySummary] =
        await Promise.all([
          api.getIngestionStatus(5),
          api.getTopologySummary()
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
        ...topologySummary.summary,
        latest_scan_at: ingestionStatus.latest_scan_at ?? 'n/a'
      });
      coverage.innerHTML =
        '<div class="status">Detailed site/region coverage is loaded on demand from the Topology page.</div>';
      validation.innerHTML =
        '<div class="status">Validation is now on-demand. Open the Validation page and load a specific family/grain with the narrowest practical dataset_family filter.</div>';
    } catch (error) {
      setMessage(container, `Overview load failed: ${String(error)}`, 'error');
    }
  }
};
