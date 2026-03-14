import { api, type KpiFamily, type KpiGrain } from '../api/client';
import { renderTable, setMessage } from '../components/render';
import type { PageModule } from '../router';

function normalizeDateStart(value: FormDataEntryValue | null): string {
  const raw = String(value ?? '').trim();
  return raw ? `${raw}T00:00:00` : '';
}

function normalizeDateEnd(value: FormDataEntryValue | null): string {
  const raw = String(value ?? '').trim();
  return raw ? `${raw}T23:59:59` : '';
}

export const page: PageModule = {
  title: 'KPI Results',
  subtitle: 'Inspect verified KPI outputs at entity_time, site_time, and region_time grains.',
  async render(container) {
    let currentOffset = 0;
    let lastRowCount = 0;

    container.innerHTML = `
      <section class="panel">
        <h3>Filters</h3>
        <form id="kpi-results-form" class="grid">
          <div class="form-grid kpi-filter-row-main">
            <label><div class="small">Family</div>
              <select name="family">
                <option value="prb">prb</option>
                <option value="bler">bler</option>
                <option value="rrc">rrc</option>
              </select>
            </label>
            <label><div class="small">Grain</div>
              <select name="grain">
                <option value="entity-time">entity-time</option>
                <option value="site-time">site-time</option>
                <option value="region-time">region-time</option>
              </select>
            </label>
            <label><div class="small">Dataset family</div><input name="dataset_family" placeholder="PM/sdr/ltefdd" value="PM/sdr/ltefdd" /></label>
            <label><div class="small">From</div><input name="collect_time_from" type="date" /></label>
            <label><div class="small">To</div><input name="collect_time_to" type="date" /></label>
          </div>
          <div class="form-grid kpi-filter-row-secondary">
            <label><div class="small">site_code</div><input name="site_code" /></label>
            <label><div class="small">region_code</div><input name="region_code" /></label>
            <label class="compact-field"><div class="small">Rows</div><input name="limit" type="number" value="50" min="1" /></label>
            <input name="offset" type="hidden" value="0" />
            <div class="actions kpi-filter-actions">
              <button id="kpi-prev-page" type="button">Previous</button>
              <button id="kpi-next-page" type="button">Next</button>
              <button class="primary" type="submit">Load results</button>
            </div>
          </div>
        </form>
      </section>
      <section class="panel">
        <h3>Results</h3>
        <div id="kpi-results-status" class="small">Set filters and click Load results. Entity-time queries require dataset_family and default to the latest collect_time when no time range is provided.</div>
        <div id="kpi-results-table"></div>
      </section>
    `;

    const form = container.querySelector<HTMLFormElement>('#kpi-results-form');
    const status = container.querySelector<HTMLElement>('#kpi-results-status');
    const table = container.querySelector<HTMLElement>('#kpi-results-table');
    const prevButton = container.querySelector<HTMLButtonElement>('#kpi-prev-page');
    const nextButton = container.querySelector<HTMLButtonElement>('#kpi-next-page');
    if (!form || !status || !table || !prevButton || !nextButton) return;

    const updatePagingControls = (limit: number) => {
      prevButton.disabled = currentOffset <= 0;
      nextButton.disabled = lastRowCount < limit;
    };

    const load = async (resetOffset = false) => {
      if (resetOffset) currentOffset = 0;
      const formData = new FormData(form);
      const family = String(formData.get('family')) as KpiFamily;
      const grain = String(formData.get('grain')) as KpiGrain;
      const datasetFamily = String(formData.get('dataset_family') ?? '').trim();
      const limit = Number(formData.get('limit') ?? 50);
      const offsetInput = form.elements.namedItem('offset') as HTMLInputElement | null;
      if (offsetInput) offsetInput.value = String(currentOffset);
      if (grain === 'entity-time' && !datasetFamily) {
        setMessage(table, 'dataset_family is required for entity-time KPI results.', 'error');
        status.innerHTML = '';
        lastRowCount = 0;
        updatePagingControls(limit);
        return;
      }
      status.innerHTML = '';
      setMessage(table, 'Loading KPI results…');
      try {
        const response = await api.getKpiResults(grain, {
          family,
          limit,
          offset: currentOffset,
          dataset_family: datasetFamily,
          collect_time_from: normalizeDateStart(formData.get('collect_time_from')),
          collect_time_to: normalizeDateEnd(formData.get('collect_time_to')),
          site_code: grain === 'site-time' ? String(formData.get('site_code') ?? '') : undefined,
          region_code: grain === 'region-time' ? String(formData.get('region_code') ?? '') : undefined
        });
        lastRowCount = response.count;
        const pageNumber = Math.floor(currentOffset / limit) + 1;
        status.innerHTML = `<div class="small">Returned ${response.count} rows. Page ${pageNumber}, offset ${currentOffset}.</div>`;
        table.innerHTML = renderTable(response.rows);
        updatePagingControls(limit);
      } catch (error) {
        lastRowCount = 0;
        updatePagingControls(limit);
        setMessage(table, `KPI results load failed: ${String(error)}`, 'error');
      }
    };

    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      await load(true);
    });

    prevButton.addEventListener('click', async () => {
      const limit = Number((new FormData(form).get('limit') ?? 50));
      currentOffset = Math.max(0, currentOffset - limit);
      await load(false);
    });

    nextButton.addEventListener('click', async () => {
      const limit = Number((new FormData(form).get('limit') ?? 50));
      currentOffset += limit;
      await load(false);
    });

    updatePagingControls(Number((new FormData(form).get('limit') ?? 50)));
  }
};
