import { api, type KpiFamily, type KpiGrain } from '../api/client';
import { renderTable, setMessage } from '../components/render';
import type { PageModule } from '../router';

export const page: PageModule = {
  title: 'Validation',
  subtitle: 'Inspect verified KPI validation summaries before trusting operational outputs.',
  async render(container) {
    container.innerHTML = `
      <section class="panel">
        <h3>Filters</h3>
        <form id="validation-form" class="grid">
          <div class="form-grid">
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
          </div>
          <div class="actions"><button class="primary" type="submit">Load validation</button></div>
        </form>
      </section>
      <section class="panel">
        <h3>Validation results</h3>
        <div id="validation-table"></div>
      </section>
    `;

    const form = container.querySelector<HTMLFormElement>('#validation-form');
    const table = container.querySelector<HTMLElement>('#validation-table');
    if (!form || !table) return;

    const load = async () => {
      const formData = new FormData(form);
      const family = String(formData.get('family')) as KpiFamily;
      const grain = String(formData.get('grain')) as KpiGrain;
      setMessage(table, 'Loading validation…');
      try {
        const response = await api.getKpiValidation(grain, { family });
        table.innerHTML = renderTable(response.rows);
      } catch (error) {
        setMessage(table, `Validation load failed: ${String(error)}`, 'error');
      }
    };

    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      await load();
    });

    await load();
  }
};
