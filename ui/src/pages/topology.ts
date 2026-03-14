import { api } from '../api/client';
import { renderTable, setMessage } from '../components/render';
import type { PageModule } from '../router';

export const page: PageModule = {
  title: 'Topology',
  subtitle: 'Inspect topology coverage and manually run entity/topology synchronization.',
  async render(container) {
    container.innerHTML = `
      <div class="grid cols-2">
        <section class="panel">
          <h3>Manual operations</h3>
          <div class="actions">
            <button id="sync-entities-btn" class="primary">Sync entities</button>
            <button id="sync-topology-btn">Sync topology</button>
          </div>
          <div id="topology-op-result"></div>
        </section>
        <section class="panel">
          <h3>Unmapped entities</h3>
          <div id="unmapped-table"></div>
        </section>
      </div>
      <div class="grid cols-2">
        <section class="panel"><h3>Site coverage</h3><div id="site-coverage-table"></div></section>
        <section class="panel"><h3>Region coverage</h3><div id="region-coverage-table"></div></section>
      </div>
    `;

    const opResult = container.querySelector<HTMLElement>('#topology-op-result');
    const unmapped = container.querySelector<HTMLElement>('#unmapped-table');
    const siteCoverage = container.querySelector<HTMLElement>('#site-coverage-table');
    const regionCoverage = container.querySelector<HTMLElement>('#region-coverage-table');
    const syncEntitiesBtn = container.querySelector<HTMLButtonElement>('#sync-entities-btn');
    const syncTopologyBtn = container.querySelector<HTMLButtonElement>('#sync-topology-btn');
    if (!opResult || !unmapped || !siteCoverage || !regionCoverage || !syncEntitiesBtn || !syncTopologyBtn) return;

    const load = async () => {
      try {
        const [unmappedEntities, siteRows, regionRows] = await Promise.all([
          api.getUnmappedEntities(20),
          api.getSiteCoverage(20),
          api.getRegionCoverage(20)
        ]);
        unmapped.innerHTML = renderTable(unmappedEntities.rows);
        siteCoverage.innerHTML = renderTable(siteRows.rows);
        regionCoverage.innerHTML = renderTable(regionRows.rows);
      } catch (error) {
        setMessage(container, `Topology page load failed: ${String(error)}`, 'error');
      }
    };

    syncEntitiesBtn.addEventListener('click', async () => {
      setMessage(opResult, 'Running sync-entities…');
      try {
        const response = await api.syncEntities();
        opResult.innerHTML = renderTable([response.result]);
      } catch (error) {
        setMessage(opResult, `sync-entities failed: ${String(error)}`, 'error');
      }
    });

    syncTopologyBtn.addEventListener('click', async () => {
      setMessage(opResult, 'Running sync-topology…');
      try {
        const response = await api.syncTopology();
        opResult.innerHTML = renderTable([response.result]);
        await load();
      } catch (error) {
        setMessage(opResult, `sync-topology failed: ${String(error)}`, 'error');
      }
    });

    await load();
  }
};
