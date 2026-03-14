import './styles.css';

import { navItems, getCurrentRoute, resolveCurrentPage } from './router';

const root = document.querySelector<HTMLDivElement>('#app');

if (!root) {
  throw new Error('Missing #app root element');
}

root.innerHTML = `
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">
        <h1>LTE PM Platform</h1>
        <p>Operator console for ingestion, topology, and verified KPI inspection.</p>
      </div>
      <nav class="nav" id="nav"></nav>
    </aside>
    <main class="main">
      <div class="page-header">
        <div>
          <h2 id="page-title"></h2>
          <p id="page-subtitle"></p>
        </div>
        <div class="small mono" id="api-base"></div>
      </div>
      <div id="page-content"></div>
    </main>
  </div>
`;

const nav = root.querySelector<HTMLElement>('#nav');
const title = root.querySelector<HTMLElement>('#page-title');
const subtitle = root.querySelector<HTMLElement>('#page-subtitle');
const content = root.querySelector<HTMLElement>('#page-content');
const apiBase = root.querySelector<HTMLElement>('#api-base');

if (!nav || !title || !subtitle || !content || !apiBase) {
  throw new Error('Missing shell elements');
}

const navElement = nav;
const titleElement = title;
const subtitleElement = subtitle;
const contentElement = content;
const apiBaseElement = apiBase;

apiBaseElement.textContent = window.__LTE_PM_API_BASE__ ?? '';

function renderNav(): void {
  const current = getCurrentRoute();
  navElement.innerHTML = navItems
    .map(
      (item) =>
        `<a href="#/${item.key}" class="${item.key === current ? 'active' : ''}">${item.label}</a>`
    )
    .join('');
}

async function renderPage(): Promise<void> {
  renderNav();
  contentElement.innerHTML = '<div class="status">Loading page…</div>';
  const page = await resolveCurrentPage();
  titleElement.textContent = page.title;
  subtitleElement.textContent = page.subtitle;
  await page.render(contentElement);
}

window.addEventListener('hashchange', () => {
  void renderPage();
});

void renderPage();
