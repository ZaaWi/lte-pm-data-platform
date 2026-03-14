export type PageKey = 'overview' | 'ingestion' | 'kpi-results' | 'validation' | 'topology';

export interface PageModule {
  title: string;
  subtitle: string;
  render(container: HTMLElement): Promise<void> | void;
}

const routes: Record<PageKey, () => Promise<PageModule>> = {
  overview: () => import('./pages/overview').then((m) => m.page),
  ingestion: () => import('./pages/ingestion').then((m) => m.page),
  'kpi-results': () => import('./pages/kpi-results').then((m) => m.page),
  validation: () => import('./pages/validation').then((m) => m.page),
  topology: () => import('./pages/topology').then((m) => m.page)
};

export function getCurrentRoute(): PageKey {
  const hash = window.location.hash.replace(/^#\/?/, '') as PageKey;
  return routes[hash] ? hash : 'overview';
}

export async function resolveCurrentPage(): Promise<PageModule> {
  return routes[getCurrentRoute()]();
}

export const navItems: Array<{ key: PageKey; label: string }> = [
  { key: 'overview', label: 'Overview' },
  { key: 'ingestion', label: 'Ingestion' },
  { key: 'kpi-results', label: 'KPI Results' },
  { key: 'validation', label: 'Validation' },
  { key: 'topology', label: 'Topology' }
];
