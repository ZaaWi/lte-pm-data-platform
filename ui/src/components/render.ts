export function escapeHtml(value: unknown): string {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

export function renderTable(rows: Array<Record<string, unknown>>): string {
  if (!rows.length) {
    return '<div class="status">No rows returned.</div>';
  }
  const columns = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
  const header = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('');
  const body = rows
    .map(
      (row) =>
        `<tr>${columns
          .map((column) => `<td>${escapeHtml(formatValue(row[column]))}</td>`)
          .join('')}</tr>`
    )
    .join('');
  return `<div class="table-wrap"><table class="table"><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table></div>`;
}

export function renderKeyValue(data: Record<string, unknown>): string {
  return `
    <div class="table-wrap">
      <table class="table">
        <tbody>
          ${Object.entries(data)
            .map(([key, value]) => `<tr><th>${escapeHtml(key)}</th><td>${escapeHtml(formatValue(value))}</td></tr>`)
            .join('')}
        </tbody>
      </table>
    </div>
  `;
}

export function setMessage(target: HTMLElement, message: string, tone: 'error' | 'success' | 'neutral' = 'neutral'): void {
  target.innerHTML = `<div class="status ${tone === 'neutral' ? '' : tone}">${escapeHtml(message)}</div>`;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}
