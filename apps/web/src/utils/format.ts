const WS_RE = /\s+/g;

export function formatNumber(n: number | null | undefined, digits?: number): string {
  if (n == null || !Number.isFinite(n)) return "-";
  if (digits == null) return new Intl.NumberFormat("en-US").format(n);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(n);
}

export function formatPct(x: number | null | undefined, digits = 2): string {
  if (x == null || !Number.isFinite(x)) return "-";
  return `${(x * 100).toFixed(digits)}%`;
}

export function formatDurationMs(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return "-";
  if (ms < 1000) return `${ms.toFixed(0)} ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(2)} s`;
  const min = sec / 60;
  if (min < 60) return `${min.toFixed(2)} min`;
  const hr = min / 60;
  return `${hr.toFixed(2)} h`;
}

export function formatBytes(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

export function truncateMiddle(s: string, max = 140): string {
  if (s.length <= max) return s;
  const left = Math.floor((max - 3) / 2);
  const right = max - 3 - left;
  return `${s.slice(0, left)}...${s.slice(s.length - right)}`;
}

export function formatSqlLabel(sql: string, max = 140): string {
  return truncateMiddle(sql.replace(WS_RE, " ").trim(), max);
}
