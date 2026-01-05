export function parseIntOrNull(v: string | undefined): number | null {
  if (v == null || v === "") return null;
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

export function parseBoolOrNull(v: string | undefined): boolean | null {
  if (v == null) return null;
  const s = v.trim().toLowerCase();
  if (!s) return null;
  if (s === "true" || s === "t" || s === "1" || s === "yes" || s === "y") return true;
  if (s === "false" || s === "f" || s === "0" || s === "no" || s === "n") return false;
  return null;
}

export function normalizeClientIp(v: string | null | undefined): string | null {
  if (!v) return null;
  const s = v.trim();
  if (!s) return null;
  const idx = s.lastIndexOf(":");
  return idx > 0 ? s.slice(0, idx) : s;
}

export function parseEventTimeMs(ts: string): number | null {
  const s = ts.trim();
  const m =
    /^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})(?:[,.](\d{3,6}))?(?:Z|([+-])(\d{2}):?(\d{2}))?$/.exec(
      s
    ) ?? null;
  if (!m) return null;
  const y = Number.parseInt(m[1], 10);
  const mo = Number.parseInt(m[2], 10);
  const d = Number.parseInt(m[3], 10);
  const h = Number.parseInt(m[4], 10);
  const mi = Number.parseInt(m[5], 10);
  const sec = Number.parseInt(m[6], 10);
  const frac = m[7];
  const ms = frac ? Number.parseInt(frac.slice(0, 3).padEnd(3, "0"), 10) : 0;
  if (![y, mo, d, h, mi, sec, ms].every(Number.isFinite)) return null;
  const hasZ = s.endsWith("Z");
  const sign = m[8];
  if (!sign && !hasZ) {
    const localMs = new Date(y, mo - 1, d, h, mi, sec, ms).getTime();
    return Number.isFinite(localMs) ? localMs : null;
  }
  const baseUtc = Date.UTC(y, mo - 1, d, h, mi, sec, ms);
  if (!sign) return baseUtc;
  const oh = Number.parseInt(m[9], 10);
  const om = Number.parseInt(m[10], 10);
  if (![oh, om].every(Number.isFinite)) return baseUtc;
  const offsetMs = (oh * 60 + om) * 60_000;
  return sign === "+" ? baseUtc - offsetMs : baseUtc + offsetMs;
}
