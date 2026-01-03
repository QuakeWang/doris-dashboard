const CR_RE = /\r/g;
const WS_RE = /\s+/g;
const NOT_IN_LIST_RE = /\bNOT\s+IN\s*\((?:[^()]|\([^()]*\))*\)/gi;
const IN_LIST_RE = /\bIN\s*\((?:[^()]|\([^()]*\))*\)/gi;
const SQL_STRING_RE = /'([^']|'')*'/g;
const NUMBER_RE = /\b\d+(\.\d+)?\b/g;

const WITH_CTE_RE = /(?:^|\bwith\b|,)\s*([a-zA-Z0-9_]+)\s+as\s*\(/gi;
const SQL_IDENT_PART_RE = /(?:`[^`]+`|[a-zA-Z0-9_$]+)/.source;
const SQL_QUALIFIED_NAME_RE = `${SQL_IDENT_PART_RE}(?:\\s*\\.\\s*${SQL_IDENT_PART_RE})*`;
const FROM_RE = new RegExp(`\\bfrom\\s+(${SQL_QUALIFIED_NAME_RE})`, "gi");

function normalizeQualifiedName(raw: string): string {
  return raw
    .split(".")
    .map((p) => p.trim().replace(/^`|`$/g, ""))
    .filter(Boolean)
    .join(".");
}

export function normalizeSqlBase(sql: string): string {
  let s = sql.replace(CR_RE, " ").replace(WS_RE, " ").trim();

  s = s.replace(NOT_IN_LIST_RE, "NOT IN (?)");
  s = s.replace(IN_LIST_RE, "IN (?)");

  s = s.replace(SQL_STRING_RE, "?");
  s = s.replace(NUMBER_RE, "?");

  return s.replace(WS_RE, " ").trim();
}

export function guessTableFromSqlTemplate(sqlTemplate: string): string | null {
  const s = sqlTemplate;
  const cte = new Set<string>();

  WITH_CTE_RE.lastIndex = 0;
  for (let m = WITH_CTE_RE.exec(s); m; m = WITH_CTE_RE.exec(s)) {
    cte.add(m[1].toLowerCase());
  }

  FROM_RE.lastIndex = 0;
  for (let m = FROM_RE.exec(s); m; m = FROM_RE.exec(s)) {
    const raw = m[1];
    if (!raw) continue;
    const t = normalizeQualifiedName(raw);
    if (!t) continue;
    const firstPart = t.split(".")[0] ?? "";
    if (firstPart && cte.has(firstPart.toLowerCase())) continue;
    return t;
  }

  return null;
}
