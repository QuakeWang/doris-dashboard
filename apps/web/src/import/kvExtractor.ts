export interface ExtractedKv {
  kv: Record<string, string>;
  stmtRaw: string | null;
}

export function extractKvAndStmt(body: string): ExtractedKv {
  const kv: Record<string, string> = {};
  let stmtRaw: string | null = null;

  const re = /\|([A-Za-z][A-Za-z0-9_().-]*)=/g;
  const matches: Array<{ key: string; keyStart: number; valueStart: number }> = [];
  while (true) {
    const m = re.exec(body);
    if (!m) break;
    matches.push({ key: m[1], keyStart: m.index, valueStart: re.lastIndex });
  }

  for (let i = 0; i < matches.length; i++) {
    const cur = matches[i];
    const next = matches[i + 1];
    const valueEnd = next ? next.keyStart : body.length;
    let value = body.slice(cur.valueStart, valueEnd).trim();
    if (!next && value.endsWith("|")) value = value.slice(0, -1).trim();
    if (!cur.key) continue;
    if (cur.key.toLowerCase() === "stmt") {
      stmtRaw = value;
      continue;
    }
    kv[cur.key] = value;
  }

  return { kv, stmtRaw };
}
