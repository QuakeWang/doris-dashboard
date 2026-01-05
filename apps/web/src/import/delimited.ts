export function splitDelimitedLine(line: string, delimiter: string, quote = '"'): string[] | null {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i] as string;
    if (inQuotes) {
      if (ch === quote) {
        const next = line[i + 1];
        if (next === quote) {
          cur += quote;
          i++;
          continue;
        }
        inQuotes = false;
        continue;
      }
      cur += ch;
      continue;
    }

    if (ch === delimiter) {
      out.push(cur);
      cur = "";
      continue;
    }
    if (ch === quote && cur === "") {
      inQuotes = true;
      continue;
    }
    cur += ch;
  }

  if (inQuotes) return null;
  out.push(cur);
  return out;
}
