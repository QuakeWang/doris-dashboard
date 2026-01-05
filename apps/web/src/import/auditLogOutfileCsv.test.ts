import { describe, expect, it } from "vitest";
import { parseAuditLogOutfileLine, unescapeOutfileText } from "./auditLogOutfileCsv";
import { splitDelimitedLine } from "./delimited";

describe("splitDelimitedLine", () => {
  it("splits CSV with quotes", () => {
    const line = 'a,"b,c",d';
    const parts = splitDelimitedLine(line, ",");
    expect(parts).toEqual(["a", "b,c", "d"]);
  });

  it("handles escaped quotes", () => {
    const line = 'a,"b""c",d';
    const parts = splitDelimitedLine(line, ",");
    expect(parts).toEqual(["a", 'b"c', "d"]);
  });
});

describe("unescapeOutfileText", () => {
  it("unescapes common sequences", () => {
    expect(unescapeOutfileText("hello\\nworld")).toBe("hello\nworld");
    expect(unescapeOutfileText("a\\tb")).toBe("a\tb");
    expect(unescapeOutfileText("a\\rb")).toBe("a\rb");
    expect(unescapeOutfileText("a\\\\b")).toBe("a\\b");
  });

  it("preserves unknown sequences", () => {
    expect(unescapeOutfileText("a\\xb")).toBe("a\\xb");
  });
});

describe("parseAuditLogOutfileLine", () => {
  it("parses TSV export of __internal_schema.audit_log", () => {
    const fields = [
      "1cd481ac9aad4c0a-8baad6985f50c6b1",
      "2026-01-05 06:44:17.002000",
      "10.16.10.6:51178",
      "root",
      "internal",
      "tpch",
      "EOF",
      "0",
      "",
      "7522",
      "6866444288",
      "147886104",
      "4",
      "0",
      "0",
      "0",
      "0",
      "19",
      "SELECT",
      "1",
      "1",
      "10.16.10.6",
      "27890",
      "e727eecfd3e6d213e6b3511a581c2e65",
      "d41d8cd98f00b204e9800998ecf8427e",
      "166479328",
      "normal",
      "UNKNOWN",
      "select\\n *\\nfrom\\n lineitem\\nwhere\\n a = 10 and b = 'x'",
    ];
    const line = fields.join("\t");
    const res = parseAuditLogOutfileLine(line, "\t");
    expect(res.kind).toBe("record");
    if (res.kind !== "record") throw new Error("expected record");
    const r = res.record;
    expect(r.queryId).toBe(fields[0]);
    expect(r.eventTimeMs).toBe(new Date(2026, 0, 5, 6, 44, 17, 2).getTime());
    expect(r.clientIp).toBe("10.16.10.6");
    expect(r.userName).toBe("root");
    expect(r.dbName).toBe("tpch");
    expect(r.state).toBe("EOF");
    expect(r.queryTimeMs).toBe(7522);
    expect(r.cpuTimeMs).toBe(27890);
    expect(r.peakMemoryBytes).toBe(166479328);
    expect(r.workloadGroup).toBe("normal");
    expect(r.cloudClusterName).toBe("UNKNOWN");
    expect(r.stmtRaw).toContain("from");
    expect(r.sqlTemplateStripped).toBe("select * from lineitem where a = ? and b = ?");
    expect(r.tableGuess).toBe("tpch.lineitem");
  });
});
