import { describe, expect, it } from "vitest";
import { parseAuditLogOutfileLine, unescapeOutfileText } from "./auditLogOutfileCsv";
import { splitDelimitedLine } from "./delimited";

describe("splitDelimitedLine", () => {
  const cases = [
    { line: 'a,"b,c",d', want: ["a", "b,c", "d"] },
    { line: 'a,"b""c",d', want: ["a", 'b"c', "d"] },
  ];
  for (const tc of cases) {
    it(tc.line, () => expect(splitDelimitedLine(tc.line, ",")).toEqual(tc.want));
  }
});

describe("unescapeOutfileText", () => {
  const cases = [
    { input: "hello\\nworld", output: "hello\nworld" },
    { input: "a\\tb", output: "a\tb" },
    { input: "a\\rb", output: "a\rb" },
    { input: "a\\\\b", output: "a\\b" },
    { input: "a\\xb", output: "a\\xb" },
  ];
  for (const tc of cases) {
    it(tc.input, () => expect(unescapeOutfileText(tc.input)).toBe(tc.output));
  }
});

describe("parseAuditLogOutfileLine", () => {
  it("parses TSV export of __internal_schema.audit_log", () => {
    const fields = new Array(29).fill("0");
    fields[0] = "test-query-id";
    fields[1] = "2026-01-05 06:44:17.002000";
    fields[2] = "203.0.113.10:51178";
    fields[3] = "test_user";
    fields[5] = "test_db";
    fields[6] = "EOF";
    fields[7] = "0";
    fields[8] = "";
    fields[9] = "7522";
    fields[21] = "203.0.113.10";
    fields[22] = "27890";
    fields[25] = "166479328";
    fields[26] = "test_wg";
    fields[27] = "test_cluster";
    fields[28] = "select\\n *\\nfrom\\n test_table\\nwhere\\n a = 10 and b = 'x'";
    const line = fields.join("\t");
    const res = parseAuditLogOutfileLine(line, "\t");
    expect(res.kind).toBe("record");
    if (res.kind !== "record") throw new Error("expected record");
    const r = res.record;
    expect(r.queryId).toBe(fields[0]);
    expect(r.eventTimeMs).toBe(new Date(2026, 0, 5, 6, 44, 17, 2).getTime());
    expect(r.clientIp).toBe("203.0.113.10");
    expect(r.userName).toBe("test_user");
    expect(r.dbName).toBe("test_db");
    expect(r.state).toBe("EOF");
    expect(r.queryTimeMs).toBe(7522);
    expect(r.cpuTimeMs).toBe(27890);
    expect(r.peakMemoryBytes).toBe(166479328);
    expect(r.workloadGroup).toBe("test_wg");
    expect(r.cloudClusterName).toBe("test_cluster");
    expect(r.stmtRaw).toContain("from");
    expect(r.sqlTemplateStripped).toBe("select * from test_table where a = ? and b = ?");
    expect(r.tableGuess).toBe("test_db.test_table");
  });
});
