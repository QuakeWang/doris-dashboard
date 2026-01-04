import { describe, expect, it } from "vitest";
import { parseAuditLogRecordBlock } from "./auditLogParser";
import { extractKvAndStmt } from "./kvExtractor";
import { iterateRecordBlocks } from "./recordReader";
import { normalizeSqlBase } from "./sqlTemplate";
import { DEFAULT_STRIPPING_RULES, applyStrippingRules } from "./strippingRules";

describe("iterateRecordBlocks", () => {
  it("splits records by timestamp prefix", async () => {
    const text =
      "2025-12-25 11:17:02,188 [query] |User=a|Stmt=select 1|CpuTimeMS=1\n" +
      "2025-12-25 11:17:03.188 INFO [query] |User=b|Stmt=select 2|CpuTimeMS=2\n";
    const file = new Blob([text], { type: "text/plain" });
    const blocks: string[] = [];
    for await (const b of iterateRecordBlocks(file)) blocks.push(b.raw);
    expect(blocks).toHaveLength(2);
    expect(blocks[0]).toContain("User=a");
    expect(blocks[1]).toContain("User=b");
  });
});

describe("extractKvAndStmt", () => {
  it("extracts Stmt value without breaking other fields", () => {
    const body = "|User=a|Stmt=select 1\nfrom t where k=1|CpuTimeMS=7|SqlHash=abc|";
    const { kv, stmtRaw } = extractKvAndStmt(body);
    expect(stmtRaw).toContain("from t");
    expect(kv["User"]).toBe("a");
    expect(kv["CpuTimeMS"]).toBe("7");
    expect(kv["SqlHash"]).toBe("abc");
  });

  it("does not rely on fixed end candidates after Stmt", () => {
    const body = "|User=a|Stmt=select 1|Time(ms)=13|ScanBytes=10|";
    const { kv, stmtRaw } = extractKvAndStmt(body);
    expect(stmtRaw).toBe("select 1");
    expect(kv["Time(ms)"]).toBe("13");
    expect(kv["ScanBytes"]).toBe("10");
  });
});

describe("sqlTemplate", () => {
  it("normalizes literals", () => {
    const s = normalizeSqlBase("select * from t where a = 10 and b = 'x'");
    expect(s).toContain("a = ?");
    expect(s).toContain("b = ?");
  });
});

describe("strippingRules", () => {
  it("strips soft delete predicate", () => {
    const base = "select * from t where a = ? and is_deleted = ?";
    const res = applyStrippingRules(base, DEFAULT_STRIPPING_RULES);
    expect(res.stripped).toBe("select * from t where a = ?");
  });
});

describe("parseAuditLogRecordBlock", () => {
  it("parses core fields", () => {
    const line =
      "2025-12-25 11:17:02,188 [query] |Client=10.0.0.1:123|User=u|Db=d|State=EOF|ErrorCode=0|ErrorMessage=|Time(ms)=13|ScanBytes=10|ScanRows=1|ReturnRows=1|QueryId=q|Stmt=select 1|CpuTimeMS=2|peakMemoryBytes=3|";
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.eventTimeMs).toBe(new Date(2025, 11, 25, 11, 17, 2, 188).getTime());
    expect(r!.clientIp).toBe("10.0.0.1");
    expect(r!.userName).toBe("u");
    expect(r!.dbName).toBe("d");
    expect(r!.queryTimeMs).toBe(13);
    expect(r!.cpuTimeMs).toBe(2);
    expect(r!.stmtRaw).toBe("select 1");
    expect(r!.sqlTemplateStripped).toBe("select ?");
  });

  it("parses dot microseconds and optional prefix", () => {
    const line =
      "2025-12-25 11:17:02.188123 INFO [query] |Client=10.0.0.1:123|User=u|Db=d|State=EOF|QueryId=q|Stmt=select 1|";
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.eventTimeMs).toBe(new Date(2025, 11, 25, 11, 17, 2, 188).getTime());
  });

  it("prefers Timestamp field over prefix time", () => {
    const line =
      "2025-12-25 11:17:02,188 [query] |Timestamp=2025-12-25 11:16:59.100|Client=10.0.0.1:123|User=u|Db=d|State=EOF|QueryId=q|Stmt=select 1|";
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.eventTimeMs).toBe(new Date(2025, 11, 25, 11, 16, 59, 100).getTime());
  });

  it("extracts table_guess from queriedTablesAndViews", () => {
    const line =
      '2025-12-25 11:17:02,188 [query] |Timestamp=2025-12-25 11:16:59.100|Db=d|QueryId=q|queriedTablesAndViews=["internal.db1.tbl1","internal.db2.tbl2"]|Stmt=select 1|';
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.tableGuess).toBe("internal.db1.tbl1");
  });

  it("extracts table_guess from backtick-qualified FROM", () => {
    const line =
      "2025-12-25 11:17:02,188 [query] |Db=d|QueryId=q|Stmt=select * from `db1`.`tbl1` where k = 1|";
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.tableGuess).toBe("db1.tbl1");
  });

  it("parses IsInternal boolean", () => {
    const line =
      "2025-12-25 11:17:02,188 [query] |IsInternal=true|User=u|Db=d|QueryId=q|Stmt=select 1|";
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.isInternal).toBe(true);
  });

  it("handles carriage returns inside Stmt", () => {
    const line =
      "2025-12-25 11:17:02,188 [query] |Db=d|QueryId=q|Stmt=select\r 1\r from t where a = 10 and b = 'x' and is_deleted = 0|";
    const r = parseAuditLogRecordBlock(line);
    expect(r).not.toBeNull();
    expect(r!.stmtRaw).toContain("\r");
    expect(r!.sqlTemplateStripped).toBe("select ? from t where a = ? and b = ?");
  });
});
