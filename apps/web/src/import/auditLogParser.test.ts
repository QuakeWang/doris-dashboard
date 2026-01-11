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
});

describe("sqlTemplate + strippingRules", () => {
  it("normalizes literals and strips soft delete predicate", () => {
    const normalized = normalizeSqlBase(
      "select * from t where a = 10 and b = 'x' and is_deleted = 0"
    );
    expect(normalized).toContain("a = ?");
    expect(normalized).toContain("b = ?");

    const res = applyStrippingRules(normalized, DEFAULT_STRIPPING_RULES);
    expect(res.stripped).toBe("select * from t where a = ? and b = ?");
  });
});

describe("parseAuditLogRecordBlock", () => {
  it("parses core fields", () => {
    const r = parseAuditLogRecordBlock(
      "2025-12-25 11:17:02.188123 INFO [query] |IsInternal=true|Client=203.0.113.10:123|User=test_user|Db=test_db|State=EOF |ErrorMessage=test_error|Time(ms)=13|CpuTimeMS=2|QueryId=test_query_id|Stmt=select 1|"
    );
    expect(r).not.toBeNull();
    expect(r!.eventTimeMs).toBe(new Date(2025, 11, 25, 11, 17, 2, 188).getTime());
    expect(r!.isInternal).toBe(true);
    expect(r!.clientIp).toBe("203.0.113.10");
    expect(r!.userName).toBe("test_user");
    expect(r!.dbName).toBe("test_db");
    expect(r!.state).toBe("EOF");
    expect(r!.errorMessage).toBe("test_error");
    expect(r!.queryTimeMs).toBe(13);
    expect(r!.cpuTimeMs).toBe(2);
    expect(r!.stmtRaw).toBe("select 1");
    expect(r!.sqlTemplateStripped).toBe("select ?");
  });

  it("extracts table_guess from queriedTablesAndViews", () => {
    const r = parseAuditLogRecordBlock(
      '2025-12-25 11:17:02,188 [query] |Db=test_db|QueryId=test_query_id|queriedTablesAndViews=["db1.tbl1","db2.tbl2"]|Stmt=select 1|'
    );
    expect(r).not.toBeNull();
    expect(r!.tableGuess).toBe("db1.tbl1");
  });

  it("extracts table_guess from backtick-qualified FROM", () => {
    const r = parseAuditLogRecordBlock(
      "2025-12-25 11:17:02,188 [query] |Db=d|QueryId=q|Stmt=select * from `db1`.`tbl1` where k = 1|"
    );
    expect(r).not.toBeNull();
    expect(r!.tableGuess).toBe("db1.tbl1");
  });
});
