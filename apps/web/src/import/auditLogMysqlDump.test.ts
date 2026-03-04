import { describe, expect, it } from "vitest";
import {
  type MysqlDumpReaderOptions,
  isMysqlDumpNoiseRecord,
  iterateAuditLogRecordsFromMysqlDump,
  looksLikeAuditLogMysqlDump,
} from "./auditLogMysqlDump";
import type { ParsedAuditLogRecord } from "./auditLogParser";

async function collect(
  sql: string,
  options: MysqlDumpReaderOptions = {}
): Promise<ParsedAuditLogRecord[]> {
  const blob = new Blob([sql], { type: "text/plain" });
  return await toArray(iterateAuditLogRecordsFromMysqlDump(blob, options));
}

async function toArray<T>(iter: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const v of iter) out.push(v);
  return out;
}

describe("looksLikeAuditLogMysqlDump", () => {
  it("detects INSERT INTO audit_log", () => {
    expect(looksLikeAuditLogMysqlDump("INSERT INTO `audit_log` VALUES (1);\n")).toBe(true);
  });

  it("detects INSERT IGNORE INTO audit_log", () => {
    expect(looksLikeAuditLogMysqlDump("INSERT IGNORE INTO `audit_log` VALUES (1);\n")).toBe(true);
  });

  it("detects schema-qualified table", () => {
    expect(
      looksLikeAuditLogMysqlDump("CREATE TABLE `__internal_schema`.`audit_log` (id int);\n")
    ).toBe(true);
  });

  it("rejects irrelevant samples", () => {
    expect(looksLikeAuditLogMysqlDump("INSERT INTO `orders` VALUES (1);\n")).toBe(false);
  });

  it("does not match when SQL marker appears inside CSV stmt field", () => {
    const csvLine =
      "q-1,2026-01-05 06:44:17.002000,203.0.113.10:51178,test_user,,test_db,EOF,0,,13,10,11,12,,,,,,,,,10.0.0.1,19,,,20,wg1,cg1,select 1; insert into audit_log values (1)";
    expect(looksLikeAuditLogMysqlDump(csvLine)).toBe(false);
  });
});

describe("iterateAuditLogRecordsFromMysqlDump", () => {
  it("parses insert with explicit columns", async () => {
    const sql = [
      "SET NAMES utf8mb4;",
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`user`,`db`,`state`,`error_code`,`error_message`,`query_time`,`scan_bytes`,`scan_rows`,`return_rows`,`frontend_ip`,`cpu_time_ms`,`peak_memory_bytes`,`workload_group`,`compute_group`,`stmt`) VALUES",
      "('q-1','2026-01-05 06:44:17.002000','203.0.113.10:51178','test_user','test_db','EOF',0,'',7522,10,11,12,'10.0.0.1',27890,166479328,'wg1','cg1','select * from test_table where a = 10 and b = \\'x\\'');",
    ].join("\n");

    const rows = await collect(sql);
    expect(rows).toHaveLength(1);
    const r = rows[0];
    expect(r?.queryId).toBe("q-1");
    expect(r?.eventTimeMs).toBe(new Date(2026, 0, 5, 6, 44, 17, 2).getTime());
    expect(r?.clientIp).toBe("203.0.113.10");
    expect(r?.userName).toBe("test_user");
    expect(r?.dbName).toBe("test_db");
    expect(r?.queryTimeMs).toBe(7522);
    expect(r?.cpuTimeMs).toBe(27890);
    expect(r?.peakMemoryBytes).toBe(166479328);
    expect(r?.workloadGroup).toBe("wg1");
    expect(r?.cloudClusterName).toBe("cg1");
    expect(r?.sqlTemplateStripped).toBe("select * from test_table where a = ? and b = ?");
    expect(r?.tableGuess).toBe("test_db.test_table");
  });

  it("parses INSERT IGNORE INTO variant", async () => {
    const sql =
      "INSERT IGNORE INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-ig-1','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1');";

    const rows = await collect(sql);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.queryId).toBe("q-ig-1");
    expect(rows[0]?.stmtRaw).toBe("select 1");
  });

  it("parses extended insert without column list by default Doris order", async () => {
    const row1 = new Array(29).fill("NULL");
    row1[0] = "'qid-1'";
    row1[1] = "'2026-01-05 06:44:17.002000'";
    row1[2] = "'203.0.113.20:51178'";
    row1[3] = "'u1'";
    row1[5] = "'db1'";
    row1[6] = "'EOF'";
    row1[7] = "0";
    row1[8] = "''";
    row1[9] = "13";
    row1[10] = "100";
    row1[11] = "101";
    row1[12] = "102";
    row1[21] = "'10.0.0.10'";
    row1[22] = "19";
    row1[25] = "20";
    row1[26] = "'wg'";
    row1[27] = "'cluster-a'";
    row1[28] = "'select * from t1 where k = 1'";

    const row2 = [...row1];
    row2[0] = "'qid-2'";
    row2[28] = "'select * from t2 where k = 2'";

    const sql = `INSERT INTO \`audit_log\` VALUES (${row1.join(",")}),(${row2.join(",")});`;
    const rows = await collect(sql);

    expect(rows).toHaveLength(2);
    expect(rows[0]?.queryId).toBe("qid-1");
    expect(rows[0]?.tableGuess).toBe("db1.t1");
    expect(rows[1]?.queryId).toBe("qid-2");
    expect(rows[1]?.tableGuess).toBe("db1.t2");
  });

  it("keeps semicolons inside string literals", async () => {
    const sql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-1','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1; select 2');";

    const rows = await collect(sql);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.stmtRaw).toBe("select 1; select 2");
    expect(rows[0]?.sqlTemplateStripped).toBe("select ?; select ?");
  });

  it("skips non-audit statements and continues", async () => {
    const sql = [
      "INSERT INTO `orders` VALUES (1);",
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-2','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 42');",
    ].join("\n");

    const rows = await collect(sql);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.queryId).toBe("q-2");
  });

  it("handles escaped newline/tab/backslash", async () => {
    const sql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-3','2026-01-05 06:44:17.002000','203.0.113.10:51178','select\\n\\t\\\\path');";

    const rows = await collect(sql);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.stmtRaw).toBe("select\n\t\\path");
  });

  it("skips oversized statements and reports badStatements", async () => {
    const bad: string[] = [];
    let finalBadStatements = 0;
    const sql = `INSERT INTO \`audit_log\` (\`query_id\`,\`time\`,\`client_ip\`,\`stmt\`) VALUES ('q-oversized','2026-01-05 06:44:17.002000','203.0.113.10:51178','${"x".repeat(512)}');`;

    const rows = await collect(sql, {
      maxStatementChars: 120,
      onProgress: (p) => {
        finalBadStatements = p.badStatements;
      },
      onBadStatement: (detail) => {
        bad.push(detail.reason);
      },
    });

    expect(rows).toHaveLength(0);
    expect(bad).toHaveLength(1);
    expect(bad[0]).toContain("exceeds max size");
    expect(finalBadStatements).toBe(1);
  });
});

describe("isMysqlDumpNoiseRecord", () => {
  it("matches known dump probe statements", () => {
    expect(isMysqlDumpNoiseRecord({ stmtRaw: "select @@version_comment limit 1" })).toBe(true);
    expect(isMysqlDumpNoiseRecord({ stmtRaw: "SHOW VARIABLES LIKE 'gtid_mode'" })).toBe(true);
    expect(isMysqlDumpNoiseRecord({ stmtRaw: "set session net_read_timeout=600" })).toBe(true);
  });

  it("keeps business sql", () => {
    expect(
      isMysqlDumpNoiseRecord({
        stmtRaw:
          "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem GROUP BY n_name",
      })
    ).toBe(false);
    expect(isMysqlDumpNoiseRecord({ stmtRaw: "START TRANSACTION" })).toBe(false);
    expect(isMysqlDumpNoiseRecord({ stmtRaw: "UNLOCK TABLES" })).toBe(false);
  });
});
