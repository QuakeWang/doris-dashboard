import type * as duckdb from "@duckdb/duckdb-wasm";
import { log } from "./messaging";
import { queryWithParams } from "./sql";

export async function createSchema(c: duckdb.AsyncDuckDBConnection): Promise<void> {
  await c.query(
    "CREATE TABLE IF NOT EXISTS meta (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL);"
  );

  const schemaRes = await queryWithParams(c, "SELECT value FROM meta WHERE key = ?", [
    "schema_version",
  ]);
  const schemaRow = schemaRes.toArray()[0] as Record<string, unknown> | undefined;
  const schemaVersion = schemaRow?.value ? String(schemaRow.value) : null;

  const expectedVersion = "5";
  if (schemaVersion !== expectedVersion) {
    log(
      `Schema version mismatch (${schemaVersion ?? "null"} != ${expectedVersion}), resetting tables.`
    );
    for (const t of ["audit_log_records", "audit_sql_templates_stripped"])
      await c.query(`DROP TABLE IF EXISTS ${t}`);
    await queryWithParams(
      c,
      "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      ["schema_version", expectedVersion]
    );
  }

  await c.query(
    "CREATE TABLE IF NOT EXISTS audit_log_records (dataset_id VARCHAR NOT NULL, record_id BIGINT NOT NULL, event_time_ms BIGINT, is_internal BOOLEAN, query_id VARCHAR, user_name VARCHAR, client_ip VARCHAR, fe_ip VARCHAR, db_name VARCHAR, state VARCHAR, error_code INTEGER, query_time_ms BIGINT, cpu_time_ms BIGINT, scan_bytes BIGINT, scan_rows BIGINT, return_rows BIGINT, peak_memory_bytes BIGINT, stmt_raw VARCHAR, stripped_template_id INTEGER);"
  );
  await c.query(
    "CREATE TABLE IF NOT EXISTS audit_sql_templates_stripped (dataset_id VARCHAR NOT NULL, stripped_template_id INTEGER NOT NULL, sql_template_stripped VARCHAR NOT NULL, table_guess VARCHAR, PRIMARY KEY(dataset_id, stripped_template_id));"
  );
}

export async function dropAuditLogIndexes(c: duckdb.AsyncDuckDBConnection): Promise<void> {
  try {
    for (const idx of ["idx_audit_dataset_time_ms", "idx_audit_dataset_stripped_tpl"])
      await c.query(`DROP INDEX IF EXISTS ${idx}`);
  } catch (e) {
    log(`DROP INDEX skipped: ${e instanceof Error ? e.message : String(e)}`);
  }
}

export async function createAuditLogIndexes(c: duckdb.AsyncDuckDBConnection): Promise<void> {
  try {
    for (const sql of [
      "CREATE INDEX IF NOT EXISTS idx_audit_dataset_time_ms ON audit_log_records(dataset_id, event_time_ms)",
      "CREATE INDEX IF NOT EXISTS idx_audit_dataset_stripped_tpl ON audit_log_records(dataset_id, stripped_template_id)",
    ])
      await c.query(sql);
  } catch (e) {
    log(`CREATE INDEX skipped: ${e instanceof Error ? e.message : String(e)}`);
  }
}
