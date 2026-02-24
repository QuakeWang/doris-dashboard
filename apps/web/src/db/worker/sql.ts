import type * as duckdb from "@duckdb/duckdb-wasm";
import type * as arrow from "apache-arrow";
import { log } from "./messaging";

type ArrowTable = Awaited<ReturnType<duckdb.AsyncDuckDBConnection["query"]>>;

export async function queryWithParams(
  c: duckdb.AsyncDuckDBConnection,
  sql: string,
  params: unknown[] = []
): Promise<ArrowTable> {
  if (params.length === 0) return await c.query(sql);
  const stmt = await c.prepare(sql);
  try {
    return await stmt.query(...params);
  } finally {
    await stmt.close();
  }
}

export async function insertArrowViaStaging(
  c: duckdb.AsyncDuckDBConnection,
  targetTable: string,
  table: arrow.Table,
  insertMode: "insert" | "insertOrIgnore" = "insert"
): Promise<void> {
  const stageName = `${targetTable}__stage_${crypto.randomUUID().replace(/-/g, "")}`;
  let stageSchema: string | undefined = "temp";
  try {
    await c.insertArrowTable(table, { name: stageName, schema: stageSchema, create: true });
  } catch (e) {
    stageSchema = undefined;
    await c.insertArrowTable(table, { name: stageName, create: true });
  }
  const stageRef = stageSchema ? `${stageSchema}.${stageName}` : stageName;
  try {
    const insertKeyword = insertMode === "insertOrIgnore" ? "INSERT OR IGNORE" : "INSERT";
    await c.query(`${insertKeyword} INTO ${targetTable} SELECT * FROM ${stageRef}`);
  } finally {
    try {
      await c.query(`DROP TABLE ${stageRef}`);
    } catch (e) {
      log(`DROP TABLE ${stageName} skipped: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
}
