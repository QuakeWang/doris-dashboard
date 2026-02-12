import { useEffect, useState } from "react";
import type { AgentClient, DorisConnectionInput } from "../agent/agentClient";

const DORIS_EXPLAIN_DATABASE_KEY = "doris.explain.database.v1";
const EXPLAIN_TREE_PREFIX_RE =
  /^\s*EXPLAIN(?:\s+(?:PARSED|ANALYZED|REWRITTEN|LOGICAL|OPTIMIZED|PHYSICAL|SHAPE|MEMO|DISTRIBUTED|ALL))?\s+TREE\b/i;

function inferExplainMode(sqlText: string): "tree" | "plan" {
  if (EXPLAIN_TREE_PREFIX_RE.test(sqlText)) return "tree";
  return "plan";
}

function readExplainDatabaseFromSession(): string {
  if (typeof window === "undefined") return "";
  try {
    return window.sessionStorage.getItem(DORIS_EXPLAIN_DATABASE_KEY) ?? "";
  } catch {
    return "";
  }
}

function writeExplainDatabaseToSession(db: string): void {
  if (typeof window === "undefined") return;
  const trimmed = db.trim();
  try {
    if (!trimmed) window.sessionStorage.removeItem(DORIS_EXPLAIN_DATABASE_KEY);
    else window.sessionStorage.setItem(DORIS_EXPLAIN_DATABASE_KEY, trimmed);
  } catch {
    // Ignore storage quota / access errors.
  }
}

export interface UseExplainInputStateArgs {
  agent: AgentClient;
  dorisConn: DorisConnectionInput | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
  sql: string;
  onChangeSql: (sql: string) => void;
  onParseResult: (text: string) => void;
}

export interface ExplainInputState {
  database: string;
  setDatabase: (database: string) => void;
  databaseList: string[];
  databaseLoading: boolean;
  loading: boolean;
  error: string | null;
  onClearSql: () => void;
  refreshDatabases: () => Promise<void>;
  onRunExplain: () => Promise<void>;
}

export default function useExplainInputState(args: UseExplainInputStateArgs): ExplainInputState {
  const { agent, dorisConn, dorisConfigured, onOpenDoris, sql, onChangeSql, onParseResult } = args;

  const [database, setDatabase] = useState(() => readExplainDatabaseFromSession());
  const [databaseList, setDatabaseList] = useState<string[]>([]);
  const [databaseLoading, setDatabaseLoading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setDatabaseLoading(false);
    setDatabaseList([]);

    const defaultDatabase = dorisConn?.database?.trim();
    if (!defaultDatabase) return;

    setDatabase((prev) => {
      if (prev.trim()) return prev;
      return defaultDatabase;
    });
  }, [dorisConn]);

  useEffect(() => {
    writeExplainDatabaseToSession(database);
  }, [database]);

  const onClearSql = () => {
    setError(null);
    onChangeSql("");
  };

  const refreshDatabases = async () => {
    setError(null);
    if (!dorisConfigured || !dorisConn) {
      setError("Doris connection is not configured.");
      onOpenDoris();
      return;
    }

    setDatabaseLoading(true);
    try {
      const dbs = await agent.listDorisDatabases({ connection: dorisConn });
      setDatabaseList(dbs);
      const currentDb = database.trim();
      if (!currentDb) {
        setDatabase(dbs[0] ?? "");
      } else if (dbs.length > 0 && !dbs.includes(currentDb)) {
        setDatabase(dbs[0] ?? "");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDatabaseLoading(false);
    }
  };

  const onRunExplain = async () => {
    setError(null);
    if (!dorisConfigured || !dorisConn) {
      setError("Doris connection is not configured.");
      onOpenDoris();
      return;
    }

    const sqlText = sql.trim();
    if (!sqlText) {
      setError("SQL is required.");
      return;
    }

    setLoading(true);
    try {
      const dbName = database.trim();
      const conn = dbName ? { ...dorisConn, database: dbName } : dorisConn;
      const mode = inferExplainMode(sqlText);
      const text = await agent.explain({
        connection: conn,
        sql: sqlText,
        mode,
      });
      onParseResult(text);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  return {
    database,
    setDatabase,
    databaseList,
    databaseLoading,
    loading,
    error,
    onClearSql,
    refreshDatabases,
    onRunExplain,
  };
}
