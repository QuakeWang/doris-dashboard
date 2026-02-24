import { useCallback, useState } from "react";
import {
  type DorisConnectionInfo,
  type DorisConnectionInput,
  isDorisConnectionConfigured,
} from "../agent/agentClient";

const DORIS_SESSION_KEY = "doris.connection.info.v1";

function asObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

export function parseDorisSessionInfo(raw: string | null): DorisConnectionInfo | null {
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
  const v = asObject(parsed);
  if (!v || typeof v.host !== "string") return null;
  const port = Number(v.port ?? 0);
  if (!Number.isFinite(port) || port <= 0 || port > 65535) return null;
  const user = String(v.user ?? "");
  if (!user) return null;
  return { host: v.host, port, user };
}

export function formatDorisLabel(conn: DorisConnectionInput | null): string {
  return conn ? `${conn.user}@${conn.host}:${conn.port}` : "-";
}

function readDorisSessionInfo(): DorisConnectionInfo | null {
  if (typeof window === "undefined") return null;
  try {
    return parseDorisSessionInfo(window.sessionStorage.getItem(DORIS_SESSION_KEY));
  } catch {
    return null;
  }
}

function writeDorisSessionInfo(info: DorisConnectionInfo | null): void {
  if (typeof window === "undefined") return;
  try {
    if (!info) window.sessionStorage.removeItem(DORIS_SESSION_KEY);
    else window.sessionStorage.setItem(DORIS_SESSION_KEY, JSON.stringify(info));
  } catch {
    // Ignore storage quota / access errors.
  }
}

export function useDorisConnectionState() {
  const [dorisModalOpen, setDorisModalOpen] = useState(false);
  const [dorisImportOpen, setDorisImportOpen] = useState(false);
  const [dorisConn, setDorisConn] = useState<DorisConnectionInput | null>(() => {
    const info = readDorisSessionInfo();
    return info ? { ...info, password: "" } : null;
  });

  const openDorisModal = useCallback(() => setDorisModalOpen(true), []);
  const closeDorisModal = useCallback(() => setDorisModalOpen(false), []);
  const openDorisImport = useCallback(() => setDorisImportOpen(true), []);
  const closeDorisImport = useCallback(() => setDorisImportOpen(false), []);

  const saveDorisConnection = useCallback((conn: DorisConnectionInput, rememberInfo: boolean) => {
    setDorisConn(conn);
    writeDorisSessionInfo(
      rememberInfo ? { host: conn.host, port: conn.port, user: conn.user } : null
    );
  }, []);

  const dorisLabel = formatDorisLabel(dorisConn);
  const dorisConfigured = isDorisConnectionConfigured(dorisConn);

  return {
    dorisConn,
    dorisLabel,
    dorisConfigured,
    dorisModalOpen,
    dorisImportOpen,
    openDorisModal,
    closeDorisModal,
    openDorisImport,
    closeDorisImport,
    saveDorisConnection,
  };
}
