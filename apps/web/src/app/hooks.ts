import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import type { DbClient } from "../db/client/dbClient";
import type {
  OverviewResult,
  QueryFilters,
  ShareRankBy,
  ShareRow,
  TopSqlRow,
} from "../db/client/protocol";
import type { AsyncData } from "../utils/useAsync";
import { useAsyncData } from "../utils/useAsync";
import type { AuditTabKey } from "./diagnosticsNavigation";

export function toErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function useDuckDbSession(
  client: DbClient,
  setError: (value: string | null) => void
): {
  ready: boolean;
  datasetId: string | null;
  retryInit: () => void;
  retryCreateDataset: () => void;
} {
  const [ready, setReady] = useState(false);
  const [datasetId, setDatasetId] = useState<string | null>(null);

  const [initToken, bumpInitToken] = useReducer((x: number) => x + 1, 0);
  const [createToken, bumpCreateToken] = useReducer((x: number) => x + 1, 0);
  const initSeqRef = useRef(0);
  const createSeqRef = useRef(0);

  const retryInit = useCallback(() => {
    setError(null);
    setReady(false);
    bumpInitToken();
  }, [setError]);

  const retryCreateDataset = useCallback(() => {
    setError(null);
    bumpCreateToken();
  }, [setError]);

  useEffect(() => {
    const seq = ++initSeqRef.current;
    client
      .init()
      .then(() => {
        if (seq !== initSeqRef.current) return;
        setError(null);
        setReady(true);
      })
      .catch((e) => {
        if (seq !== initSeqRef.current) return;
        const message = toErrorMessage(e);
        if (message.toLowerCase().includes("timed out") && message.includes("type=init")) {
          setError("Local analysis initialization timed out. Click Retry or reload the page.");
          return;
        }
        setError(message);
      });
  }, [client, initToken, setError]);

  useEffect(() => {
    if (!ready || datasetId) return;
    const seq = ++createSeqRef.current;
    client
      .createDataset("session")
      .then((r) => {
        if (seq !== createSeqRef.current) return;
        setDatasetId(r.datasetId);
      })
      .catch((e) => {
        if (seq !== createSeqRef.current) return;
        setError(toErrorMessage(e));
      });
  }, [client, createToken, datasetId, ready, setError]);

  return { ready, datasetId, retryInit, retryCreateDataset };
}

export function useDatasetQueries(params: {
  client: DbClient;
  datasetId: string | null;
  importing: boolean;
  activeTab: AuditTabKey | null;
  filters: QueryFilters;
  shareTopN: number;
  shareRankBy: ShareRankBy;
  setError: (value: string | null) => void;
}): {
  overviewQuery: AsyncData<OverviewResult | null>;
  topSqlQuery: AsyncData<TopSqlRow[]>;
  shareQuery: AsyncData<ShareRow[]>;
} {
  const { client, datasetId, importing, activeTab, filters, shareTopN, shareRankBy, setError } =
    params;

  const canQuery = !!datasetId && !importing;
  const queryOptions = { keepDataOnError: true, onError: setError };

  const runDatasetQuery = <T>(fallback: T, fn: (id: string) => Promise<T>): Promise<T> => {
    if (!datasetId) return Promise.resolve(fallback);
    setError(null);
    return fn(datasetId);
  };

  const overviewQuery = useAsyncData<OverviewResult | null>(
    activeTab === "overview" && canQuery,
    () => runDatasetQuery(null, (id) => client.queryOverview(id, filters)),
    [activeTab, client, datasetId, filters, importing],
    null,
    queryOptions
  );

  const topSqlQuery = useAsyncData<TopSqlRow[]>(
    activeTab === "topSql" && canQuery,
    () => runDatasetQuery([], (id) => client.queryTopSql(id, 50, filters)),
    [activeTab, client, datasetId, filters, importing],
    [],
    queryOptions
  );

  const shareQuery = useAsyncData<ShareRow[]>(
    activeTab === "share" && canQuery,
    () => runDatasetQuery([], (id) => client.queryShare(id, shareTopN, shareRankBy, filters)),
    [activeTab, client, datasetId, filters, importing, shareRankBy, shareTopN],
    [],
    queryOptions
  );

  return { overviewQuery, topSqlQuery, shareQuery };
}
