import { useCallback, useEffect, useRef, useState } from "react";
import type { DbClient } from "../db/client/dbClient";
import type { OverviewResult, QueryFilters, ShareRow, TopSqlRow } from "../db/client/protocol";
import type { AsyncData } from "../utils/useAsync";
import { useAsyncData } from "../utils/useAsync";

export type TabKey = "overview" | "topSql" | "share";

export function toErrorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function parseTabFromSearch(search: string): TabKey | null {
  const tab = new URLSearchParams(search).get("tab");
  return tab === "overview" || tab === "topSql" || tab === "share" ? tab : null;
}

function pushTabToUrl(tab: TabKey): void {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  url.searchParams.set("tab", tab);
  window.history.pushState({}, "", url.toString());
}

export function useTabState(defaultTab: TabKey = "overview"): {
  activeTab: TabKey;
  setTab: (tab: TabKey) => void;
} {
  const [activeTab, setActiveTab] = useState<TabKey>(defaultTab);

  const setTab = useCallback((tab: TabKey) => {
    setActiveTab(tab);
    pushTabToUrl(tab);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const syncTabFromUrl = () => {
      const tab = parseTabFromSearch(window.location.search);
      if (tab) setActiveTab(tab);
    };
    syncTabFromUrl();
    window.addEventListener("popstate", syncTabFromUrl);
    return () => window.removeEventListener("popstate", syncTabFromUrl);
  }, []);

  return { activeTab, setTab };
}

export function useDuckDbSession(
  client: DbClient,
  setError: (value: string | null) => void
): {
  ready: boolean;
  datasetId: string | null;
  createDataset: (name: string) => Promise<string>;
} {
  const [ready, setReady] = useState(false);
  const [datasetId, setDatasetId] = useState<string | null>(null);

  const createSeqRef = useRef(0);

  const createDataset = useCallback(
    async (name: string) => {
      const seq = ++createSeqRef.current;
      const r = await client.createDataset(name);
      if (seq === createSeqRef.current) setDatasetId(r.datasetId);
      return r.datasetId;
    },
    [client]
  );

  useEffect(() => {
    client
      .init()
      .then(() => setReady(true))
      .catch((e) => setError(toErrorMessage(e)));
  }, [client, setError]);

  useEffect(() => {
    if (!ready || datasetId) return;
    createDataset("session").catch((e) => setError(toErrorMessage(e)));
  }, [createDataset, datasetId, ready, setError]);

  return { ready, datasetId, createDataset };
}

export function useDatasetQueries(params: {
  client: DbClient;
  datasetId: string | null;
  importing: boolean;
  activeTab: TabKey;
  filters: QueryFilters;
  shareTopN: number;
  shareRankBy: "totalCpuMs" | "totalTimeMs";
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
