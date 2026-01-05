import type {
  DimensionKey,
  DimensionRankBy,
  DimensionTopRow,
  ImportProgress,
  OverviewResult,
  QueryFilters,
  QuerySampleRow,
  SampleOrderBy,
  ShareRow,
  TemplateSeriesResult,
  TopSqlRow,
  WorkerRequest,
  WorkerResponse,
} from "./protocol";

type WithoutRequestId<T> = T extends { requestId: string } ? Omit<T, "requestId"> : never;
type WorkerRequestWithoutId = WithoutRequestId<WorkerRequest>;

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
};

function getOrCreateTabSessionId(): string {
  const fallback = crypto.randomUUID();
  if (typeof window === "undefined") return fallback;
  try {
    const s = window.sessionStorage;
    const existing = s.getItem("dd_tab_session_id");
    if (existing) return existing;
    s.setItem("dd_tab_session_id", fallback);
  } catch {
    // ignore
  }
  return fallback;
}

export class DbClient {
  private worker: Worker;
  private pending = new Map<string, PendingRequest>();
  private importProgressHandlers = new Map<string, (p: ImportProgress) => void>();
  private tabSessionId: string;

  constructor() {
    this.tabSessionId = getOrCreateTabSessionId();
    this.worker = new Worker(new URL("../worker/duckdb.worker.ts", import.meta.url), {
      type: "module",
    });
    this.worker.onmessage = (ev: MessageEvent<WorkerResponse>) => {
      const msg = ev.data;
      if (msg.type === "event") {
        if (msg.event.type === "importProgress") {
          const handler = this.importProgressHandlers.get(msg.event.requestId);
          if (handler) handler(msg.event.progress);
        }
        if (msg.event.type === "log") {
          console.debug(`[duckdb.worker] ${msg.event.message}`);
        }
        return;
      }
      const pending = this.pending.get(msg.requestId);
      if (!pending) return;
      this.pending.delete(msg.requestId);
      if (msg.ok) pending.resolve(msg.result);
      else pending.reject(new Error(msg.error.message));
    };
  }

  async init(): Promise<void> {
    await this.request({ type: "init", tabSessionId: this.tabSessionId });
  }

  async createDataset(name: string): Promise<{ datasetId: string }> {
    return await this.request<{ datasetId: string }>({ type: "createDataset", name });
  }

  async importAuditLog(
    datasetId: string,
    file: File,
    onProgress: (p: ImportProgress) => void
  ): Promise<void> {
    const requestId = this.newRequestId();
    this.importProgressHandlers.set(requestId, onProgress);
    try {
      await this.request({ type: "importAuditLog", datasetId, file }, requestId);
    } finally {
      this.importProgressHandlers.delete(requestId);
    }
  }

  async queryTopSql(datasetId: string, topN: number, filters: QueryFilters): Promise<TopSqlRow[]> {
    return await this.request<TopSqlRow[]>({ type: "queryTopSql", datasetId, topN, filters });
  }

  async queryOverview(datasetId: string, filters: QueryFilters): Promise<OverviewResult> {
    return await this.request<OverviewResult>({ type: "queryOverview", datasetId, filters });
  }

  async queryShare(
    datasetId: string,
    topN: number,
    rankBy: "totalCpuMs" | "totalTimeMs",
    filters: QueryFilters
  ): Promise<ShareRow[]> {
    return await this.request<ShareRow[]>({ type: "queryShare", datasetId, topN, rankBy, filters });
  }

  async querySamples(
    datasetId: string,
    templateHash: string,
    limit: number,
    orderBy: SampleOrderBy,
    filters: QueryFilters
  ): Promise<QuerySampleRow[]> {
    return await this.request<QuerySampleRow[]>({
      type: "querySamples",
      datasetId,
      templateHash,
      limit,
      orderBy,
      filters,
    });
  }

  async queryTemplateSeries(
    datasetId: string,
    templateHash: string,
    bucketSeconds: number,
    filters: QueryFilters
  ): Promise<TemplateSeriesResult> {
    return await this.request<TemplateSeriesResult>({
      type: "queryTemplateSeries",
      datasetId,
      templateHash,
      bucketSeconds,
      filters,
    });
  }

  async queryDimensionTop(
    datasetId: string,
    templateHash: string,
    dimension: DimensionKey,
    topN: number,
    rankBy: DimensionRankBy,
    filters: QueryFilters
  ): Promise<DimensionTopRow[]> {
    return await this.request<DimensionTopRow[]>({
      type: "queryDimensionTop",
      datasetId,
      templateHash,
      dimension,
      topN,
      rankBy,
      filters,
    });
  }

  async cancelCurrentTask(): Promise<void> {
    await this.request({ type: "cancel" });
  }

  private async request<T = unknown>(req: WorkerRequestWithoutId, requestId?: string): Promise<T> {
    const id = requestId ?? this.newRequestId();
    const msg: WorkerRequest = { ...(req as WorkerRequest), requestId: id };

    const promise = new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: resolve as unknown as PendingRequest["resolve"],
        reject: reject as unknown as PendingRequest["reject"],
      });
    });
    this.worker.postMessage(msg);
    return promise;
  }

  private newRequestId(): string {
    return crypto.randomUUID();
  }
}
