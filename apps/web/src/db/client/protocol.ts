export interface QueryFilters {
  startMs?: number;
  endMs?: number;
  excludeInternal?: boolean;
  userName?: string;
  dbName?: string;
  clientIp?: string;
  state?: string;
}

export type WorkerRequest =
  | { type: "init"; requestId: string }
  | { type: "createDataset"; requestId: string; name: string }
  | { type: "importAuditLog"; requestId: string; datasetId: string; file: File }
  | {
      type: "queryOverview";
      requestId: string;
      datasetId: string;
      filters: QueryFilters;
    }
  | {
      type: "queryTopSql";
      requestId: string;
      datasetId: string;
      topN: number;
      filters: QueryFilters;
    }
  | {
      type: "queryShare";
      requestId: string;
      datasetId: string;
      topN: number;
      rankBy: "totalCpuMs" | "totalTimeMs";
      filters: QueryFilters;
    }
  | {
      type: "querySamples";
      requestId: string;
      datasetId: string;
      templateHash: string;
      limit: number;
      orderBy: SampleOrderBy;
      filters: QueryFilters;
    }
  | {
      type: "queryTemplateSeries";
      requestId: string;
      datasetId: string;
      templateHash: string;
      bucketSeconds: number;
      filters: QueryFilters;
    }
  | {
      type: "queryDimensionTop";
      requestId: string;
      datasetId: string;
      templateHash: string;
      dimension: DimensionKey;
      topN: number;
      rankBy: DimensionRankBy;
      filters: QueryFilters;
    }
  | {
      type: "cancel";
      requestId: string;
    };

export type WorkerEvent =
  | { type: "importProgress"; requestId: string; progress: ImportProgress }
  | { type: "log"; message: string };

export type WorkerResponse =
  | { type: "response"; requestId: string; ok: true; result: unknown }
  | { type: "response"; requestId: string; ok: false; error: { message: string; stack?: string } }
  | { type: "event"; event: WorkerEvent };

export interface ImportProgress {
  bytesRead: number;
  bytesTotal: number;
  recordsParsed: number;
  recordsInserted: number;
  badRecords: number;
}

export interface TopSqlRow {
  templateHash: string;
  template: string;
  tableGuess: string | null;
  execCount: number;
  totalCpuMs: number;
  totalTimeMs: number;
  avgTimeMs: number;
  maxTimeMs: number;
  p95TimeMs: number | null;
}

export interface OverviewResult {
  records: number;
  totalCpuMs: number;
  totalTimeMs: number;
  failedCount: number;
  p95TimeMs: number | null;
  p99TimeMs: number | null;
  minTimeMs: number | null;
  maxTimeMs: number | null;

  approxDistinctUsers: number;
  approxDistinctDbs: number;
  approxDistinctClientIps: number;
  approxDistinctStrippedTemplates: number;
  topDbsByCpu: DbTopRow[];
  topTablesByCpu: TableTopRow[];
  topUsersByCpu: DimensionTopRow[];
  topClientIpsByCpu: DimensionTopRow[];
}

export interface ShareRow {
  templateHash: string;
  template: string;
  execCount: number;
  totalCpuMs: number;
  totalTimeMs: number;
  cpuShare: number;
  timeShare: number;
  isOthers: boolean;
}

export interface DbTopRow {
  dbName: string;
  execCount: number;
  totalCpuMs: number;
  totalTimeMs: number;
}

export interface TableTopRow {
  dbName: string;
  tableName: string;
  execCount: number;
  totalCpuMs: number;
  totalTimeMs: number;
}

export type SampleOrderBy = "queryTimeMs" | "cpuTimeMs";

export interface QuerySampleRow {
  recordId: number;
  eventTimeMs: number | null;
  queryId: string | null;
  userName: string | null;
  dbName: string | null;
  clientIp: string | null;
  state: string | null;
  queryTimeMs: number | null;
  cpuTimeMs: number | null;
  scanBytes: number | null;
  scanRows: number | null;
  returnRows: number | null;
  stmtRaw: string | null;
}

export interface TemplateSeriesResult {
  bucketSeconds: number;
  bucketStarts: string[];
  execCounts: number[];
  totalCpuMs: number[];
  totalTimeMs: number[];
}

export type DimensionKey = "userName" | "dbName" | "clientIp";
export type DimensionRankBy = "totalCpuMs" | "totalTimeMs" | "execCount";

export interface DimensionTopRow {
  name: string;
  execCount: number;
  totalCpuMs: number;
  totalTimeMs: number;
}
