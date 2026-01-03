import type { WorkerRequest } from "../client/protocol";
import { ensureDb } from "./engine";
import { handleCancel, handleImportAuditLog } from "./importAuditLog";
import { fail, reply } from "./messaging";
import {
  handleQueryDimensionTop,
  handleQueryOverview,
  handleQuerySamples,
  handleQueryShare,
  handleQueryTemplateSeries,
  handleQueryTopSql,
} from "./queryHandlers";

self.onmessage = async (ev: MessageEvent<WorkerRequest>) => {
  const msg = ev.data;
  try {
    switch (msg.type) {
      case "init":
        await ensureDb();
        return reply({
          type: "response",
          requestId: msg.requestId,
          ok: true,
          result: { ok: true },
        });
      case "createDataset": {
        await ensureDb();
        const datasetId = crypto.randomUUID();
        return reply({
          type: "response",
          requestId: msg.requestId,
          ok: true,
          result: { datasetId },
        });
      }
      case "importAuditLog":
        return await handleImportAuditLog(msg.requestId, msg.datasetId, msg.file);
      case "queryOverview":
        return await handleQueryOverview(msg.requestId, msg.datasetId, msg.filters);
      case "queryTopSql":
        return await handleQueryTopSql(msg.requestId, msg.datasetId, msg.topN, msg.filters);
      case "queryShare":
        return await handleQueryShare(
          msg.requestId,
          msg.datasetId,
          msg.topN,
          msg.rankBy,
          msg.filters
        );
      case "querySamples":
        return await handleQuerySamples(
          msg.requestId,
          msg.datasetId,
          msg.templateHash,
          msg.limit,
          msg.orderBy,
          msg.filters
        );
      case "queryTemplateSeries":
        return await handleQueryTemplateSeries(
          msg.requestId,
          msg.datasetId,
          msg.templateHash,
          msg.bucketSeconds,
          msg.filters
        );
      case "queryDimensionTop":
        return await handleQueryDimensionTop(
          msg.requestId,
          msg.datasetId,
          msg.templateHash,
          msg.dimension,
          msg.topN,
          msg.rankBy,
          msg.filters
        );
      case "cancel":
        return await handleCancel(msg.requestId);
      default:
        throw new Error(`Unsupported request type: ${(msg as WorkerRequest).type}`);
    }
  } catch (e) {
    fail(msg.requestId, e);
  }
};
