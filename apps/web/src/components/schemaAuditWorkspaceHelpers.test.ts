import { describe, expect, it } from "vitest";
import type { SchemaAuditFinding } from "../agent/agentClient";
import {
  buildSchemaAuditConnectionKey,
  formatSchemaAuditEvidence,
  splitSchemaAuditFindings,
} from "./schemaAuditWorkspaceHelpers";

describe("buildSchemaAuditConnectionKey", () => {
  it("changes key when password changes", () => {
    const base = {
      host: "127.0.0.1",
      port: 9030,
      user: "root",
      database: "db1",
    };
    const keyA = buildSchemaAuditConnectionKey({ ...base, password: "pwd-a" });
    const keyB = buildSchemaAuditConnectionKey({ ...base, password: "pwd-b" });

    expect(keyA).not.toBe(keyB);
  });
});

describe("splitSchemaAuditFindings", () => {
  it("splits bucket and non-bucket findings while preserving order", () => {
    const findings: SchemaAuditFinding[] = [
      {
        ruleId: "SA-E001",
        severity: "warn",
        confidence: 0.9,
        summary: "Empty partitions are high",
        evidence: {},
      },
      {
        ruleId: "SA-B003",
        severity: "warn",
        confidence: 0.8,
        summary: "Bucket jump detected",
        evidence: {},
      },
      {
        ruleId: "SA-D004",
        severity: "warn",
        confidence: 0.85,
        summary: "Dynamic window mismatch",
        evidence: {},
      },
    ];

    const out = splitSchemaAuditFindings(findings);
    expect(out.bucket.map((finding) => finding.ruleId)).toEqual(["SA-B003"]);
    expect(out.nonBucket.map((finding) => finding.ruleId)).toEqual(["SA-E001", "SA-D004"]);
  });
});

describe("formatSchemaAuditEvidence", () => {
  it("truncates very long evidence payload", () => {
    const out = formatSchemaAuditEvidence({ payload: "x".repeat(2600) });
    expect(out.endsWith("\n...")).toBe(true);
  });
});
