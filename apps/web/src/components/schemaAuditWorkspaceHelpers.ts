import type { DorisConnectionInput, SchemaAuditFinding } from "../agent/agentClient";

export function isSchemaAuditBucketFinding(ruleId: string): boolean {
  return ruleId.startsWith("SA-B");
}

export function splitSchemaAuditFindings(findings: SchemaAuditFinding[] | undefined): {
  bucket: SchemaAuditFinding[];
  nonBucket: SchemaAuditFinding[];
} {
  if (!findings || findings.length === 0) {
    return { bucket: [], nonBucket: [] };
  }
  const bucket: SchemaAuditFinding[] = [];
  const nonBucket: SchemaAuditFinding[] = [];
  for (let i = 0; i < findings.length; i++) {
    const finding = findings[i];
    if (isSchemaAuditBucketFinding(finding.ruleId)) {
      bucket.push(finding);
      continue;
    }
    nonBucket.push(finding);
  }
  return { bucket, nonBucket };
}

export function buildSchemaAuditConnectionKey(conn: DorisConnectionInput | null): string {
  if (!conn) return "";
  return `${conn.user}@${conn.host}:${conn.port}/${conn.database ?? ""}#${conn.password}`;
}

export function formatSchemaAuditEvidence(evidence: Record<string, unknown>): string {
  try {
    const raw = JSON.stringify(evidence, null, 2) ?? "";
    if (raw.length <= 2400) return raw;
    return `${raw.slice(0, 2400)}\n...`;
  } catch {
    return String(evidence);
  }
}
