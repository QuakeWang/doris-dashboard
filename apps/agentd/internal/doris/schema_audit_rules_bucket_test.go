package doris

import "testing"

const testSchemaAuditGB = 1024 * 1024 * 1024

func TestEvaluateSchemaAuditBucketFindingsTooSmall(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p20260224",
				Rows:          100000,
				DataSizeBytes: 50 * testSchemaAuditGB,
				Buckets:       1,
			},
		},
		"CREATE TABLE t (...) DISTRIBUTED BY HASH(`id`) BUCKETS 1",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B001") {
		t.Fatalf("expected SA-B001, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketFindingsTooLarge(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p20260224",
				Rows:          100000,
				DataSizeBytes: 1 * testSchemaAuditGB,
				Buckets:       10,
			},
		},
		"CREATE TABLE t (...) DISTRIBUTED BY HASH(`id`) BUCKETS 10",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B002") {
		t.Fatalf("expected SA-B002, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketJumpFinding(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketJumpFinding(
		[]SchemaAuditPartition{
			{Name: "p1", Buckets: 2},
			{Name: "p2", Buckets: 8},
			{Name: "p3", Buckets: 8},
		},
		schemaAuditCreateTableDescriptor{
			DistributionType: "hash",
			AutoBucket:       true,
		},
		0.5,
	)

	if !hasSchemaAuditRule(findings, "SA-B003") {
		t.Fatalf("expected SA-B003, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketJumpFindingUsesRangeLowerOrder(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketJumpFinding(
		[]SchemaAuditPartition{
			{
				Name:       "z_old",
				RangeLower: "2026-01-01 00:00:00",
				Buckets:    2,
			},
			{
				Name:       "b_mid",
				RangeLower: "2026-01-02 00:00:00",
				Buckets:    4,
			},
			{
				Name:       "a_new",
				RangeLower: "2026-01-03 00:00:00",
				Buckets:    3,
			},
		},
		schemaAuditCreateTableDescriptor{
			DistributionType: "hash",
			AutoBucket:       true,
		},
		0.5,
	)

	if !hasSchemaAuditRule(findings, "SA-B003") {
		t.Fatalf("expected SA-B003 when range lower ordering detects jump, got %+v", findings)
	}
	for i := range findings {
		if findings[i].RuleID != "SA-B003" {
			continue
		}
		source, _ := findings[i].Evidence["orderSource"].(string)
		if source != "range_lower" {
			t.Fatalf("expected range_lower order source, got %+v", findings[i].Evidence)
		}
	}
}

func TestEvaluateSchemaAuditBucketJumpFindingUsesPartialRangeLowerOrder(t *testing.T) {
	t.Parallel()

	partialFindings := evaluateSchemaAuditBucketJumpFinding(
		[]SchemaAuditPartition{
			{
				Name:    "manual_anchor",
				Buckets: 1,
			},
			{
				Name:       "a_new",
				RangeLower: "2026-01-03 00:00:00",
				Buckets:    3,
			},
			{
				Name:       "z_old",
				RangeLower: "2026-01-01 00:00:00",
				Buckets:    2,
			},
			{
				Name:       "b_mid",
				RangeLower: "2026-01-02 00:00:00",
				Buckets:    8,
			},
		},
		schemaAuditCreateTableDescriptor{
			DistributionType: "hash",
			AutoBucket:       true,
		},
		0.5,
	)

	partialFinding, ok := schemaAuditFindingByRule(partialFindings, "SA-B003")
	if !ok {
		t.Fatalf("expected SA-B003 when partial range lower ordering detects jump, got %+v", partialFindings)
	}
	source, _ := partialFinding.Evidence["orderSource"].(string)
	if source != "range_lower_partial" {
		t.Fatalf("expected range_lower_partial order source, got %+v", partialFinding.Evidence)
	}

	fullFindings := evaluateSchemaAuditBucketJumpFinding(
		[]SchemaAuditPartition{
			{
				Name:       "manual_anchor",
				RangeLower: "2025-12-31 00:00:00",
				Buckets:    1,
			},
			{
				Name:       "a_new",
				RangeLower: "2026-01-03 00:00:00",
				Buckets:    3,
			},
			{
				Name:       "z_old",
				RangeLower: "2026-01-01 00:00:00",
				Buckets:    2,
			},
			{
				Name:       "b_mid",
				RangeLower: "2026-01-02 00:00:00",
				Buckets:    8,
			},
		},
		schemaAuditCreateTableDescriptor{
			DistributionType: "hash",
			AutoBucket:       true,
		},
		0.5,
	)
	fullFinding, ok := schemaAuditFindingByRule(fullFindings, "SA-B003")
	if !ok {
		t.Fatalf("expected SA-B003 in full range-lower scenario, got %+v", fullFindings)
	}
	if partialFinding.Confidence >= fullFinding.Confidence {
		t.Fatalf(
			"expected partial-range confidence < full-range confidence, partial=%v full=%v",
			partialFinding.Confidence,
			fullFinding.Confidence,
		)
	}
}

func TestEvaluateSchemaAuditBucketFindingsRandomOnUnique(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p20260224",
				Rows:          100000,
				DataSizeBytes: 1 * testSchemaAuditGB,
				Buckets:       2,
			},
		},
		"CREATE TABLE `t` (`k1` bigint) ENGINE=OLAP UNIQUE KEY(`k1`) DISTRIBUTED BY RANDOM BUCKETS 2",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B005") {
		t.Fatalf("expected SA-B005, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketFindingsHashKeyMismatch(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p20260224",
				Rows:          100000,
				DataSizeBytes: 1 * testSchemaAuditGB,
				Buckets:       2,
			},
		},
		"CREATE TABLE `t` (`k1` bigint, `k2` bigint) ENGINE=OLAP UNIQUE KEY(`k1`) DISTRIBUTED BY HASH(`k2`) BUCKETS 2",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B006") {
		t.Fatalf("expected SA-B006, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketFindingsMetadataInsufficient(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p1",
				Rows:          0,
				DataSizeBytes: 0,
				Buckets:       0,
				Empty:         true,
			},
			{
				Name:          "p2",
				Rows:          0,
				DataSizeBytes: 0,
				Buckets:       0,
				Empty:         true,
			},
		},
		"CREATE TABLE t (...) DISTRIBUTED BY HASH(`id`) BUCKETS 1",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B004") {
		t.Fatalf("expected SA-B004, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketFindingsTabletSizeOutOfRange(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p1",
				Rows:          100000,
				DataSizeBytes: 10 * testSchemaAuditGB,
				Buckets:       40,
			},
		},
		"CREATE TABLE t (...) DISTRIBUTED BY HASH(`id`) BUCKETS 40",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B007") {
		t.Fatalf("expected SA-B007, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditBucketFindingsEmitsB009(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditBucketFindings(
		[]SchemaAuditPartition{
			{
				Name:          "p20260224",
				Rows:          100000,
				DataSizeBytes: 50 * testSchemaAuditGB,
				Buckets:       1,
			},
		},
		"CREATE TABLE t (...) DISTRIBUTED BY HASH(`id`) BUCKETS 1",
		defaultSchemaAuditBucketRuleConfig(),
	)

	if !hasSchemaAuditRule(findings, "SA-B009") {
		t.Fatalf("expected SA-B009, got %+v", findings)
	}
}

func TestParseSchemaAuditCreateTableDescriptor(t *testing.T) {
	t.Parallel()

	createTableSQL := `
CREATE TABLE ` + "`t`" + ` (
  ` + "`id`" + ` bigint NOT NULL,
  ` + "`ts`" + ` datetime NOT NULL
) ENGINE=OLAP
UNIQUE KEY(` + "`id`" + `, ` + "`ts`" + `)
DISTRIBUTED BY HASH(` + "`id`" + `) BUCKETS AUTO
PROPERTIES ("replication_num" = "1");`

	descriptor := parseSchemaAuditCreateTableDescriptor(createTableSQL)
	if descriptor.KeysType != "unique" {
		t.Fatalf("unexpected keys type: %s", descriptor.KeysType)
	}
	if descriptor.DistributionType != "hash" {
		t.Fatalf("unexpected distribution type: %s", descriptor.DistributionType)
	}
	if !descriptor.AutoBucket {
		t.Fatalf("expected auto bucket")
	}
	if len(descriptor.KeyColumns) != 2 || descriptor.KeyColumns[0] != "id" || descriptor.KeyColumns[1] != "ts" {
		t.Fatalf("unexpected key columns: %+v", descriptor.KeyColumns)
	}
	if len(descriptor.DistributionColumns) != 1 || descriptor.DistributionColumns[0] != "id" {
		t.Fatalf("unexpected distribution columns: %+v", descriptor.DistributionColumns)
	}
}

func hasSchemaAuditRule(findings []SchemaAuditFinding, ruleID string) bool {
	for i := range findings {
		if findings[i].RuleID == ruleID {
			return true
		}
	}
	return false
}

func schemaAuditFindingByRule(findings []SchemaAuditFinding, ruleID string) (SchemaAuditFinding, bool) {
	for i := range findings {
		if findings[i].RuleID == ruleID {
			return findings[i], true
		}
	}
	return SchemaAuditFinding{}, false
}
