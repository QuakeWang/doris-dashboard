package doris

import (
	"strings"
	"testing"
)

func assertSchemaAuditQueryContains(t *testing.T, query string, fragments ...string) {
	t.Helper()
	for i := range fragments {
		fragment := fragments[i]
		if !strings.Contains(query, fragment) {
			t.Fatalf("query should contain %q: %s", fragment, query)
		}
	}
}

func assertSchemaAuditQueryNotContains(t *testing.T, query string, fragments ...string) {
	t.Helper()
	for i := range fragments {
		fragment := fragments[i]
		if strings.Contains(query, fragment) {
			t.Fatalf("query should not contain %q: %s", fragment, query)
		}
	}
}

func TestBuildSchemaAuditScanQueryWithoutDynamicProperties(t *testing.T) {
	t.Parallel()

	query := buildSchemaAuditScanQuery("", false, 123)
	assertSchemaAuditQueryContains(
		t,
		query,
		"WITH candidates AS (",
		"INNER JOIN candidates c ON c.table_schema = p.table_schema",
		"'' AS dynamic_partition_enable",
		"ORDER BY t.table_schema, t.table_name LIMIT 123",
		"CASE WHEN COALESCE(ps.partition_count, 0) > 0",
		"COALESCE(ps.empty_partition_count, 0) DESC",
	)
	assertSchemaAuditQueryNotContains(
		t,
		query,
		"information_schema.table_properties",
		"LOWER(COALESCE(dp.dynamic_partition_enable, ''))",
		"ORDER BY candidates.table_schema, candidates.table_name LIMIT 123",
	)
}

func TestBuildSchemaAuditScanQueryWithDynamicProperties(t *testing.T) {
	t.Parallel()

	query := buildSchemaAuditScanQuery("", true, 0)
	assertSchemaAuditQueryContains(
		t,
		query,
		"information_schema.table_properties",
		"dynamic_properties AS (",
		"LEFT JOIN dynamic_properties dp",
		"COALESCE(dp.dynamic_partition_enable, '') AS dynamic_partition_enable",
		"dynamic_partition_time_unit",
		"LOWER(COALESCE(dp.dynamic_partition_enable, ''))",
	)
	assertSchemaAuditQueryNotContains(t, query, " LIMIT ")
}

func TestResolveSchemaAuditScanLimit(t *testing.T) {
	t.Parallel()

	defaultLimit := resolveSchemaAuditScanLimit(SchemaAuditScanOptions{})
	if defaultLimit != schemaAuditScanLimitDefault {
		t.Fatalf("unexpected default scan limit: %d", defaultLimit)
	}

	filteredLimit := resolveSchemaAuditScanLimit(
		SchemaAuditScanOptions{Database: "db1"},
	)
	if filteredLimit != schemaAuditScanLimitFiltered {
		t.Fatalf("unexpected filtered scan limit: %d", filteredLimit)
	}
}

func TestParseSchemaAuditPartitionRangeLowerBound(t *testing.T) {
	t.Parallel()

	lower := parseSchemaAuditPartitionRangeLowerBound(
		"[types: [DATETIMEV2]; keys: [2026-01-12 00:00:00]; ..types: [DATETIMEV2]; keys: [2026-01-13 00:00:00]; )",
	)
	if lower != "2026-01-12 00:00:00" {
		t.Fatalf("unexpected range lower bound: %q", lower)
	}

	if parsed := parseSchemaAuditPartitionRangeLowerBound("invalid range payload"); parsed != "" {
		t.Fatalf("expected empty lower bound on invalid payload, got %q", parsed)
	}

	compositeLower := parseSchemaAuditPartitionRangeLowerBound(
		"[types: [DATETIMEV2, INT]; keys: [2026-01-12 00:00:00, 100]; ..types: [DATETIMEV2, INT]; keys: [2026-01-13 00:00:00, 200]; )",
	)
	if compositeLower != "2026-01-12 00:00:00" {
		t.Fatalf("unexpected composite range lower bound: %q", compositeLower)
	}
}

func TestPaginateSchemaAuditItemsReturnsEmptySlice(t *testing.T) {
	t.Parallel()

	empty := paginateSchemaAuditItems(nil, 1, 50)
	if empty == nil || len(empty) != 0 {
		t.Fatalf("expected non-nil empty slice, got %+v", empty)
	}

	outOfRange := paginateSchemaAuditItems(
		[]SchemaAuditScanItem{
			{Database: "db1", Table: "tbl1"},
		},
		2,
		50,
	)
	if outOfRange == nil || len(outOfRange) != 0 {
		t.Fatalf("expected non-nil empty slice for out of range page, got %+v", outOfRange)
	}
}

func TestEvaluateSchemaAuditScanFindingsMarksFutureUncertainty(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditScanFindings(
		schemaAuditPartitionSummary{
			PartitionCount:      20,
			EmptyPartitionCount: 10,
		},
		map[string]string{
			"dynamic_partition.enable": "true",
			"dynamic_partition.start":  "-45",
			"dynamic_partition.end":    "10",
		},
	)
	if !hasSchemaAuditRule(findings, "SA-E001") {
		t.Fatalf("expected SA-E001, got %+v", findings)
	}
	for i := range findings {
		if findings[i].RuleID == "SA-E001" {
			if findings[i].Confidence >= 0.95 {
				t.Fatalf("expected reduced confidence for uncertain future partitions, got %+v", findings[i])
			}
		}
	}
}

func TestEvaluateSchemaAuditScanFindingsStillDetectsNonFutureEmptyRisk(t *testing.T) {
	t.Parallel()

	findings := evaluateSchemaAuditScanFindings(
		schemaAuditPartitionSummary{
			PartitionCount:      30,
			EmptyPartitionCount: 25,
		},
		map[string]string{
			"dynamic_partition.enable": "true",
			"dynamic_partition.start":  "-45",
			"dynamic_partition.end":    "10",
		},
	)
	if !hasSchemaAuditRule(findings, "SA-E001") {
		t.Fatalf("expected SA-E001, got %+v", findings)
	}
	if !hasSchemaAuditRule(findings, "SA-D004") {
		t.Fatalf("expected SA-D004, got %+v", findings)
	}
}
