package doris

import (
	"fmt"
	"testing"
	"time"
)

func TestEvaluateSchemaAuditFindingsExcludesFutureTailForE002(t *testing.T) {
	t.Parallel()

	partitions := make([]SchemaAuditPartition, 0, 16)
	for i := 1; i <= 8; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p200001%02d", i),
			Empty: false,
		})
	}
	for i := 1; i <= 8; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p209901%02d", i),
			Empty: true,
		})
	}

	findings := evaluateSchemaAuditFindings(partitions, map[string]string{
		"dynamic_partition.enable":            "true",
		"dynamic_partition.end":               "8",
		"dynamic_partition.time_unit":         "DAY",
		"dynamic_partition.prefix":            "p",
		"dynamic_partition.time_zone":         "Asia/Shanghai",
		"dynamic_partition.start":             "-7",
		"dynamic_partition.start_day_of_week": "1",
	})
	if hasSchemaAuditRule(findings, "SA-E002") {
		t.Fatalf("expected SA-E002 to be suppressed by future-tail exclusion, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditFindingsKeepsE002WhenNonFutureTailIsLong(t *testing.T) {
	t.Parallel()

	partitions := make([]SchemaAuditPartition, 0, 28)
	for i := 1; i <= 20; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p200001%02d", i),
			Empty: i > 10,
		})
	}
	for i := 1; i <= 8; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p209901%02d", i),
			Empty: true,
		})
	}

	findings := evaluateSchemaAuditFindings(partitions, map[string]string{
		"dynamic_partition.enable":    "true",
		"dynamic_partition.end":       "8",
		"dynamic_partition.time_unit": "DAY",
		"dynamic_partition.prefix":    "p",
		"dynamic_partition.time_zone": "Asia/Shanghai",
	})
	if !hasSchemaAuditRule(findings, "SA-E002") {
		t.Fatalf("expected SA-E002 when non-future empty tail remains long, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditFindingsUsesRangeLowerOrderForTail(t *testing.T) {
	t.Parallel()

	partitions := []SchemaAuditPartition{
		{
			Name:       "z_old_anchor",
			RangeLower: "2026-01-01 00:00:00",
			Empty:      false,
		},
		{
			Name:       "a01",
			RangeLower: "2026-01-02 00:00:00",
			Empty:      true,
		},
		{
			Name:       "a02",
			RangeLower: "2026-01-03 00:00:00",
			Empty:      true,
		},
		{
			Name:       "a03",
			RangeLower: "2026-01-04 00:00:00",
			Empty:      true,
		},
		{
			Name:       "a04",
			RangeLower: "2026-01-05 00:00:00",
			Empty:      true,
		},
		{
			Name:       "a05",
			RangeLower: "2026-01-06 00:00:00",
			Empty:      true,
		},
		{
			Name:       "a06",
			RangeLower: "2026-01-07 00:00:00",
			Empty:      true,
		},
		{
			Name:       "a07",
			RangeLower: "2026-01-08 00:00:00",
			Empty:      true,
		},
	}

	findings := evaluateSchemaAuditFindings(partitions, nil)
	if !hasSchemaAuditRule(findings, "SA-E002") {
		t.Fatalf("expected SA-E002 when range lower ordering reveals long tail, got %+v", findings)
	}
	for i := range findings {
		if findings[i].RuleID != "SA-E002" {
			continue
		}
		source, _ := findings[i].Evidence["orderSource"].(string)
		if source != "range_lower" {
			t.Fatalf("expected range_lower order source, got %+v", findings[i].Evidence)
		}
	}
}

func TestSchemaAuditOrderPartitionsForTimelineUsesPartialRangeLower(t *testing.T) {
	t.Parallel()

	partitions := []SchemaAuditPartition{
		{
			Name:  "manual_anchor",
			Empty: false,
		},
		{
			Name:       "a_new",
			RangeLower: "2026-01-03 00:00:00",
			Empty:      true,
		},
		{
			Name:       "z_old",
			RangeLower: "2026-01-01 00:00:00",
			Empty:      false,
		},
		{
			Name:       "b_mid",
			RangeLower: "2026-01-02 00:00:00",
			Empty:      true,
		},
	}

	ordered, source := schemaAuditOrderPartitionsForTimeline(partitions, nil)
	if source != "range_lower_partial" {
		t.Fatalf("expected range_lower_partial source, got %q", source)
	}
	wantOrder := []string{"manual_anchor", "z_old", "b_mid", "a_new"}
	for i := range wantOrder {
		if ordered[i].Name != wantOrder[i] {
			t.Fatalf("unexpected order at %d: got=%s want=%s", i, ordered[i].Name, wantOrder[i])
		}
	}
}

func TestEvaluateSchemaAuditFindingsLowersE002ConfidenceForPartialRangeOrder(t *testing.T) {
	t.Parallel()

	partitions := []SchemaAuditPartition{
		{
			Name:  "manual_anchor",
			Empty: false,
		},
		{
			Name:       "p01",
			RangeLower: "2026-01-01 00:00:00",
			Empty:      true,
		},
		{
			Name:       "p02",
			RangeLower: "2026-01-02 00:00:00",
			Empty:      true,
		},
		{
			Name:       "p03",
			RangeLower: "2026-01-03 00:00:00",
			Empty:      true,
		},
		{
			Name:       "p04",
			RangeLower: "2026-01-04 00:00:00",
			Empty:      true,
		},
		{
			Name:       "p05",
			RangeLower: "2026-01-05 00:00:00",
			Empty:      true,
		},
		{
			Name:       "p06",
			RangeLower: "2026-01-06 00:00:00",
			Empty:      true,
		},
		{
			Name:       "p07",
			RangeLower: "2026-01-07 00:00:00",
			Empty:      true,
		},
	}

	partialFindings := evaluateSchemaAuditFindings(partitions, nil)
	partialFinding, ok := schemaAuditFindingByRule(partialFindings, "SA-E002")
	if !ok {
		t.Fatalf("expected SA-E002 finding, got %+v", partialFindings)
	}
	source, _ := partialFinding.Evidence["orderSource"].(string)
	if source != "range_lower_partial" {
		t.Fatalf("expected range_lower_partial source, got %+v", partialFinding.Evidence)
	}

	fullRangeFindings := evaluateSchemaAuditFindings(
		[]SchemaAuditPartition{
			{
				Name:       "manual_anchor",
				RangeLower: "2025-12-31 00:00:00",
				Empty:      false,
			},
			{
				Name:       "p01",
				RangeLower: "2026-01-01 00:00:00",
				Empty:      true,
			},
			{
				Name:       "p02",
				RangeLower: "2026-01-02 00:00:00",
				Empty:      true,
			},
			{
				Name:       "p03",
				RangeLower: "2026-01-03 00:00:00",
				Empty:      true,
			},
			{
				Name:       "p04",
				RangeLower: "2026-01-04 00:00:00",
				Empty:      true,
			},
			{
				Name:       "p05",
				RangeLower: "2026-01-05 00:00:00",
				Empty:      true,
			},
			{
				Name:       "p06",
				RangeLower: "2026-01-06 00:00:00",
				Empty:      true,
			},
			{
				Name:       "p07",
				RangeLower: "2026-01-07 00:00:00",
				Empty:      true,
			},
		},
		nil,
	)
	fullRangeFinding, ok := schemaAuditFindingByRule(fullRangeFindings, "SA-E002")
	if !ok {
		t.Fatalf("expected SA-E002 finding in full range scenario, got %+v", fullRangeFindings)
	}
	if partialFinding.Confidence >= fullRangeFinding.Confidence {
		t.Fatalf(
			"expected partial-range confidence < full-range confidence, partial=%v full=%v",
			partialFinding.Confidence,
			fullRangeFinding.Confidence,
		)
	}
}

func TestEvaluateSchemaAuditFindingsExcludesFutureEmptyForE001AndD004(t *testing.T) {
	t.Parallel()

	partitions := make([]SchemaAuditPartition, 0, 20)
	for i := 1; i <= 10; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p200001%02d", i),
			Empty: false,
		})
	}
	for i := 1; i <= 10; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p209901%02d", i),
			Empty: true,
		})
	}

	findings := evaluateSchemaAuditFindings(partitions, map[string]string{
		"dynamic_partition.enable":    "true",
		"dynamic_partition.end":       "10",
		"dynamic_partition.time_unit": "DAY",
		"dynamic_partition.prefix":    "p",
		"dynamic_partition.time_zone": "Asia/Shanghai",
	})
	if hasSchemaAuditRule(findings, "SA-E001") {
		t.Fatalf("expected SA-E001 to be suppressed after precise future exclusion, got %+v", findings)
	}
	if hasSchemaAuditRule(findings, "SA-D004") {
		t.Fatalf("expected SA-D004 to be suppressed after precise future exclusion, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditFindingsStillWarnsForNonFutureEmpty(t *testing.T) {
	t.Parallel()

	partitions := make([]SchemaAuditPartition, 0, 30)
	for i := 1; i <= 5; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p200001%02d", i),
			Empty: false,
		})
	}
	for i := 6; i <= 20; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p200001%02d", i),
			Empty: true,
		})
	}
	for i := 1; i <= 10; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p209901%02d", i),
			Empty: true,
		})
	}

	findings := evaluateSchemaAuditFindings(partitions, map[string]string{
		"dynamic_partition.enable":    "true",
		"dynamic_partition.end":       "10",
		"dynamic_partition.time_unit": "DAY",
		"dynamic_partition.prefix":    "p",
		"dynamic_partition.time_zone": "Asia/Shanghai",
	})
	if !hasSchemaAuditRule(findings, "SA-E001") {
		t.Fatalf("expected SA-E001 for non-future empty partitions, got %+v", findings)
	}
	if !hasSchemaAuditRule(findings, "SA-D004") {
		t.Fatalf("expected SA-D004 for non-future empty partitions, got %+v", findings)
	}
}

func TestEvaluateSchemaAuditFindingsLowersConfidenceWhenFutureCannotBeClassified(t *testing.T) {
	t.Parallel()

	partitions := make([]SchemaAuditPartition, 0, 30)
	for i := 1; i <= 30; i++ {
		partitions = append(partitions, SchemaAuditPartition{
			Name:  fmt.Sprintf("p%02d", i),
			Empty: i > 5,
		})
	}

	findings := evaluateSchemaAuditFindings(partitions, map[string]string{
		"dynamic_partition.enable":    "true",
		"dynamic_partition.end":       "10",
		"dynamic_partition.time_unit": "DAY",
		"dynamic_partition.prefix":    "p",
	})
	if !hasSchemaAuditRule(findings, "SA-E001") {
		t.Fatalf("expected SA-E001, got %+v", findings)
	}
	for i := range findings {
		if findings[i].RuleID == "SA-E001" && findings[i].Confidence >= 0.95 {
			t.Fatalf("expected reduced confidence when future cannot be classified, got %+v", findings[i])
		}
	}
}

func TestSchemaAuditIsFutureDynamicPartitionNameWeek(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("UTC+8", 8*3600)
	reference := time.Date(2026, time.February, 26, 12, 0, 0, 0, location)
	futureYear, futureWeek := schemaAuditWeekPartitionToken(reference.AddDate(0, 0, 7), 1, location)
	pastYear, pastWeek := schemaAuditWeekPartitionToken(reference.AddDate(0, 0, -7), 1, location)

	futureName := fmt.Sprintf("p%04d_%02d", futureYear, futureWeek)
	pastName := fmt.Sprintf("p%04d_%02d", pastYear, pastWeek)

	isFuture, ok := schemaAuditIsFutureDynamicPartitionName(
		futureName,
		"p",
		"WEEK",
		reference,
		location,
		1,
	)
	if !ok || !isFuture {
		t.Fatalf("expected future week partition to be classified, ok=%v, isFuture=%v", ok, isFuture)
	}

	isFuture, ok = schemaAuditIsFutureDynamicPartitionName(
		pastName,
		"p",
		"WEEK",
		reference,
		location,
		1,
	)
	if !ok || isFuture {
		t.Fatalf("expected past week partition to be non-future, ok=%v, isFuture=%v", ok, isFuture)
	}
}

func TestSchemaAuditEffectiveEmptyStatsUsesPartitionRangeFirst(t *testing.T) {
	t.Parallel()

	partitions := []SchemaAuditPartition{
		{
			Name:       "manual_old",
			Empty:      false,
			RangeLower: "2000-01-01 00:00:00",
		},
		{
			Name:       "manual_future",
			Empty:      true,
			RangeLower: "2099-01-01 00:00:00",
		},
	}

	effectiveTotal, effectiveEmpty, evidence, classified := schemaAuditEffectiveEmptyStatsForPartitions(
		partitions,
		map[string]string{
			"dynamic_partition.enable":    "true",
			"dynamic_partition.end":       "10",
			"dynamic_partition.time_unit": "DAY",
			"dynamic_partition.prefix":    "p",
		},
	)
	if !classified {
		t.Fatalf("expected range-based future classification")
	}
	if effectiveTotal != 1 || effectiveEmpty != 0 {
		t.Fatalf("unexpected effective stats: total=%d empty=%d", effectiveTotal, effectiveEmpty)
	}
	if evidence["futureExclusionSource"] != "partition_range" {
		t.Fatalf("expected partition_range source, got %+v", evidence)
	}
}

func TestSchemaAuditEffectiveEmptyStatsDoesNotClassifyPartialNameParse(t *testing.T) {
	t.Parallel()

	partitions := make([]SchemaAuditPartition, 0, 6)
	partitions = append(partitions,
		SchemaAuditPartition{Name: "p20000101", Empty: false},
		SchemaAuditPartition{Name: "p20000102", Empty: false},
		SchemaAuditPartition{Name: "p20990101", Empty: true},
		SchemaAuditPartition{Name: "p20990102", Empty: true},
		SchemaAuditPartition{Name: "p20990103", Empty: true},
		SchemaAuditPartition{Name: "manual_partition", Empty: true},
	)

	_, _, evidence, classified := schemaAuditEffectiveEmptyStatsForPartitions(
		partitions,
		map[string]string{
			"dynamic_partition.enable":    "true",
			"dynamic_partition.end":       "10",
			"dynamic_partition.time_unit": "DAY",
			"dynamic_partition.prefix":    "p",
		},
	)
	if classified {
		t.Fatalf("expected partial parse to be unclassified, got evidence=%+v", evidence)
	}
	if evidence["futureExclusionSource"] != "unrecognized_no_exclusion" {
		t.Fatalf("expected unrecognized_no_exclusion source, got %+v", evidence)
	}
}

func TestSchemaAuditParsePartitionLowerBoundTime(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("UTC+8", 8*3600)
	tests := []struct {
		name  string
		raw   string
		ok    bool
		year  int
		month time.Month
		day   int
	}{
		{
			name:  "date only",
			raw:   "2026-02-26",
			ok:    true,
			year:  2026,
			month: time.February,
			day:   26,
		},
		{
			name:  "datetime",
			raw:   "2026-02-26 09:30:00",
			ok:    true,
			year:  2026,
			month: time.February,
			day:   26,
		},
		{
			name:  "datetime nanos",
			raw:   "2026-02-26 09:30:00.123456",
			ok:    true,
			year:  2026,
			month: time.February,
			day:   26,
		},
		{
			name:  "composite lower bound keeps first key",
			raw:   "2026-02-26 09:30:00, 100",
			ok:    true,
			year:  2026,
			month: time.February,
			day:   26,
		},
		{
			name: "compact day should be rejected",
			raw:  "20260226",
			ok:   false,
		},
		{
			name: "year only should be rejected",
			raw:  "2026",
			ok:   false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, ok := schemaAuditParsePartitionLowerBoundTime(tc.raw, location)
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got %v, parsed=%v", tc.ok, ok, parsed)
			}
			if !tc.ok {
				return
			}
			if parsed.Year() != tc.year || parsed.Month() != tc.month || parsed.Day() != tc.day {
				t.Fatalf(
					"unexpected parsed date, got %04d-%02d-%02d",
					parsed.Year(),
					parsed.Month(),
					parsed.Day(),
				)
			}
		})
	}
}
