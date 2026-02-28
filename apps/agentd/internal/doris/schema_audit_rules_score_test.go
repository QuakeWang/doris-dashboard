package doris

import "testing"

func TestComputeSchemaAuditScoreRespectsImpact(t *testing.T) {
	t.Parallel()

	lowImpact := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-E001",
			Severity:   "warn",
			Confidence: 0.95,
			Evidence: map[string]any{
				"emptyRatio":      0.30,
				"totalPartitions": 64,
			},
		},
	})
	highImpact := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-E001",
			Severity:   "critical",
			Confidence: 0.95,
			Evidence: map[string]any{
				"emptyRatio":      0.90,
				"totalPartitions": 64,
			},
		},
	})
	if lowImpact >= highImpact {
		t.Fatalf("expected high impact score > low impact score, got low=%d high=%d", lowImpact, highImpact)
	}
}

func TestComputeSchemaAuditScoreRespectsCoverage(t *testing.T) {
	t.Parallel()

	smallSample := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-B001",
			Severity:   "warn",
			Confidence: 0.85,
			Evidence: map[string]any{
				"anomalyCount":        1,
				"validPartitionCount": 2,
			},
		},
	})
	largeSample := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-B001",
			Severity:   "warn",
			Confidence: 0.85,
			Evidence: map[string]any{
				"anomalyCount":        32,
				"validPartitionCount": 64,
			},
		},
	})
	if smallSample >= largeSample {
		t.Fatalf("expected large sample score > small sample score, got small=%d large=%d", smallSample, largeSample)
	}
}

func TestComputeSchemaAuditScoreProbabilisticMerge(t *testing.T) {
	t.Parallel()

	criticalOnly := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-B006",
			Severity:   "critical",
			Confidence: 0.95,
			Evidence:   map[string]any{"keysType": "unique"},
		},
	})
	withAdditionalWarn := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-B006",
			Severity:   "critical",
			Confidence: 0.95,
			Evidence:   map[string]any{"keysType": "unique"},
		},
		{
			RuleID:     "SA-E002",
			Severity:   "warn",
			Confidence: 0.90,
			Evidence: map[string]any{
				"emptyTailCount": 10,
				"threshold":      7,
				"partitionCount": 32,
			},
		},
	})
	if withAdditionalWarn <= criticalOnly {
		t.Fatalf("expected additional finding to increase score, got base=%d combined=%d", criticalOnly, withAdditionalWarn)
	}
	if withAdditionalWarn >= 100 {
		t.Fatalf("expected probabilistic merge to avoid hard saturation, got %d", withAdditionalWarn)
	}
}

func TestComputeSchemaAuditScoreD004RespectsDynamicWindowSpan(t *testing.T) {
	t.Parallel()

	smallWindow := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-D004",
			Severity:   "warn",
			Confidence: 0.90,
			Evidence: map[string]any{
				"emptyRatio":         0.70,
				"windowSpan":         16,
				"windowSpanWarn":     32,
				"windowSpanCritical": 64,
				"partitionCount":     64,
			},
		},
	})

	largeWindow := computeSchemaAuditScore([]SchemaAuditFinding{
		{
			RuleID:     "SA-D004",
			Severity:   "warn",
			Confidence: 0.90,
			Evidence: map[string]any{
				"emptyRatio":         0.70,
				"windowSpan":         96,
				"windowSpanWarn":     32,
				"windowSpanCritical": 64,
				"partitionCount":     64,
			},
		},
	})

	if smallWindow >= largeWindow {
		t.Fatalf("expected larger dynamic window to increase D004 score, got small=%d large=%d", smallWindow, largeWindow)
	}
}

func TestSchemaAuditDynamicWindowSpan(t *testing.T) {
	t.Parallel()

	span, ok := schemaAuditDynamicWindowSpan(map[string]string{
		"dynamic_partition.start": "-7",
		"dynamic_partition.end":   "3",
	})
	if !ok {
		t.Fatalf("expected dynamic window span to be parsed")
	}
	if span != 11 {
		t.Fatalf("expected span 11, got %d", span)
	}

	_, ok = schemaAuditDynamicWindowSpan(map[string]string{
		"dynamic_partition.start": "x",
		"dynamic_partition.end":   "3",
	})
	if ok {
		t.Fatalf("expected invalid start to be rejected")
	}

	_, ok = schemaAuditDynamicWindowSpan(map[string]string{
		"dynamic_partition.start": "10",
		"dynamic_partition.end":   "3",
	})
	if ok {
		t.Fatalf("expected inverted window range to be rejected")
	}
}
