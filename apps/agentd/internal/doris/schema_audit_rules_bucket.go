package doris

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

const (
	schemaAuditDefaultAutoBucketMinBuckets       = 1
	schemaAuditDefaultAutoBucketMaxBuckets       = 128
	schemaAuditDefaultAutoBucketOutOfBoundsRatio = 0.5

	schemaAuditAdaptiveStorageComputeSizePerBucketGB = 10
	schemaAuditAdaptiveClassicSizePerBucketGB        = 5

	schemaAuditBucketSize100MB = 100 * 1024 * 1024
	schemaAuditBucketSize1GB   = 1024 * 1024 * 1024

	schemaAuditBestPracticeTabletSizeMinBytes = 1 * 1024 * 1024 * 1024
	schemaAuditBestPracticeTabletSizeMaxBytes = 10 * 1024 * 1024 * 1024
)

var (
	schemaAuditKeyClausePattern  = regexp.MustCompile(`(?is)\b(DUPLICATE|UNIQUE|AGGREGATE)\s+KEY\s*\(([^)]*)\)`)
	schemaAuditHashDistPattern   = regexp.MustCompile(`(?is)DISTRIBUTED\s+BY\s+HASH\s*\(([^)]*)\)\s*BUCKETS\s*(AUTO|\d+)`)
	schemaAuditRandomDistPattern = regexp.MustCompile(`(?is)DISTRIBUTED\s+BY\s+RANDOM\s*BUCKETS\s*(AUTO|\d+)`)
)

type schemaAuditBucketRuleConfig struct {
	MinBuckets               int
	MaxBuckets               int
	PartitionSizePerBucketGB int
	OutOfBoundsRatio         float64
}

type schemaAuditBucketEstimate struct {
	ExpectedMin int
	ExpectedMax int
	LowerBound  int
	UpperBound  int
}

type schemaAuditBucketAnomalySample struct {
	PartitionName string
	Buckets       int
	Estimate      schemaAuditBucketEstimate
}

type schemaAuditBucketJumpSample struct {
	FromName    string
	ToName      string
	FromBuckets int
	ToBuckets   int
	Ratio       float64
}

type schemaAuditCreateTableDescriptor struct {
	KeysType            string
	KeyColumns          []string
	DistributionType    string
	DistributionColumns []string
	AutoBucket          bool
	Buckets             int
}

func defaultSchemaAuditBucketRuleConfig() schemaAuditBucketRuleConfig {
	return schemaAuditBucketRuleConfig{
		MinBuckets:               schemaAuditDefaultAutoBucketMinBuckets,
		MaxBuckets:               schemaAuditDefaultAutoBucketMaxBuckets,
		PartitionSizePerBucketGB: -1,
		OutOfBoundsRatio:         schemaAuditDefaultAutoBucketOutOfBoundsRatio,
	}
}

func normalizeSchemaAuditBucketRuleConfig(
	cfg schemaAuditBucketRuleConfig,
) schemaAuditBucketRuleConfig {
	out := cfg
	if out.MinBuckets <= 0 {
		out.MinBuckets = schemaAuditDefaultAutoBucketMinBuckets
	}
	if out.MaxBuckets <= 0 {
		out.MaxBuckets = schemaAuditDefaultAutoBucketMaxBuckets
	}
	if out.MaxBuckets < out.MinBuckets {
		out.MaxBuckets = out.MinBuckets
	}
	if out.OutOfBoundsRatio <= 0 {
		out.OutOfBoundsRatio = schemaAuditDefaultAutoBucketOutOfBoundsRatio
	}
	if out.OutOfBoundsRatio > 0.95 {
		out.OutOfBoundsRatio = 0.95
	}
	return out
}

func evaluateSchemaAuditBucketFindings(
	partitions []SchemaAuditPartition,
	createTableSQL string,
	cfg schemaAuditBucketRuleConfig,
) []SchemaAuditFinding {
	normalizedConfig := normalizeSchemaAuditBucketRuleConfig(cfg)
	tableDescriptor := parseSchemaAuditCreateTableDescriptor(createTableSQL)
	if len(partitions) == 0 {
		return evaluateSchemaAuditBucketDDLFindings(tableDescriptor)
	}
	findings := evaluateSchemaAuditBucketDDLFindings(tableDescriptor)

	tooSmall := make([]schemaAuditBucketAnomalySample, 0, len(partitions))
	tooLarge := make([]schemaAuditBucketAnomalySample, 0, len(partitions))
	validCount := 0
	missingBucketCount := 0
	missingSizeCount := 0
	for i := range partitions {
		partition := partitions[i]
		if partition.Buckets <= 0 {
			missingBucketCount++
			continue
		}
		if partition.DataSizeBytes == 0 {
			missingSizeCount++
			continue
		}
		validCount++
		estimate := estimateSchemaAuditBucket(
			partition.DataSizeBytes,
			normalizedConfig,
		)
		if partition.Buckets < estimate.LowerBound {
			tooSmall = append(tooSmall, schemaAuditBucketAnomalySample{
				PartitionName: partition.Name,
				Buckets:       partition.Buckets,
				Estimate:      estimate,
			})
			continue
		}
		if partition.Buckets > estimate.UpperBound {
			tooLarge = append(tooLarge, schemaAuditBucketAnomalySample{
				PartitionName: partition.Name,
				Buckets:       partition.Buckets,
				Estimate:      estimate,
			})
		}
	}

	if validCount == 0 {
		severity := "info"
		if missingBucketCount > 0 {
			severity = "warn"
		}
		findings = append(findings, SchemaAuditFinding{
			RuleID:     "SA-B004",
			Severity:   severity,
			Confidence: 0.8,
			Summary:    "Bucket estimation skipped due to insufficient partition size or bucket metadata",
			Evidence: map[string]any{
				"partitionCount":     len(partitions),
				"missingBucketCount": missingBucketCount,
				"missingSizeCount":   missingSizeCount,
			},
			Recommendation: "Ensure SHOW PARTITIONS includes Buckets/DataSize and evaluate on non-empty partitions.",
		})
	} else {
		if len(tooSmall) > 0 {
			findings = append(findings, SchemaAuditFinding{
				RuleID:     "SA-B001",
				Severity:   schemaAuditBucketSeverity(len(tooSmall), validCount),
				Confidence: 0.85,
				Summary:    "Detected partitions where buckets are significantly lower than source-aligned estimate",
				Evidence: map[string]any{
					"validPartitionCount": validCount,
					"anomalyCount":        len(tooSmall),
					"outOfBoundsRatio":    normalizedConfig.OutOfBoundsRatio,
					"samples":             toSchemaAuditBucketSamples(tooSmall, 5),
				},
				Recommendation: "Increase bucket count or enable AUTO buckets for future partitions.",
			})
		}
		if len(tooLarge) > 0 {
			findings = append(findings, SchemaAuditFinding{
				RuleID:     "SA-B002",
				Severity:   schemaAuditBucketSeverity(len(tooLarge), validCount),
				Confidence: 0.85,
				Summary:    "Detected partitions where buckets are significantly higher than source-aligned estimate",
				Evidence: map[string]any{
					"validPartitionCount": validCount,
					"anomalyCount":        len(tooLarge),
					"outOfBoundsRatio":    normalizedConfig.OutOfBoundsRatio,
					"samples":             toSchemaAuditBucketSamples(tooLarge, 5),
				},
				Recommendation: "Reduce bucket count to avoid oversized tablet fanout and scheduling overhead.",
			})
		}
	}

	bucketJumpFindings := evaluateSchemaAuditBucketJumpFinding(
		partitions,
		tableDescriptor,
		normalizedConfig.OutOfBoundsRatio,
	)
	findings = append(findings, bucketJumpFindings...)

	findings = append(
		findings,
		evaluateSchemaAuditBucketBestPracticeFindings(partitions)...,
	)
	if shouldEmitBucketChangeExpectationFinding(findings) {
		findings = append(findings, SchemaAuditFinding{
			RuleID:     "SA-B009",
			Severity:   "info",
			Confidence: 0.9,
			Summary:    "Bucket adjustments affect only newly created partitions",
			Evidence: map[string]any{
				"partitionCount":               len(partitions),
				"bucketAdjustmentScope":        "new partitions only",
				"existingPartitionBucketFixed": true,
			},
			Recommendation: "Plan bucket changes with partition lifecycle (add new partitions and phase out old ones).",
		})
	}
	return findings
}

func evaluateSchemaAuditBucketJumpFinding(
	partitions []SchemaAuditPartition,
	tableDescriptor schemaAuditCreateTableDescriptor,
	outOfBoundsRatio float64,
) []SchemaAuditFinding {
	if !tableDescriptor.AutoBucket {
		return nil
	}
	if len(partitions) <= 1 {
		return nil
	}

	ordered, orderSource := schemaAuditOrderPartitionsForTimeline(partitions, nil)

	jumps := make([]schemaAuditBucketJumpSample, 0, len(ordered))
	for i := 1; i < len(ordered); i++ {
		prev := ordered[i-1]
		curr := ordered[i]
		if prev.Buckets <= 0 || curr.Buckets <= 0 {
			continue
		}
		if prev.Buckets == curr.Buckets {
			continue
		}
		upperBound := float64(prev.Buckets) * (1 + outOfBoundsRatio)
		lowerBound := float64(prev.Buckets) * (1 - outOfBoundsRatio)
		if float64(curr.Buckets) > upperBound || float64(curr.Buckets) < lowerBound {
			jumps = append(jumps, schemaAuditBucketJumpSample{
				FromName:    prev.Name,
				ToName:      curr.Name,
				FromBuckets: prev.Buckets,
				ToBuckets:   curr.Buckets,
				Ratio:       float64(curr.Buckets) / float64(prev.Buckets),
			})
		}
	}
	if len(jumps) == 0 {
		return nil
	}

	confidence := schemaAuditTimelineConfidence(orderSource, false)
	return []SchemaAuditFinding{
		{
			RuleID:     "SA-B003",
			Severity:   schemaAuditBucketSeverity(len(jumps), len(ordered)-1),
			Confidence: confidence,
			Summary:    "Detected AUTO bucket jumps that exceed source threshold between adjacent partitions",
			Evidence: map[string]any{
				"partitionCount":    len(ordered),
				"jumpCount":         len(jumps),
				"orderSource":       orderSource,
				"outOfBoundsRatio":  outOfBoundsRatio,
				"transitionSamples": limitSchemaAuditBucketJumpSamples(jumps, 5),
			},
			Recommendation: "Review AUTO bucket estimate inputs and verify dynamic partition growth trend.",
		},
	}
}

func evaluateSchemaAuditBucketDDLFindings(
	tableDescriptor schemaAuditCreateTableDescriptor,
) []SchemaAuditFinding {
	findings := make([]SchemaAuditFinding, 0, 2)

	if tableDescriptor.DistributionType == "random" && tableDescriptor.KeysType != "" && tableDescriptor.KeysType != "duplicate" {
		severity := "info"
		confidence := 0.75
		summary := "Random distribution may be suboptimal for current table key model"
		recommendation := "Prefer HASH distribution when point-query pruning or strict key-model checks are needed."
		if tableDescriptor.KeysType == "unique" {
			severity = "critical"
			confidence = 0.95
			summary = "UNIQUE KEY table should not use RANDOM distribution"
			recommendation = "Switch to HASH distribution using key columns."
		}
		findings = append(findings, SchemaAuditFinding{
			RuleID:     "SA-B005",
			Severity:   severity,
			Confidence: confidence,
			Summary:    summary,
			Evidence: map[string]any{
				"keysType":         tableDescriptor.KeysType,
				"distributionType": tableDescriptor.DistributionType,
				"autoBucket":       tableDescriptor.AutoBucket,
				"buckets":          tableDescriptor.Buckets,
			},
			Recommendation: recommendation,
		})
	}

	if tableDescriptor.DistributionType == "hash" &&
		(tableDescriptor.KeysType == "unique" || tableDescriptor.KeysType == "aggregate") &&
		len(tableDescriptor.KeyColumns) > 0 &&
		len(tableDescriptor.DistributionColumns) > 0 {
		keySet := make(map[string]struct{}, len(tableDescriptor.KeyColumns))
		for i := range tableDescriptor.KeyColumns {
			keySet[normalizeSchemaAuditColumnName(tableDescriptor.KeyColumns[i])] = struct{}{}
		}
		invalidColumns := make([]string, 0, len(tableDescriptor.DistributionColumns))
		for i := range tableDescriptor.DistributionColumns {
			col := normalizeSchemaAuditColumnName(tableDescriptor.DistributionColumns[i])
			if col == "" {
				continue
			}
			if _, ok := keySet[col]; ok {
				continue
			}
			invalidColumns = append(invalidColumns, tableDescriptor.DistributionColumns[i])
		}
		if len(invalidColumns) > 0 {
			severity := "warn"
			if tableDescriptor.KeysType == "unique" {
				severity = "critical"
			}
			findings = append(findings, SchemaAuditFinding{
				RuleID:     "SA-B006",
				Severity:   severity,
				Confidence: 0.95,
				Summary:    "HASH distribution contains non-key columns for current key model",
				Evidence: map[string]any{
					"keysType":              tableDescriptor.KeysType,
					"keyColumns":            tableDescriptor.KeyColumns,
					"distributionColumns":   tableDescriptor.DistributionColumns,
					"invalidDistKeyColumns": invalidColumns,
				},
				Recommendation: "Use key columns as HASH distribution columns to satisfy FE validation constraints.",
			})
		}
	}

	return findings
}

func evaluateSchemaAuditBucketBestPracticeFindings(
	partitions []SchemaAuditPartition,
) []SchemaAuditFinding {
	totalDataBytes, totalTabletCount, partitionWithBucketCount := summarizeSchemaAuditTabletLayout(partitions)
	findings := make([]SchemaAuditFinding, 0, 2)

	if totalTabletCount > 0 && totalDataBytes > 0 {
		averageTabletSizeBytes := float64(totalDataBytes) / float64(totalTabletCount)
		if averageTabletSizeBytes < schemaAuditBestPracticeTabletSizeMinBytes ||
			averageTabletSizeBytes > schemaAuditBestPracticeTabletSizeMaxBytes {
			severity := "info"
			if averageTabletSizeBytes < schemaAuditBestPracticeTabletSizeMinBytes/2 ||
				averageTabletSizeBytes > schemaAuditBestPracticeTabletSizeMaxBytes*2 {
				severity = "warn"
			}
			findings = append(findings, SchemaAuditFinding{
				RuleID:     "SA-B007",
				Severity:   severity,
				Confidence: 0.8,
				Summary:    "Average tablet size is outside recommended 1-10GB range",
				Evidence: map[string]any{
					"partitionCount":           len(partitions),
					"partitionWithBucketCount": partitionWithBucketCount,
					"totalTabletCount":         totalTabletCount,
					"totalDataBytes":           totalDataBytes,
					"averageTabletSizeBytes":   averageTabletSizeBytes,
					"recommendedMinBytes":      schemaAuditBestPracticeTabletSizeMinBytes,
					"recommendedMaxBytes":      schemaAuditBestPracticeTabletSizeMaxBytes,
				},
				Recommendation: "Tune bucket count so average tablet size converges to 1-10GB over active partitions.",
			})
		}
	}

	return findings
}

func summarizeSchemaAuditTabletLayout(
	partitions []SchemaAuditPartition,
) (totalDataBytes uint64, totalTabletCount int, partitionWithBucketCount int) {
	var data uint64
	tablets := 0
	partitionCount := 0
	for i := range partitions {
		if partitions[i].Buckets <= 0 {
			continue
		}
		partitionCount++
		tablets += partitions[i].Buckets
		data += partitions[i].DataSizeBytes
	}
	return data, tablets, partitionCount
}

func shouldEmitBucketChangeExpectationFinding(findings []SchemaAuditFinding) bool {
	for i := range findings {
		switch findings[i].RuleID {
		case "SA-B001", "SA-B002", "SA-B003", "SA-B005", "SA-B006", "SA-B007":
			return true
		}
	}
	return false
}

func parseSchemaAuditCreateTableDescriptor(createTableSQL string) schemaAuditCreateTableDescriptor {
	descriptor := schemaAuditCreateTableDescriptor{}

	if match := schemaAuditKeyClausePattern.FindStringSubmatch(createTableSQL); len(match) >= 3 {
		descriptor.KeysType = strings.ToLower(strings.TrimSpace(match[1]))
		descriptor.KeyColumns = parseSchemaAuditIdentifierList(match[2])
	}

	if match := schemaAuditHashDistPattern.FindStringSubmatch(createTableSQL); len(match) >= 3 {
		descriptor.DistributionType = "hash"
		descriptor.DistributionColumns = parseSchemaAuditIdentifierList(match[1])
		bucketToken := strings.TrimSpace(strings.ToUpper(match[2]))
		descriptor.AutoBucket = bucketToken == "AUTO"
		if !descriptor.AutoBucket {
			if v, err := strconv.Atoi(bucketToken); err == nil && v > 0 {
				descriptor.Buckets = v
			}
		}
		return descriptor
	}

	if match := schemaAuditRandomDistPattern.FindStringSubmatch(createTableSQL); len(match) >= 2 {
		descriptor.DistributionType = "random"
		bucketToken := strings.TrimSpace(strings.ToUpper(match[1]))
		descriptor.AutoBucket = bucketToken == "AUTO"
		if !descriptor.AutoBucket {
			if v, err := strconv.Atoi(bucketToken); err == nil && v > 0 {
				descriptor.Buckets = v
			}
		}
	}
	return descriptor
}

func parseSchemaAuditIdentifierList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for i := range parts {
		trimmed := strings.TrimSpace(parts[i])
		trimmed = strings.Trim(trimmed, "` ")
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func normalizeSchemaAuditColumnName(name string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(name), "`"))
}

func estimateSchemaAuditBucket(
	compressedPartitionSizeBytes uint64,
	cfg schemaAuditBucketRuleConfig,
) schemaAuditBucketEstimate {
	expectedMin := 0
	expectedMax := 0
	if cfg.PartitionSizePerBucketGB > 0 {
		expected := estimateSchemaAuditBucketsByPartitionSize(compressedPartitionSizeBytes, cfg.PartitionSizePerBucketGB, cfg)
		expectedMin = expected
		expectedMax = expected
	} else {
		estimateClassic := estimateSchemaAuditBucketsByPartitionSize(
			compressedPartitionSizeBytes,
			schemaAuditAdaptiveClassicSizePerBucketGB,
			cfg,
		)
		estimateStorageCompute := estimateSchemaAuditBucketsByPartitionSize(
			compressedPartitionSizeBytes,
			schemaAuditAdaptiveStorageComputeSizePerBucketGB,
			cfg,
		)
		if estimateClassic <= estimateStorageCompute {
			expectedMin = estimateClassic
			expectedMax = estimateStorageCompute
		} else {
			expectedMin = estimateStorageCompute
			expectedMax = estimateClassic
		}
	}

	lowerBound := int(math.Floor(float64(expectedMin) * (1 - cfg.OutOfBoundsRatio)))
	if lowerBound < cfg.MinBuckets {
		lowerBound = cfg.MinBuckets
	}
	upperBound := int(math.Ceil(float64(expectedMax) * (1 + cfg.OutOfBoundsRatio)))
	if upperBound < lowerBound {
		upperBound = lowerBound
	}
	if upperBound > cfg.MaxBuckets {
		upperBound = cfg.MaxBuckets
	}
	return schemaAuditBucketEstimate{
		ExpectedMin: expectedMin,
		ExpectedMax: expectedMax,
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
	}
}

func estimateSchemaAuditBucketsByPartitionSize(
	compressedPartitionSizeBytes uint64,
	partitionSizePerBucketGB int,
	cfg schemaAuditBucketRuleConfig,
) int {
	estimated := 1
	switch {
	case compressedPartitionSizeBytes <= schemaAuditBucketSize100MB:
		estimated = 1
	case compressedPartitionSizeBytes <= schemaAuditBucketSize1GB:
		estimated = 2
	default:
		denominator := float64(partitionSizePerBucketGB * schemaAuditBucketSize1GB)
		estimated = int(math.Ceil(float64(compressedPartitionSizeBytes) / denominator))
	}
	if estimated < cfg.MinBuckets {
		estimated = cfg.MinBuckets
	}
	if estimated > cfg.MaxBuckets {
		estimated = cfg.MaxBuckets
	}
	return estimated
}

func toSchemaAuditBucketSamples(
	samples []schemaAuditBucketAnomalySample,
	limit int,
) []map[string]any {
	limited := samples
	if len(limited) > limit {
		limited = limited[:limit]
	}
	out := make([]map[string]any, 0, len(limited))
	for i := range limited {
		out = append(out, map[string]any{
			"partitionName": limited[i].PartitionName,
			"actualBuckets": limited[i].Buckets,
			"expectedMin":   limited[i].Estimate.ExpectedMin,
			"expectedMax":   limited[i].Estimate.ExpectedMax,
			"lowerBound":    limited[i].Estimate.LowerBound,
			"upperBound":    limited[i].Estimate.UpperBound,
		})
	}
	return out
}

func limitSchemaAuditBucketJumpSamples(
	samples []schemaAuditBucketJumpSample,
	limit int,
) []map[string]any {
	limited := samples
	if len(limited) > limit {
		limited = limited[:limit]
	}
	out := make([]map[string]any, 0, len(limited))
	for i := range limited {
		out = append(out, map[string]any{
			"fromPartition": limited[i].FromName,
			"toPartition":   limited[i].ToName,
			"fromBuckets":   limited[i].FromBuckets,
			"toBuckets":     limited[i].ToBuckets,
			"ratio":         limited[i].Ratio,
		})
	}
	return out
}

func schemaAuditBucketSeverity(anomalyCount int, sampleCount int) string {
	if sampleCount <= 0 {
		return "info"
	}
	ratio := float64(anomalyCount) / float64(sampleCount)
	if ratio >= 0.5 || anomalyCount >= 3 {
		return "warn"
	}
	return "info"
}
