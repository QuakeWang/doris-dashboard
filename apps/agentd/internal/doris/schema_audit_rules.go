package doris

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	schemaAuditEmptyRatioWarn            = 0.3
	schemaAuditEmptyRatioCritical        = 0.6
	schemaAuditEmptyTailThreshold        = 7
	schemaAuditDynamicWindowSpanWarn     = 32
	schemaAuditDynamicWindowSpanCritical = 64
	schemaAuditDateTimeNanosLayout       = "2006-01-02 15:04:05.999999999"

	schemaAuditScoreMax                  = 100
	schemaAuditScoreWarnSeverityFactor   = 0.70
	schemaAuditScoreInfoSeverityFactor   = 0.35
	schemaAuditScoreCoverageSampleTarget = 16.0
	schemaAuditScoreCoverageMinFactor    = 0.55
	schemaAuditScoreMinConfidence        = 0.5
	schemaAuditScoreMaxContribution      = 0.95
)

func evaluateSchemaAuditFindings(partitions []SchemaAuditPartition, dynamicProperties map[string]string) []SchemaAuditFinding {
	findings := make([]SchemaAuditFinding, 0, 4)
	totalPartitions := len(partitions)
	if totalPartitions == 0 {
		return findings
	}

	emptyCount := 0
	for i := range partitions {
		if partitions[i].Empty {
			emptyCount++
		}
	}
	effectiveTotalPartitions, effectiveEmptyCount, exclusionEvidence, futurePartitionClassified := schemaAuditEffectiveEmptyStatsForPartitions(
		partitions,
		dynamicProperties,
	)
	emptyRatio := ratio(effectiveEmptyCount, effectiveTotalPartitions)
	dynamicWindowSpan, hasDynamicWindowSpan := schemaAuditDynamicWindowSpan(dynamicProperties)
	futureWindow, hasFutureWindow := schemaAuditDynamicFutureOffset(dynamicProperties)
	futureUncertain := isDynamicPartitionEnabled(dynamicProperties) && hasFutureWindow && futureWindow > 0 && !futurePartitionClassified

	if emptyRatio >= schemaAuditEmptyRatioWarn {
		severity := "warn"
		if emptyRatio >= schemaAuditEmptyRatioCritical {
			severity = "critical"
		}
		confidence := 0.95
		if futureUncertain {
			confidence = 0.75
		}
		findings = append(findings, SchemaAuditFinding{
			RuleID:     "SA-E001",
			Severity:   severity,
			Confidence: confidence,
			Summary:    "Empty partition ratio is high",
			Evidence: map[string]any{
				"totalPartitions":          effectiveTotalPartitions,
				"emptyPartitions":          effectiveEmptyCount,
				"emptyRatio":               emptyRatio,
				"rawTotalPartitions":       totalPartitions,
				"rawEmptyPartitions":       emptyCount,
				"excludedFuturePartitions": exclusionEvidence["excludedFuturePartitions"],
				"excludedFutureEmpty":      exclusionEvidence["excludedFutureEmpty"],
				"futureExclusionSource":    exclusionEvidence["futureExclusionSource"],
				"futurePartitionUncertain": futureUncertain,
				"potentialFutureWindow":    exclusionEvidence["potentialFutureWindow"],
				"warnThreshold":            schemaAuditEmptyRatioWarn,
				"criticalThreshold":        schemaAuditEmptyRatioCritical,
			},
			Recommendation: "Reduce dynamic partition window and clean long-term empty partitions.",
		})
	}

	ordered, orderSource := schemaAuditOrderPartitionsForTimeline(partitions, dynamicProperties)
	emptyTailCount := 0
	for i := len(ordered) - 1; i >= 0; i-- {
		if !ordered[i].Empty {
			break
		}
		emptyTailCount++
	}

	effectiveEmptyTailCount := emptyTailCount
	tailExclusionSource := "none"
	tailFutureClassified := false
	if isDynamicPartitionEnabled(dynamicProperties) && emptyTailCount > 0 {
		effectiveEmptyTailCount, tailExclusionSource, tailFutureClassified = schemaAuditEffectiveEmptyTailCount(
			ordered,
			dynamicProperties,
			time.Now(),
		)
	}
	tailFutureUncertain := isDynamicPartitionEnabled(dynamicProperties) && hasFutureWindow && futureWindow > 0 && !tailFutureClassified
	if effectiveEmptyTailCount >= schemaAuditEmptyTailThreshold {
		confidence := schemaAuditTimelineConfidence(orderSource, tailFutureUncertain)
		findings = append(findings, SchemaAuditFinding{
			RuleID:     "SA-E002",
			Severity:   "warn",
			Confidence: confidence,
			Summary:    "Detected consecutive empty partitions in the latest partition tail",
			Evidence: map[string]any{
				"emptyTailCount":           effectiveEmptyTailCount,
				"rawEmptyTailCount":        emptyTailCount,
				"excludedFutureTailEmpty":  emptyTailCount - effectiveEmptyTailCount,
				"orderSource":              orderSource,
				"futureExclusionSource":    tailExclusionSource,
				"futurePartitionUncertain": tailFutureUncertain,
				"threshold":                schemaAuditEmptyTailThreshold,
				"latestPartitionName":      ordered[len(ordered)-1].Name,
			},
			Recommendation: "Check whether dynamic partition end/start are too wide for current write traffic.",
		})
	}

	if isDynamicPartitionEnabled(dynamicProperties) && emptyRatio >= schemaAuditEmptyRatioCritical {
		confidence := 0.9
		if futureUncertain {
			confidence = 0.65
		}
		evidence := map[string]any{
			"dynamicPartitionEnabled":  true,
			"emptyRatio":               emptyRatio,
			"rawEmptyRatio":            ratio(emptyCount, totalPartitions),
			"totalPartitions":          effectiveTotalPartitions,
			"emptyPartitions":          effectiveEmptyCount,
			"rawTotalPartitions":       totalPartitions,
			"rawEmptyPartitions":       emptyCount,
			"excludedFuturePartitions": exclusionEvidence["excludedFuturePartitions"],
			"excludedFutureEmpty":      exclusionEvidence["excludedFutureEmpty"],
			"futureExclusionSource":    exclusionEvidence["futureExclusionSource"],
			"futurePartitionUncertain": futureUncertain,
			"potentialFutureWindow":    exclusionEvidence["potentialFutureWindow"],
			"start":                    dynamicProperties["dynamic_partition.start"],
			"end":                      dynamicProperties["dynamic_partition.end"],
			"buckets":                  dynamicProperties["dynamic_partition.buckets"],
			"windowSpanWarn":           schemaAuditDynamicWindowSpanWarn,
			"windowSpanCritical":       schemaAuditDynamicWindowSpanCritical,
		}
		if hasDynamicWindowSpan {
			evidence["windowSpan"] = dynamicWindowSpan
		}
		findings = append(findings, SchemaAuditFinding{
			RuleID:         "SA-D004",
			Severity:       "warn",
			Confidence:     confidence,
			Summary:        "Dynamic partition window is creating mostly empty partitions",
			Evidence:       evidence,
			Recommendation: "Shrink dynamic_partition.end/start and align partition window with real data arrival.",
		})
	}

	return findings
}

func evaluateSchemaAuditTableDetailFindings(
	partitions []SchemaAuditPartition,
	dynamicProperties map[string]string,
	createTableSQL string,
	bucketConfig schemaAuditBucketRuleConfig,
) []SchemaAuditFinding {
	findings := evaluateSchemaAuditFindings(partitions, dynamicProperties)
	findings = append(findings, evaluateSchemaAuditBucketFindings(partitions, createTableSQL, bucketConfig)...)
	return findings
}

func summarizeSchemaAuditFindings(findings []SchemaAuditFinding) []SchemaAuditFindingSummary {
	out := make([]SchemaAuditFindingSummary, 0, len(findings))
	for i := range findings {
		out = append(out, SchemaAuditFindingSummary{
			RuleID:   findings[i].RuleID,
			Severity: findings[i].Severity,
			Summary:  findings[i].Summary,
		})
	}
	return out
}

func computeSchemaAuditScore(findings []SchemaAuditFinding) int {
	if len(findings) == 0 {
		return 0
	}

	safeRatio := 1.0
	for i := range findings {
		contribution := schemaAuditScoreContribution(findings[i])
		if contribution <= 0 {
			continue
		}
		safeRatio *= (1 - contribution)
	}

	score := int(math.Round((1 - safeRatio) * float64(schemaAuditScoreMax)))
	if score < 0 {
		return 0
	}
	if score > schemaAuditScoreMax {
		return schemaAuditScoreMax
	}
	return score
}

func schemaAuditScoreContribution(finding SchemaAuditFinding) float64 {
	severity := schemaAuditSeverityFactor(finding.Severity)
	if severity <= 0 {
		return 0
	}

	weight := schemaAuditRuleWeight(finding.RuleID)
	impact := schemaAuditRuleImpact(finding)
	confidence := schemaAuditClampFloat(finding.Confidence, schemaAuditScoreMinConfidence, 1)
	coverage := schemaAuditCoverageFactor(finding.Evidence)

	contribution := severity * weight * impact * confidence * coverage
	return schemaAuditClampFloat(contribution, 0, schemaAuditScoreMaxContribution)
}

func schemaAuditSeverityFactor(severity string) float64 {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case "critical":
		return 1
	case "warn":
		return schemaAuditScoreWarnSeverityFactor
	case "info":
		return schemaAuditScoreInfoSeverityFactor
	default:
		return schemaAuditScoreInfoSeverityFactor
	}
}

func schemaAuditRuleWeight(ruleID string) float64 {
	switch strings.ToUpper(strings.TrimSpace(ruleID)) {
	case "SA-B005", "SA-B006":
		return 1.0
	case "SA-E001":
		return 0.95
	case "SA-D004":
		return 0.85
	case "SA-E002":
		return 0.80
	case "SA-B001", "SA-B002", "SA-B003":
		return 0.75
	case "SA-B004":
		return 0.60
	case "SA-B007":
		return 0.55
	case "SA-B009":
		return 0.25
	default:
		return 0.65
	}
}

func schemaAuditRuleImpact(finding SchemaAuditFinding) float64 {
	evidence := finding.Evidence
	switch strings.ToUpper(strings.TrimSpace(finding.RuleID)) {
	case "SA-E001":
		if ratio, ok := schemaAuditEvidenceNumber(evidence, "emptyRatio"); ok {
			return schemaAuditClampFloat(0.25+0.75*ratio, 0.25, 1)
		}
		return 0.70
	case "SA-D004":
		impact := 0.65
		if ratio, ok := schemaAuditEvidenceNumber(evidence, "emptyRatio"); ok {
			impact = schemaAuditClampFloat(0.30+0.70*ratio, 0.30, 1)
		}
		impact *= schemaAuditDynamicWindowImpact(evidence)
		return schemaAuditClampFloat(impact, 0.35, 1)
	case "SA-E002":
		tailCount, okTail := schemaAuditEvidenceNumber(evidence, "emptyTailCount")
		threshold, okThreshold := schemaAuditEvidenceNumber(evidence, "threshold")
		if okTail && okThreshold && threshold > 0 {
			ratio := tailCount / (2 * threshold)
			return schemaAuditClampFloat(0.35+0.65*ratio, 0.35, 1)
		}
		return 0.65
	case "SA-B001", "SA-B002":
		anomalyCount, okAnomaly := schemaAuditEvidenceNumber(evidence, "anomalyCount")
		validCount, okValid := schemaAuditEvidenceNumber(evidence, "validPartitionCount")
		if okAnomaly && okValid && validCount > 0 {
			ratio := anomalyCount / validCount
			return schemaAuditClampFloat(0.30+0.70*ratio, 0.30, 1)
		}
		return 0.60
	case "SA-B003":
		jumpCount, okJump := schemaAuditEvidenceNumber(evidence, "jumpCount")
		partitionCount, okPartition := schemaAuditEvidenceNumber(evidence, "partitionCount")
		if okJump && okPartition && partitionCount > 1 {
			ratio := jumpCount / (partitionCount - 1)
			return schemaAuditClampFloat(0.30+0.70*ratio, 0.30, 1)
		}
		return 0.60
	case "SA-B004":
		partitionCount, okPartition := schemaAuditEvidenceNumber(evidence, "partitionCount")
		missingBucketCount, okBucket := schemaAuditEvidenceNumber(evidence, "missingBucketCount")
		missingSizeCount, okSize := schemaAuditEvidenceNumber(evidence, "missingSizeCount")
		if okPartition && partitionCount > 0 && okBucket && okSize {
			ratio := math.Max(missingBucketCount, missingSizeCount) / partitionCount
			return schemaAuditClampFloat(0.35+0.65*ratio, 0.35, 1)
		}
		return 0.55
	case "SA-B005", "SA-B006":
		return 1.0
	case "SA-B007":
		avgSize, okAvg := schemaAuditEvidenceNumber(evidence, "averageTabletSizeBytes")
		minSize, okMin := schemaAuditEvidenceNumber(evidence, "recommendedMinBytes")
		maxSize, okMax := schemaAuditEvidenceNumber(evidence, "recommendedMaxBytes")
		if okAvg && okMin && okMax && minSize > 0 && maxSize > minSize {
			ratio := 1.0
			switch {
			case avgSize < minSize:
				ratio = minSize / math.Max(avgSize, 1)
			case avgSize > maxSize:
				ratio = avgSize / maxSize
			}
			distance := math.Min(ratio-1, 2)
			return schemaAuditClampFloat(0.35+0.325*distance, 0.35, 1)
		}
		return 0.50
	case "SA-B009":
		return 0.25
	default:
		return 0.60
	}
}

func schemaAuditDynamicWindowImpact(evidence map[string]any) float64 {
	if len(evidence) == 0 {
		return 1
	}
	windowSpan, okSpan := schemaAuditEvidenceNumber(evidence, "windowSpan")
	warnThreshold, okWarn := schemaAuditEvidenceNumber(evidence, "windowSpanWarn")
	criticalThreshold, okCritical := schemaAuditEvidenceNumber(evidence, "windowSpanCritical")
	if !okSpan || !okWarn || !okCritical || warnThreshold <= 0 || criticalThreshold <= warnThreshold {
		return 1
	}
	if windowSpan <= warnThreshold {
		return 0.8
	}
	if windowSpan >= criticalThreshold {
		return 1
	}
	ratio := (windowSpan - warnThreshold) / (criticalThreshold - warnThreshold)
	return schemaAuditClampFloat(0.8+0.2*ratio, 0.8, 1)
}

func schemaAuditCoverageFactor(evidence map[string]any) float64 {
	if len(evidence) == 0 {
		return 1
	}
	samples, ok := schemaAuditEvidenceNumber(
		evidence,
		"validPartitionCount",
		"totalPartitions",
		"partitionCount",
	)
	if !ok || samples <= 0 {
		return 1
	}
	if samples >= schemaAuditScoreCoverageSampleTarget {
		return 1
	}
	return schemaAuditClampFloat(
		schemaAuditScoreCoverageMinFactor+
			(1-schemaAuditScoreCoverageMinFactor)*(samples/schemaAuditScoreCoverageSampleTarget),
		schemaAuditScoreCoverageMinFactor,
		1,
	)
}

func schemaAuditEvidenceNumber(evidence map[string]any, keys ...string) (float64, bool) {
	if len(evidence) == 0 {
		return 0, false
	}
	for i := range keys {
		raw, ok := evidence[keys[i]]
		if !ok {
			continue
		}
		switch value := raw.(type) {
		case int:
			return float64(value), true
		case int8:
			return float64(value), true
		case int16:
			return float64(value), true
		case int32:
			return float64(value), true
		case int64:
			return float64(value), true
		case uint:
			return float64(value), true
		case uint8:
			return float64(value), true
		case uint16:
			return float64(value), true
		case uint32:
			return float64(value), true
		case uint64:
			return float64(value), true
		case float32:
			return float64(value), true
		case float64:
			return value, true
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func schemaAuditClampFloat(value float64, min float64, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func isDynamicPartitionEnabled(properties map[string]string) bool {
	v, ok := properties["dynamic_partition.enable"]
	if !ok {
		return false
	}
	enabled, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(v)))
	return err == nil && enabled
}

func schemaAuditDynamicWindowSpan(properties map[string]string) (int, bool) {
	if len(properties) == 0 {
		return 0, false
	}
	startRaw, okStart := properties["dynamic_partition.start"]
	endRaw, okEnd := properties["dynamic_partition.end"]
	if !okStart || !okEnd {
		return 0, false
	}
	start, err := strconv.Atoi(strings.TrimSpace(startRaw))
	if err != nil {
		return 0, false
	}
	end, err := strconv.Atoi(strings.TrimSpace(endRaw))
	if err != nil {
		return 0, false
	}
	if end < start {
		return 0, false
	}
	return end - start + 1, true
}

func schemaAuditEffectiveEmptyStatsForPartitions(
	partitions []SchemaAuditPartition,
	properties map[string]string,
) (effectiveTotal int, effectiveEmpty int, evidence map[string]any, futurePartitionClassified bool) {
	total := len(partitions)
	empty := 0
	for i := range partitions {
		if partitions[i].Empty {
			empty++
		}
	}
	excludedFuturePartitions := 0
	excludedFutureEmpty := 0
	potentialFutureWindow := 0
	exclusionSource := "none"
	classified := false
	if isDynamicPartitionEnabled(properties) {
		futurePartitions, futureEmpty, source, ok := schemaAuditCountFuturePartitions(
			partitions,
			properties,
			time.Now(),
		)
		if ok {
			excludedFuturePartitions = futurePartitions
			excludedFutureEmpty = futureEmpty
			exclusionSource = source
			classified = true
		} else if end, ok := schemaAuditDynamicFutureOffset(properties); ok && end > 0 {
			potentialFutureWindow = end
			exclusionSource = "unrecognized_no_exclusion"
		}
	}
	if excludedFuturePartitions > total {
		excludedFuturePartitions = total
	}
	if excludedFutureEmpty > empty {
		excludedFutureEmpty = empty
	}
	return total - excludedFuturePartitions, empty - excludedFutureEmpty, map[string]any{
		"excludedFuturePartitions": excludedFuturePartitions,
		"excludedFutureEmpty":      excludedFutureEmpty,
		"potentialFutureWindow":    potentialFutureWindow,
		"futureExclusionSource":    exclusionSource,
	}, classified
}

func schemaAuditDynamicFutureOffset(properties map[string]string) (int, bool) {
	raw, ok := properties["dynamic_partition.end"]
	if !ok {
		return 0, false
	}
	end, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	if end <= 0 {
		return 0, true
	}
	return end, true
}

func schemaAuditCountFuturePartitions(
	partitions []SchemaAuditPartition,
	properties map[string]string,
	now time.Time,
) (futurePartitions int, futureEmpty int, source string, classified bool) {
	futureFlags, source, classified := schemaAuditClassifyFuturePartitions(
		partitions,
		properties,
		now,
	)
	if !classified {
		return 0, 0, source, false
	}

	futureCount := 0
	futureEmptyCount := 0
	for i := range futureFlags {
		if !futureFlags[i] {
			continue
		}
		futureCount++
		if partitions[i].Empty {
			futureEmptyCount++
		}
	}
	return futureCount, futureEmptyCount, source, true
}

func schemaAuditClassifyFuturePartitions(
	partitions []SchemaAuditPartition,
	properties map[string]string,
	now time.Time,
) (futureFlags []bool, source string, classified bool) {
	prefix := strings.TrimSpace(properties["dynamic_partition.prefix"])
	timeUnit := strings.ToUpper(strings.TrimSpace(properties["dynamic_partition.time_unit"]))
	if timeUnit == "" {
		return nil, "none", false
	}
	startDayOfWeek := schemaAuditDynamicStartDayOfWeek(properties)
	location := schemaAuditDynamicLocation(properties)
	reference := now.In(location)

	if byRange, ok := schemaAuditClassifyFuturePartitionsByRange(
		partitions,
		timeUnit,
		reference,
		location,
		startDayOfWeek,
	); ok {
		return byRange, "partition_range", true
	}
	if byName, ok := schemaAuditClassifyFuturePartitionsByName(
		partitions,
		prefix,
		timeUnit,
		reference,
		location,
		startDayOfWeek,
	); ok {
		return byName, "partition_name", true
	}
	return nil, "none", false
}

func schemaAuditClassifyFuturePartitionsByRange(
	partitions []SchemaAuditPartition,
	timeUnit string,
	reference time.Time,
	location *time.Location,
	startDayOfWeek int,
) ([]bool, bool) {
	flags := make([]bool, len(partitions))
	for i := range partitions {
		isFuture, ok := schemaAuditIsFutureDynamicPartitionRangeLower(
			partitions[i].RangeLower,
			timeUnit,
			reference,
			location,
			startDayOfWeek,
		)
		if !ok {
			return nil, false
		}
		flags[i] = isFuture
	}
	return flags, true
}

func schemaAuditClassifyFuturePartitionsByName(
	partitions []SchemaAuditPartition,
	prefix string,
	timeUnit string,
	reference time.Time,
	location *time.Location,
	startDayOfWeek int,
) ([]bool, bool) {
	flags := make([]bool, len(partitions))
	for i := range partitions {
		isFuture, ok := schemaAuditIsFutureDynamicPartitionName(
			partitions[i].Name,
			prefix,
			timeUnit,
			reference,
			location,
			startDayOfWeek,
		)
		if !ok {
			return nil, false
		}
		flags[i] = isFuture
	}
	return flags, true
}

func schemaAuditIsFutureDynamicPartitionRangeLower(
	rangeLower string,
	timeUnit string,
	reference time.Time,
	location *time.Location,
	startDayOfWeek int,
) (bool, bool) {
	partitionTime, ok := schemaAuditParsePartitionLowerBoundTime(rangeLower, location)
	if !ok {
		return false, false
	}
	referenceLocal := reference.In(location)
	partitionLocal := partitionTime.In(location)

	switch timeUnit {
	case "DAY":
		partitionDay := time.Date(partitionLocal.Year(), partitionLocal.Month(), partitionLocal.Day(), 0, 0, 0, 0, location)
		referenceDay := time.Date(referenceLocal.Year(), referenceLocal.Month(), referenceLocal.Day(), 0, 0, 0, 0, location)
		return partitionDay.After(referenceDay), true
	case "HOUR":
		partitionHour := time.Date(partitionLocal.Year(), partitionLocal.Month(), partitionLocal.Day(), partitionLocal.Hour(), 0, 0, 0, location)
		referenceHour := time.Date(referenceLocal.Year(), referenceLocal.Month(), referenceLocal.Day(), referenceLocal.Hour(), 0, 0, 0, location)
		return partitionHour.After(referenceHour), true
	case "MONTH":
		partitionMonth := time.Date(partitionLocal.Year(), partitionLocal.Month(), 1, 0, 0, 0, 0, location)
		referenceMonth := time.Date(referenceLocal.Year(), referenceLocal.Month(), 1, 0, 0, 0, 0, location)
		return partitionMonth.After(referenceMonth), true
	case "YEAR":
		partitionYear := time.Date(partitionLocal.Year(), time.January, 1, 0, 0, 0, 0, location)
		referenceYear := time.Date(referenceLocal.Year(), time.January, 1, 0, 0, 0, 0, location)
		return partitionYear.After(referenceYear), true
	case "WEEK":
		partitionWeek := schemaAuditStartOfWeek(partitionLocal, startDayOfWeek, location)
		referenceWeek := schemaAuditStartOfWeek(referenceLocal, startDayOfWeek, location)
		return partitionWeek.After(referenceWeek), true
	default:
		return false, false
	}
}

func schemaAuditParsePartitionLowerBoundTime(raw string, location *time.Location) (time.Time, bool) {
	value := schemaAuditPrimaryPartitionLowerBound(strings.TrimSpace(strings.Trim(raw, "\"'")))
	if value == "" {
		return time.Time{}, false
	}

	if !strings.Contains(value, "-") {
		return time.Time{}, false
	}
	if strings.Contains(value, " ") {
		layouts := []string{
			time.DateTime,
			schemaAuditDateTimeNanosLayout,
		}
		for i := range layouts {
			parsed, err := time.ParseInLocation(layouts[i], value, location)
			if err == nil {
				return parsed, true
			}
		}
		return time.Time{}, false
	}

	parsed, err := time.ParseInLocation(time.DateOnly, value, location)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func schemaAuditTimelineConfidence(orderSource string, uncertainFuture bool) float64 {
	if orderSource == "range_lower_partial" {
		if uncertainFuture {
			return 0.55
		}
		return 0.75
	}
	if uncertainFuture {
		return 0.65
	}
	return 0.9
}

func schemaAuditStartOfWeek(ts time.Time, startDayOfWeek int, location *time.Location) time.Time {
	day := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, location)
	weekday := schemaAuditWeekdayToDayOfWeek(day.Weekday())
	startDay := schemaAuditNormalizeStartDayOfWeek(startDayOfWeek)
	offset := (weekday - startDay + 7) % 7
	return day.AddDate(0, 0, -offset)
}

func schemaAuditEffectiveEmptyTailCount(
	ordered []SchemaAuditPartition,
	properties map[string]string,
	now time.Time,
) (count int, source string, classified bool) {
	futureFlags, source, ok := schemaAuditClassifyFuturePartitions(ordered, properties, now)
	if ok {
		tail := 0
		for i := len(ordered) - 1; i >= 0; i-- {
			if futureFlags[i] {
				continue
			}
			if !ordered[i].Empty {
				break
			}
			tail++
		}
		return tail, source, true
	}
	rawTail := 0
	for i := len(ordered) - 1; i >= 0; i-- {
		if !ordered[i].Empty {
			break
		}
		rawTail++
	}
	if end, ok := schemaAuditDynamicFutureOffset(properties); ok && end > 0 {
		return rawTail, "unrecognized_no_exclusion", false
	}
	return rawTail, "none", false
}

func schemaAuditIsFutureDynamicPartitionName(
	partitionName string,
	prefix string,
	timeUnit string,
	reference time.Time,
	location *time.Location,
	startDayOfWeek int,
) (bool, bool) {
	name := strings.TrimSpace(partitionName)
	if prefix != "" {
		if !strings.HasPrefix(name, prefix) {
			return false, false
		}
		name = strings.TrimPrefix(name, prefix)
	}
	if name == "" {
		return false, false
	}
	switch timeUnit {
	case "DAY":
		if len(name) != 8 {
			return false, false
		}
		partitionTime, err := time.ParseInLocation("20060102", name, location)
		if err != nil {
			return false, false
		}
		referenceDay := time.Date(reference.Year(), reference.Month(), reference.Day(), 0, 0, 0, 0, location)
		return partitionTime.After(referenceDay), true
	case "HOUR":
		if len(name) != 10 {
			return false, false
		}
		partitionTime, err := time.ParseInLocation("2006010215", name, location)
		if err != nil {
			return false, false
		}
		referenceHour := reference.Truncate(time.Hour)
		return partitionTime.After(referenceHour), true
	case "MONTH":
		if len(name) != 6 {
			return false, false
		}
		partitionTime, err := time.ParseInLocation("200601", name, location)
		if err != nil {
			return false, false
		}
		referenceMonth := time.Date(reference.Year(), reference.Month(), 1, 0, 0, 0, 0, location)
		return partitionTime.After(referenceMonth), true
	case "YEAR":
		if len(name) != 4 {
			return false, false
		}
		partitionTime, err := time.ParseInLocation("2006", name, location)
		if err != nil {
			return false, false
		}
		referenceYear := time.Date(reference.Year(), time.January, 1, 0, 0, 0, 0, location)
		return partitionTime.After(referenceYear), true
	case "WEEK":
		parts := strings.Split(name, "_")
		if len(parts) != 2 {
			return false, false
		}
		partitionYear, errYear := strconv.Atoi(parts[0])
		partitionWeek, errWeek := strconv.Atoi(parts[1])
		if errYear != nil || errWeek != nil || partitionWeek <= 0 {
			return false, false
		}
		currentYear, currentWeek := schemaAuditWeekPartitionToken(reference, startDayOfWeek, location)
		if partitionYear != currentYear {
			return partitionYear > currentYear, true
		}
		return partitionWeek > currentWeek, true
	default:
		return false, false
	}
}

func schemaAuditDynamicStartDayOfWeek(properties map[string]string) int {
	raw := strings.TrimSpace(properties["dynamic_partition.start_day_of_week"])
	if raw == "" {
		return 1
	}
	day, err := strconv.Atoi(raw)
	if err != nil {
		return 1
	}
	return schemaAuditNormalizeStartDayOfWeek(day)
}

func schemaAuditDynamicLocation(properties map[string]string) *time.Location {
	location := time.Local
	if tz := strings.TrimSpace(properties["dynamic_partition.time_zone"]); tz != "" {
		if loaded, err := time.LoadLocation(tz); err == nil {
			location = loaded
		}
	}
	return location
}

func schemaAuditOrderPartitionsForTimeline(
	partitions []SchemaAuditPartition,
	properties map[string]string,
) ([]SchemaAuditPartition, string) {
	ordered := slices.Clone(partitions)
	if len(ordered) <= 1 {
		return ordered, "input_order"
	}

	location := schemaAuditDynamicLocation(properties)
	type partitionWithLowerBound struct {
		partition SchemaAuditPartition
		lower     time.Time
	}
	withLowerBound := make([]partitionWithLowerBound, 0, len(ordered))
	parsedPositions := make([]int, 0, len(ordered))
	for i := range ordered {
		lower, ok := schemaAuditParsePartitionLowerBoundTime(
			ordered[i].RangeLower,
			location,
		)
		if !ok {
			continue
		}
		withLowerBound = append(withLowerBound, partitionWithLowerBound{
			partition: ordered[i],
			lower:     lower,
		})
		parsedPositions = append(parsedPositions, i)
	}
	if len(withLowerBound) == 0 {
		return ordered, "input_order"
	}

	sort.SliceStable(withLowerBound, func(i, j int) bool {
		left := withLowerBound[i]
		right := withLowerBound[j]
		if left.lower.Before(right.lower) {
			return true
		}
		if left.lower.After(right.lower) {
			return false
		}
		return left.partition.Name < right.partition.Name
	})

	if len(withLowerBound) < len(ordered) {
		for i := range withLowerBound {
			ordered[parsedPositions[i]] = withLowerBound[i].partition
		}
		return ordered, "range_lower_partial"
	}

	for i := range withLowerBound {
		ordered[i] = withLowerBound[i].partition
	}
	return ordered, "range_lower"
}

func schemaAuditWeekPartitionToken(reference time.Time, startDayOfWeek int, location *time.Location) (int, int) {
	localReference := reference.In(location)
	week := schemaAuditWeekOfYear(localReference, startDayOfWeek, location)
	if week <= 1 && localReference.Month() >= time.December {
		week += 52
	}
	return localReference.Year(), week
}

func schemaAuditWeekOfYear(day time.Time, startDayOfWeek int, location *time.Location) int {
	startDay := schemaAuditNormalizeStartDayOfWeek(startDayOfWeek)
	normalizedDay := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, location)
	jan1 := time.Date(normalizedDay.Year(), time.January, 1, 0, 0, 0, 0, location)
	jan1Day := schemaAuditWeekdayToDayOfWeek(jan1.Weekday())
	offset := (jan1Day - startDay + 7) % 7
	return (normalizedDay.YearDay()+offset-1)/7 + 1
}

func schemaAuditWeekdayToDayOfWeek(weekday time.Weekday) int {
	if weekday == time.Sunday {
		return 7
	}
	return int(weekday)
}

func schemaAuditNormalizeStartDayOfWeek(day int) int {
	if day < 1 || day > 7 {
		return 1
	}
	return day
}
