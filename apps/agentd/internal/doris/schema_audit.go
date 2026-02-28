package doris

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	schemaAuditDefaultPage     = 1
	schemaAuditDefaultPageSize = 50
	schemaAuditMaxPageSize     = 200

	schemaAuditScanLimitDefault  = 5000
	schemaAuditScanLimitFiltered = 20000
)

const schemaAuditSystemDatabasePredicate = "table_schema NOT IN ('information_schema','mysql','performance_schema','sys','__internal_schema')"

var dynamicPartitionPropertyPattern = regexp.MustCompile(`(?i)["'](dynamic_partition\.[^"']+)["']\s*=\s*["']([^"']*)["']`)
var schemaAuditPartitionRangeLowerBoundPattern = regexp.MustCompile(`(?i)keys:\s*\[([^\]]+)\]`)

var schemaAuditScanDynamicPropertyColumns = []struct {
	Property string
	Column   string
}{
	{Property: "dynamic_partition.enable", Column: "dynamic_partition_enable"},
	{Property: "dynamic_partition.start", Column: "dynamic_partition_start"},
	{Property: "dynamic_partition.end", Column: "dynamic_partition_end"},
	{Property: "dynamic_partition.buckets", Column: "dynamic_partition_buckets"},
	{Property: "dynamic_partition.time_unit", Column: "dynamic_partition_time_unit"},
	{Property: "dynamic_partition.prefix", Column: "dynamic_partition_prefix"},
	{Property: "dynamic_partition.time_zone", Column: "dynamic_partition_time_zone"},
	{Property: "dynamic_partition.start_day_of_week", Column: "dynamic_partition_start_day_of_week"},
}

type schemaAuditTableKey struct {
	Database string
	Table    string
}

type schemaAuditPartitionSummary struct {
	PartitionCount      int
	EmptyPartitionCount int
}

type schemaAuditScanRow struct {
	Key               schemaAuditTableKey
	PartitionSummary  schemaAuditPartitionSummary
	DynamicProperties map[string]string
}

type schemaAuditScanCollection struct {
	Rows      []schemaAuditScanRow
	ScanLimit int
	Truncated bool
}

func BuildSchemaAuditScan(
	ctx context.Context,
	cfg ConnConfig,
	opts SchemaAuditScanOptions,
) (SchemaAuditScanResult, error) {
	normalized := SchemaAuditScanOptions{
		Database:  strings.TrimSpace(opts.Database),
		TableLike: strings.TrimSpace(opts.TableLike),
		Page:      opts.Page,
		PageSize:  opts.PageSize,
	}
	cfg.Database = ""

	db, err := openAndPing(ctx, cfg)
	if err != nil {
		return SchemaAuditScanResult{}, err
	}
	defer db.Close()

	scanCollection, err := collectSchemaAuditScanRows(ctx, db, normalized)
	if err != nil {
		return SchemaAuditScanResult{}, err
	}
	scanRows := scanCollection.Rows

	databaseSet := make(map[string]struct{}, len(scanRows))
	items := make([]SchemaAuditScanItem, 0, len(scanRows))
	inventory := SchemaAuditInventory{
		TableCount: len(scanRows),
	}
	for i := range scanRows {
		key := scanRows[i].Key
		databaseSet[key.Database] = struct{}{}

		partitionSummary := scanRows[i].PartitionSummary
		if partitionSummary.PartitionCount > 0 {
			inventory.PartitionedTableCount++
		}
		inventory.TotalPartitionCount += partitionSummary.PartitionCount
		inventory.EmptyPartitionCount += partitionSummary.EmptyPartitionCount

		dynamicProperties := scanRows[i].DynamicProperties
		dynamicPartitionEnabled := isDynamicPartitionEnabled(dynamicProperties)
		if dynamicPartitionEnabled {
			inventory.DynamicPartitionTableCount++
		}

		findings := evaluateSchemaAuditScanFindings(partitionSummary, dynamicProperties)
		items = append(items, SchemaAuditScanItem{
			Database:                key.Database,
			Table:                   key.Table,
			PartitionCount:          partitionSummary.PartitionCount,
			EmptyPartitionCount:     partitionSummary.EmptyPartitionCount,
			EmptyPartitionRatio:     ratio(partitionSummary.EmptyPartitionCount, partitionSummary.PartitionCount),
			DynamicPartitionEnabled: dynamicPartitionEnabled,
			Score:                   computeSchemaAuditScore(findings),
			FindingCount:            len(findings),
			Findings:                summarizeSchemaAuditFindings(findings),
		})
	}
	inventory.DatabaseCount = len(databaseSet)
	inventory.EmptyPartitionRatio = ratio(inventory.EmptyPartitionCount, inventory.TotalPartitionCount)

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		if items[i].FindingCount != items[j].FindingCount {
			return items[i].FindingCount > items[j].FindingCount
		}
		if items[i].Database != items[j].Database {
			return items[i].Database < items[j].Database
		}
		return items[i].Table < items[j].Table
	})

	page, pageSize := normalizePagination(normalized.Page, normalized.PageSize)
	page = clampSchemaAuditPage(page, pageSize, len(items))
	pagedItems := paginateSchemaAuditItems(items, page, pageSize)

	return SchemaAuditScanResult{
		Inventory:  inventory,
		Items:      pagedItems,
		Page:       page,
		PageSize:   pageSize,
		TotalItems: len(items),
		Truncated:  scanCollection.Truncated,
		ScanLimit:  scanCollection.ScanLimit,
		Warning:    schemaAuditScanWarning(scanCollection),
	}, nil
}

func BuildSchemaAuditTableDetail(
	ctx context.Context,
	cfg ConnConfig,
	database string,
	table string,
) (SchemaAuditTableDetailResult, error) {
	normalizedDatabase, err := validateSchemaAuditIdentifier(database, "database")
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}
	normalizedTable, err := validateSchemaAuditIdentifier(table, "table")
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}

	cfg.Database = ""
	db, err := openAndPing(ctx, cfg)
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}
	defer db.Close()

	createTableSQL, err := showSchemaAuditCreateTableSQL(ctx, db, normalizedDatabase, normalizedTable)
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}

	dynamicProperties := parseDynamicPartitionPropertiesFromCreateTable(createTableSQL)
	tableProperties, err := collectSchemaAuditDynamicPropertiesForTable(
		ctx,
		db,
		normalizedDatabase,
		normalizedTable,
	)
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}
	for k, v := range tableProperties {
		dynamicProperties[k] = v
	}

	partitions, err := showSchemaAuditPartitions(ctx, db, normalizedDatabase, normalizedTable)
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}
	indexes, err := showSchemaAuditIndexes(ctx, db, normalizedDatabase, normalizedTable)
	if err != nil {
		return SchemaAuditTableDetailResult{}, err
	}

	bucketRuleConfig := defaultSchemaAuditBucketRuleConfig()
	findings := evaluateSchemaAuditTableDetailFindings(
		partitions,
		dynamicProperties,
		createTableSQL,
		bucketRuleConfig,
	)
	return SchemaAuditTableDetailResult{
		Database:          normalizedDatabase,
		Table:             normalizedTable,
		CreateTableSQL:    createTableSQL,
		DynamicProperties: dynamicProperties,
		Partitions:        partitions,
		Indexes:           indexes,
		Findings:          findings,
	}, nil
}

func normalizePagination(page int, pageSize int) (int, int) {
	normalizedPage := page
	if normalizedPage <= 0 {
		normalizedPage = schemaAuditDefaultPage
	}
	normalizedPageSize := pageSize
	if normalizedPageSize <= 0 {
		normalizedPageSize = schemaAuditDefaultPageSize
	}
	if normalizedPageSize > schemaAuditMaxPageSize {
		normalizedPageSize = schemaAuditMaxPageSize
	}
	return normalizedPage, normalizedPageSize
}

func clampSchemaAuditPage(page int, pageSize int, totalItems int) int {
	if totalItems <= 0 || pageSize <= 0 {
		return schemaAuditDefaultPage
	}
	if page < schemaAuditDefaultPage {
		return schemaAuditDefaultPage
	}
	maxPage := (totalItems-1)/pageSize + 1
	if page > maxPage {
		return maxPage
	}
	return page
}

func paginateSchemaAuditItems(
	items []SchemaAuditScanItem,
	page int,
	pageSize int,
) []SchemaAuditScanItem {
	if len(items) == 0 {
		return []SchemaAuditScanItem{}
	}
	if page <= 0 || pageSize <= 0 {
		return []SchemaAuditScanItem{}
	}
	// Prevent integer overflow on (page-1)*pageSize for extremely large page values.
	if page > 1 && (page-1) > (math.MaxInt/pageSize) {
		return []SchemaAuditScanItem{}
	}
	start := (page - 1) * pageSize
	if start < 0 || start >= len(items) {
		return []SchemaAuditScanItem{}
	}
	end := start + pageSize
	if end < start {
		return []SchemaAuditScanItem{}
	}
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}

func collectSchemaAuditScanRows(
	ctx context.Context,
	db *sql.DB,
	opts SchemaAuditScanOptions,
) (schemaAuditScanCollection, error) {
	tableFilters := buildSchemaAuditFiltersForAlias(opts, "t")
	scanLimit := resolveSchemaAuditScanLimit(opts)
	queryLimit := 0
	if scanLimit > 0 {
		queryLimit = scanLimit + 1
	}

	query := buildSchemaAuditScanQuery(tableFilters, true, queryLimit)
	rows, _, err := queryRowsAsStringMaps(ctx, db, query)
	if err != nil && isSchemaAuditOptionalMetadataError(err) {
		query = buildSchemaAuditScanQuery(tableFilters, false, queryLimit)
		rows, _, err = queryRowsAsStringMaps(ctx, db, query)
	}
	if err != nil {
		return schemaAuditScanCollection{}, err
	}

	out := make([]schemaAuditScanRow, 0, len(rows))
	for i := range rows {
		row := rows[i]
		database := strings.TrimSpace(firstNonEmptyValue(row, "table_schema"))
		table := strings.TrimSpace(firstNonEmptyValue(row, "table_name"))
		if database == "" || table == "" {
			continue
		}

		partitionCount, _ := parseIntLoose(firstNonEmptyValue(row, "partition_count"))
		emptyPartitionCount, _ := parseIntLoose(firstNonEmptyValue(row, "empty_partition_count"))
		if partitionCount < 0 {
			partitionCount = 0
		}
		if emptyPartitionCount < 0 {
			emptyPartitionCount = 0
		}
		if emptyPartitionCount > partitionCount {
			emptyPartitionCount = partitionCount
		}
		partitionSummary := schemaAuditPartitionSummary{
			PartitionCount:      partitionCount,
			EmptyPartitionCount: emptyPartitionCount,
		}

		dynamicProperties := collectSchemaAuditDynamicPropertiesFromScanRow(row)

		out = append(out, schemaAuditScanRow{
			Key: schemaAuditTableKey{
				Database: database,
				Table:    table,
			},
			PartitionSummary:  partitionSummary,
			DynamicProperties: dynamicProperties,
		})
	}
	truncated := false
	if scanLimit > 0 && len(out) > scanLimit {
		out = out[:scanLimit]
		truncated = true
	}
	return schemaAuditScanCollection{
		Rows:      out,
		ScanLimit: scanLimit,
		Truncated: truncated,
	}, nil
}

func collectSchemaAuditDynamicPropertiesFromScanRow(row map[string]string) map[string]string {
	properties := make(map[string]string, len(schemaAuditScanDynamicPropertyColumns))
	for i := range schemaAuditScanDynamicPropertyColumns {
		column := schemaAuditScanDynamicPropertyColumns[i]
		value := strings.TrimSpace(firstNonEmptyValue(row, column.Column))
		if value == "" {
			continue
		}
		properties[column.Property] = value
	}
	if len(properties) == 0 {
		return nil
	}
	return properties
}

func buildSchemaAuditScanDynamicSelect(includeDynamicProperties bool) string {
	projections := make([]string, 0, len(schemaAuditScanDynamicPropertyColumns))
	for i := range schemaAuditScanDynamicPropertyColumns {
		column := schemaAuditScanDynamicPropertyColumns[i].Column
		if includeDynamicProperties {
			projections = append(
				projections,
				fmt.Sprintf("COALESCE(dp.%s, '') AS %s", column, column),
			)
			continue
		}
		projections = append(projections, fmt.Sprintf("'' AS %s", column))
	}
	return strings.Join(projections, ", ") + " "
}

func buildSchemaAuditScanDynamicPropertiesCTE() string {
	selectItems := make([]string, 0, len(schemaAuditScanDynamicPropertyColumns)+2)
	selectItems = append(selectItems, "tp.table_schema", "tp.table_name")
	for i := range schemaAuditScanDynamicPropertyColumns {
		property := schemaAuditScanDynamicPropertyColumns[i].Property
		column := schemaAuditScanDynamicPropertyColumns[i].Column
		selectItems = append(
			selectItems,
			fmt.Sprintf(
				"MAX(CASE WHEN LOWER(tp.property_name) = '%s' THEN tp.property_value END) AS %s",
				property,
				column,
			),
		)
	}

	return ", dynamic_properties AS (" +
		"SELECT " + strings.Join(selectItems, ", ") + " " +
		"FROM information_schema.table_properties tp " +
		"INNER JOIN candidates c ON c.table_schema = tp.table_schema AND c.table_name = tp.table_name " +
		"WHERE tp.property_name LIKE 'dynamic_partition.%' " +
		"GROUP BY tp.table_schema, tp.table_name" +
		") "
}

func buildSchemaAuditScanQuery(
	tableFilters string,
	includeDynamicProperties bool,
	rowLimit int,
) string {
	candidatesQuery := "" +
		"SELECT t.table_schema, t.table_name " +
		"FROM information_schema.tables t " +
		"WHERE t.table_type = 'BASE TABLE' " +
		"AND (t.engine = 'Doris' OR t.engine = 'OLAP') " +
		"AND " + schemaAuditSystemDatabasePredicate +
		tableFilters +
		" ORDER BY t.table_schema, t.table_name"
	if rowLimit > 0 {
		candidatesQuery += fmt.Sprintf(" LIMIT %d", rowLimit)
	}

	partitionSummaryQuery := "" +
		"SELECT p.table_schema, p.table_name, " +
		"COUNT(p.partition_name) AS partition_count, " +
		"SUM(CASE WHEN p.partition_name IS NOT NULL AND p.data_length = 0 AND (p.table_rows IS NULL OR p.table_rows = 0) THEN 1 ELSE 0 END) AS empty_partition_count " +
		"FROM information_schema.partitions p " +
		"INNER JOIN candidates c ON c.table_schema = p.table_schema AND c.table_name = p.table_name " +
		"GROUP BY p.table_schema, p.table_name"

	dynamicSelect := buildSchemaAuditScanDynamicSelect(false)
	dynamicCTE := ""
	dynamicJoin := ""
	priorityOrder := buildSchemaAuditScanPriorityOrder(false)
	if includeDynamicProperties {
		dynamicSelect = buildSchemaAuditScanDynamicSelect(true)
		dynamicCTE = buildSchemaAuditScanDynamicPropertiesCTE()
		dynamicJoin = "" +
			"LEFT JOIN dynamic_properties dp ON dp.table_schema = candidates.table_schema AND dp.table_name = candidates.table_name "
		priorityOrder = buildSchemaAuditScanPriorityOrder(true)
	}

	query := "" +
		"WITH candidates AS (" + candidatesQuery + "), " +
		"partition_summary AS (" + partitionSummaryQuery + ") " +
		dynamicCTE +
		"SELECT candidates.table_schema, candidates.table_name, " +
		"COALESCE(ps.partition_count, 0) AS partition_count, " +
		"COALESCE(ps.empty_partition_count, 0) AS empty_partition_count, " +
		dynamicSelect +
		"FROM candidates " +
		"LEFT JOIN partition_summary ps ON ps.table_schema = candidates.table_schema AND ps.table_name = candidates.table_name " +
		dynamicJoin +
		priorityOrder
	return query
}

func buildSchemaAuditScanPriorityOrder(includeDynamicProperties bool) string {
	dynamicEnabledSortExpr := "0"
	if includeDynamicProperties {
		dynamicEnabledSortExpr = "CASE WHEN LOWER(COALESCE(dp.dynamic_partition_enable, '')) IN ('true', '1') THEN 1 ELSE 0 END"
	}
	return " ORDER BY " +
		"CASE " +
		"WHEN COALESCE(ps.partition_count, 0) > 0 THEN CAST(COALESCE(ps.empty_partition_count, 0) AS DOUBLE) / COALESCE(ps.partition_count, 1) " +
		"ELSE 0 " +
		"END DESC, " +
		"COALESCE(ps.empty_partition_count, 0) DESC, " +
		dynamicEnabledSortExpr + " DESC, " +
		"candidates.table_schema, candidates.table_name"
}

func resolveSchemaAuditScanLimit(opts SchemaAuditScanOptions) int {
	if strings.TrimSpace(opts.Database) != "" || strings.TrimSpace(opts.TableLike) != "" {
		return schemaAuditScanLimitFiltered
	}
	return schemaAuditScanLimitDefault
}

func schemaAuditScanWarning(collection schemaAuditScanCollection) string {
	if !collection.Truncated || collection.ScanLimit <= 0 {
		return ""
	}
	return fmt.Sprintf(
		"Schema audit result is truncated to first %d candidate tables (schema/table order), then ranked by empty-partition risk.",
		collection.ScanLimit,
	)
}

func collectSchemaAuditDynamicPropertiesForTable(
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
) (map[string]string, error) {
	query := "" +
		"SELECT property_name, property_value " +
		"FROM information_schema.table_properties " +
		"WHERE table_schema = " + quoteSchemaAuditStringLiteral(database) +
		" AND table_name = " + quoteSchemaAuditStringLiteral(table) + " " +
		"AND property_name LIKE 'dynamic_partition.%'"

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		if isSchemaAuditOptionalMetadataError(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	properties := make(map[string]string, 8)
	for rows.Next() {
		var propertyName sql.NullString
		var propertyValue sql.NullString
		if err := rows.Scan(&propertyName, &propertyValue); err != nil {
			return nil, err
		}
		if !propertyName.Valid {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(propertyName.String))
		if name == "" {
			continue
		}
		properties[name] = strings.TrimSpace(nullStringValue(propertyValue))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return properties, nil
}

func showSchemaAuditCreateTableSQL(
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
) (string, error) {
	query := fmt.Sprintf(
		"SHOW CREATE TABLE %s.%s",
		quoteSchemaAuditIdentifier(database),
		quoteSchemaAuditIdentifier(table),
	)
	rows, columns, err := queryRowsAsStringMaps(ctx, db, query)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errors.New("unexpected SHOW CREATE TABLE result: no rows")
	}
	row := rows[0]

	if createSQL := firstNonEmptyValue(row, "create table", "create_table"); createSQL != "" {
		return createSQL, nil
	}
	if len(columns) >= 2 {
		if createSQL := strings.TrimSpace(row[columns[1]]); createSQL != "" {
			return createSQL, nil
		}
	}
	return "", errors.New("unexpected SHOW CREATE TABLE result: missing create sql")
}

func showSchemaAuditPartitions(
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
) ([]SchemaAuditPartition, error) {
	query := fmt.Sprintf(
		"SHOW PARTITIONS FROM %s.%s",
		quoteSchemaAuditIdentifier(database),
		quoteSchemaAuditIdentifier(table),
	)
	rows, _, err := queryRowsAsStringMaps(ctx, db, query)
	if err != nil {
		return nil, err
	}

	partitions := make([]SchemaAuditPartition, 0, len(rows))
	for i := range rows {
		row := rows[i]
		name := strings.TrimSpace(firstNonEmptyValue(row, "partitionname", "partition_name", "partition"))
		if name == "" {
			continue
		}

		rowCountValue := firstNonEmptyValue(row, "rowcount", "rows", "table_rows")
		rowCount, hasRowCount := parseUint64Loose(rowCountValue)

		dataSizeValue := firstNonEmptyValue(row, "datasize", "data_size", "data_length")
		dataSizeBytes, hasDataSize := parseByteSize(dataSizeValue)
		rangeLower := parseSchemaAuditPartitionRangeLowerBound(
			firstNonEmptyValue(row, "range"),
		)

		buckets := 0
		if parsedBuckets, ok := parseIntLoose(firstNonEmptyValue(row, "buckets", "bucket_num", "bucketnum")); ok {
			buckets = parsedBuckets
		}

		empty := false
		if hasRowCount && hasDataSize {
			empty = rowCount == 0 && dataSizeBytes == 0
		} else if hasDataSize {
			empty = dataSizeBytes == 0
		}

		partitions = append(partitions, SchemaAuditPartition{
			Name:          name,
			Rows:          rowCount,
			DataSizeBytes: dataSizeBytes,
			Buckets:       buckets,
			Empty:         empty,
			RangeLower:    rangeLower,
		})
	}
	return partitions, nil
}

func parseSchemaAuditPartitionRangeLowerBound(raw string) string {
	rangeValue := strings.TrimSpace(raw)
	if rangeValue == "" {
		return ""
	}
	matches := schemaAuditPartitionRangeLowerBoundPattern.FindStringSubmatch(rangeValue)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(strings.Trim(schemaAuditPrimaryPartitionLowerBound(matches[1]), "\"'"))
}

func schemaAuditPrimaryPartitionLowerBound(raw string) string {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return ""
	}
	if comma := strings.Index(normalized, ","); comma >= 0 {
		normalized = normalized[:comma]
	}
	return strings.TrimSpace(normalized)
}

func showSchemaAuditIndexes(
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
) ([]SchemaAuditIndex, error) {
	query := fmt.Sprintf(
		"SHOW INDEX FROM %s.%s",
		quoteSchemaAuditIdentifier(database),
		quoteSchemaAuditIdentifier(table),
	)
	rows, _, err := queryRowsAsStringMaps(ctx, db, query)
	if err != nil {
		return nil, err
	}

	type columnRef struct {
		seq    int
		column string
	}
	type indexAggregate struct {
		name      string
		indexType string
		columns   []columnRef
	}

	order := make([]string, 0, len(rows))
	aggregates := make(map[string]*indexAggregate, len(rows))
	for i := range rows {
		row := rows[i]
		indexName := strings.TrimSpace(firstNonEmptyValue(row, "key_name", "index_name", "indexname", "name"))
		if indexName == "" {
			continue
		}
		key := strings.ToLower(indexName)
		aggregate, ok := aggregates[key]
		if !ok {
			aggregate = &indexAggregate{name: indexName}
			aggregates[key] = aggregate
			order = append(order, key)
		}
		if aggregate.indexType == "" {
			aggregate.indexType = strings.TrimSpace(firstNonEmptyValue(row, "index_type", "indextype", "type"))
		}
		columnName := strings.TrimSpace(firstNonEmptyValue(row, "column_name", "columnname"))
		if columnName == "" {
			continue
		}
		seq := len(aggregate.columns) + 1
		if parsed, ok := parseIntLoose(firstNonEmptyValue(row, "seq_in_index", "seqinindex", "seq")); ok && parsed > 0 {
			seq = parsed
		}
		aggregate.columns = append(aggregate.columns, columnRef{
			seq:    seq,
			column: columnName,
		})
	}

	indexes := make([]SchemaAuditIndex, 0, len(order))
	for i := range order {
		aggregate := aggregates[order[i]]
		sort.SliceStable(aggregate.columns, func(a, b int) bool {
			if aggregate.columns[a].seq != aggregate.columns[b].seq {
				return aggregate.columns[a].seq < aggregate.columns[b].seq
			}
			return aggregate.columns[a].column < aggregate.columns[b].column
		})
		columns := make([]string, 0, len(aggregate.columns))
		for j := range aggregate.columns {
			if len(columns) > 0 && columns[len(columns)-1] == aggregate.columns[j].column {
				continue
			}
			columns = append(columns, aggregate.columns[j].column)
		}
		indexes = append(indexes, SchemaAuditIndex{
			Name:      aggregate.name,
			IndexType: aggregate.indexType,
			Columns:   columns,
		})
	}
	return indexes, nil
}

func queryRowsAsStringMaps(
	ctx context.Context,
	db *sql.DB,
	query string,
) ([]map[string]string, []string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	lowerColumns := make([]string, len(columns))
	for i := range columns {
		lowerColumns[i] = strings.ToLower(columns[i])
	}

	raw := make([]sql.RawBytes, len(columns))
	dest := make([]any, len(columns))
	for i := range raw {
		dest[i] = &raw[i]
	}

	out := make([]map[string]string, 0, 64)
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, nil, err
		}
		item := make(map[string]string, len(columns))
		for i := range lowerColumns {
			if raw[i] == nil {
				item[lowerColumns[i]] = ""
				continue
			}
			item[lowerColumns[i]] = string(raw[i])
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return out, lowerColumns, nil
}

func parseDynamicPartitionPropertiesFromCreateTable(createTableSQL string) map[string]string {
	properties := make(map[string]string, 8)
	matches := dynamicPartitionPropertyPattern.FindAllStringSubmatch(createTableSQL, -1)
	for i := range matches {
		if len(matches[i]) < 3 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(matches[i][1]))
		value := strings.TrimSpace(matches[i][2])
		if key == "" {
			continue
		}
		properties[key] = value
	}
	return properties
}

func evaluateSchemaAuditScanFindings(
	summary schemaAuditPartitionSummary,
	dynamicProperties map[string]string,
) []SchemaAuditFinding {
	if summary.PartitionCount == 0 {
		return nil
	}

	emptyRatio := ratio(summary.EmptyPartitionCount, summary.PartitionCount)
	futureWindow, hasFutureWindow := schemaAuditDynamicFutureOffset(dynamicProperties)
	futureUncertain := isDynamicPartitionEnabled(dynamicProperties) && hasFutureWindow && futureWindow > 0
	findings := make([]SchemaAuditFinding, 0, 2)
	dynamicWindowSpan, hasDynamicWindowSpan := schemaAuditDynamicWindowSpan(dynamicProperties)

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
				"totalPartitions":          summary.PartitionCount,
				"emptyPartitions":          summary.EmptyPartitionCount,
				"emptyRatio":               emptyRatio,
				"futurePartitionUncertain": futureUncertain,
				"potentialFutureWindow":    futureWindow,
				"futureExclusionSource":    "scan_summary_no_exclusion",
				"warnThreshold":            schemaAuditEmptyRatioWarn,
				"criticalThreshold":        schemaAuditEmptyRatioCritical,
				"partitionTailKnown":       false,
			},
			Recommendation: "Reduce dynamic partition window and clean long-term empty partitions.",
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
			"totalPartitions":          summary.PartitionCount,
			"emptyPartitions":          summary.EmptyPartitionCount,
			"futurePartitionUncertain": futureUncertain,
			"potentialFutureWindow":    futureWindow,
			"futureExclusionSource":    "scan_summary_no_exclusion",
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

func buildSchemaAuditFiltersForAlias(opts SchemaAuditScanOptions, alias string) string {
	prefix := strings.TrimSpace(alias)
	if prefix != "" {
		prefix += "."
	}
	return buildSchemaAuditFiltersWithColumns(opts, prefix+"table_schema", prefix+"table_name")
}

func buildSchemaAuditFiltersWithColumns(
	opts SchemaAuditScanOptions,
	databaseColumn string,
	tableColumn string,
) string {
	filters := make([]string, 0, 2)
	if database := strings.TrimSpace(opts.Database); database != "" {
		filters = append(filters, databaseColumn+" = "+quoteSchemaAuditStringLiteral(database))
	}
	if opts.TableLike != "" {
		pattern := normalizeSchemaAuditLikePattern(opts.TableLike)
		filters = append(filters, tableColumn+" LIKE "+quoteSchemaAuditStringLiteral(pattern))
	}
	if len(filters) == 0 {
		return ""
	}
	return " AND " + strings.Join(filters, " AND ")
}

func normalizeSchemaAuditLikePattern(pattern string) string {
	trimmed := strings.TrimSpace(pattern)
	if trimmed == "" {
		return "%"
	}
	if strings.ContainsAny(trimmed, "%_") {
		return trimmed
	}
	return "%" + trimmed + "%"
}

func quoteSchemaAuditStringLiteral(value string) string {
	escaped := strings.ReplaceAll(value, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "'", "''")
	return "'" + escaped + "'"
}

func validateSchemaAuditIdentifier(value string, fieldName string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("%s is required", fieldName)
	}
	if strings.ContainsAny(trimmed, "`;\r\n\t") {
		return "", fmt.Errorf("%s is invalid", fieldName)
	}
	return trimmed, nil
}

func quoteSchemaAuditIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func parseIntLoose(raw string) (int, bool) {
	v, ok := parseUint64Loose(raw)
	if !ok {
		return 0, false
	}
	if v > math.MaxInt {
		return math.MaxInt, true
	}
	return int(v), true
}

func parseUint64Loose(raw string) (uint64, bool) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return 0, false
	}
	normalized = strings.ReplaceAll(normalized, ",", "")
	if i, err := strconv.ParseUint(normalized, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(normalized, 64); err == nil {
		if f < 0 {
			return 0, true
		}
		if f > math.MaxUint64 {
			return math.MaxUint64, true
		}
		return uint64(f), true
	}
	return 0, false
}

func parseByteSize(raw string) (uint64, bool) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return 0, false
	}
	normalized = strings.ReplaceAll(normalized, ",", "")

	fields := strings.Fields(normalized)
	numberPart := ""
	unit := "B"
	if len(fields) == 1 {
		num, maybeUnit := splitLeadingNumber(fields[0])
		numberPart = num
		if maybeUnit != "" {
			unit = maybeUnit
		}
	} else {
		numberPart = fields[0]
		unit = fields[1]
	}
	if numberPart == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(numberPart, 64)
	if err != nil {
		return 0, false
	}
	if value < 0 {
		return 0, true
	}

	multiplier, ok := resolveByteUnitMultiplier(unit)
	if !ok {
		return 0, false
	}
	bytes := value * multiplier
	if bytes >= math.MaxUint64 {
		return math.MaxUint64, true
	}
	return uint64(bytes), true
}

func splitLeadingNumber(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	i := 0
	for i < len(raw) {
		ch := raw[i]
		if (ch >= '0' && ch <= '9') || ch == '.' || ch == '-' || ch == '+' {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return "", raw
	}
	return raw[:i], raw[i:]
}

func resolveByteUnitMultiplier(rawUnit string) (float64, bool) {
	unit := strings.ToUpper(strings.TrimSpace(rawUnit))
	switch unit {
	case "", "B":
		return 1, true
	case "K", "KB", "KIB":
		return 1024, true
	case "M", "MB", "MIB":
		return 1024 * 1024, true
	case "G", "GB", "GIB":
		return 1024 * 1024 * 1024, true
	case "T", "TB", "TIB":
		return 1024 * 1024 * 1024 * 1024, true
	case "P", "PB", "PIB":
		return 1024 * 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func ratio(numerator int, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func firstNonEmptyValue(row map[string]string, keys ...string) string {
	for i := range keys {
		value := strings.TrimSpace(row[strings.ToLower(keys[i])])
		if value == "" {
			continue
		}
		return value
	}
	return ""
}

func nullStringValue(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func isSchemaAuditOptionalMetadataError(err error) bool {
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "information_schema.table_properties") &&
		(strings.Contains(text, "doesn't exist") ||
			strings.Contains(text, "does not exist") ||
			strings.Contains(text, "unknown table") ||
			strings.Contains(text, "unknown column")) {
		return true
	}
	return false
}
