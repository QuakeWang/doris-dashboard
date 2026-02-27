package doris

type SchemaAuditScanOptions struct {
	Database  string
	TableLike string
	Page      int
	PageSize  int
}

type SchemaAuditInventory struct {
	DatabaseCount              int     `json:"databaseCount"`
	TableCount                 int     `json:"tableCount"`
	PartitionedTableCount      int     `json:"partitionedTableCount"`
	TotalPartitionCount        int     `json:"totalPartitionCount"`
	EmptyPartitionCount        int     `json:"emptyPartitionCount"`
	EmptyPartitionRatio        float64 `json:"emptyPartitionRatio"`
	DynamicPartitionTableCount int     `json:"dynamicPartitionTableCount"`
}

type SchemaAuditFindingSummary struct {
	RuleID   string `json:"ruleId"`
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
}

type SchemaAuditScanItem struct {
	Database                string                      `json:"database"`
	Table                   string                      `json:"table"`
	PartitionCount          int                         `json:"partitionCount"`
	EmptyPartitionCount     int                         `json:"emptyPartitionCount"`
	EmptyPartitionRatio     float64                     `json:"emptyPartitionRatio"`
	DynamicPartitionEnabled bool                        `json:"dynamicPartitionEnabled"`
	Score                   int                         `json:"score"`
	FindingCount            int                         `json:"findingCount"`
	Findings                []SchemaAuditFindingSummary `json:"findings"`
}

type SchemaAuditScanResult struct {
	Inventory  SchemaAuditInventory  `json:"inventory"`
	Items      []SchemaAuditScanItem `json:"items"`
	Page       int                   `json:"page"`
	PageSize   int                   `json:"pageSize"`
	TotalItems int                   `json:"totalItems"`
	Truncated  bool                  `json:"truncated"`
	ScanLimit  int                   `json:"scanLimit"`
	Warning    string                `json:"warning,omitempty"`
}

type SchemaAuditFinding struct {
	RuleID         string         `json:"ruleId"`
	Severity       string         `json:"severity"`
	Confidence     float64        `json:"confidence"`
	Summary        string         `json:"summary"`
	Evidence       map[string]any `json:"evidence"`
	Recommendation string         `json:"recommendation,omitempty"`
}

type SchemaAuditPartition struct {
	Name          string `json:"name"`
	Rows          uint64 `json:"rows"`
	DataSizeBytes uint64 `json:"dataSizeBytes"`
	Buckets       int    `json:"buckets"`
	Empty         bool   `json:"empty"`
	RangeLower    string `json:"-"`
}

type SchemaAuditIndex struct {
	Name      string   `json:"name"`
	IndexType string   `json:"indexType"`
	Columns   []string `json:"columns"`
}

type SchemaAuditTableDetailResult struct {
	Database          string                 `json:"database"`
	Table             string                 `json:"table"`
	CreateTableSQL    string                 `json:"createTableSql"`
	DynamicProperties map[string]string      `json:"dynamicProperties"`
	Partitions        []SchemaAuditPartition `json:"partitions"`
	Indexes           []SchemaAuditIndex     `json:"indexes"`
	Findings          []SchemaAuditFinding   `json:"findings"`
}
