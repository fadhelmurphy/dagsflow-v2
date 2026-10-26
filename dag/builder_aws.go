package dag

// Builder palette entries of the Athena and Redshift nodes.

func init() { Nodes = append(Nodes, awsWarehouseNodes()...) }

func athenaConnFields() []NodeField {
	return []NodeField{
		{Name: "database", Label: "Database", Kind: "text", Help: "Glue database for table names without one"},
		{Name: "workgroup", Label: "Workgroup", Kind: "text", Placeholder: "primary"},
		{Name: "output_location", Label: "Query results", Kind: "text", Placeholder: "s3://bucket/athena-results/", Help: "Needed unless the workgroup sets a results location"},
		{Name: "region", Label: "Region", Kind: "text", Placeholder: "from the connection"},
		{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Default: aws_default (keys, IAM role, or assume role)"},
	}
}

func redshiftConnFields() []NodeField {
	return []NodeField{
		{Name: "database", Label: "Database", Kind: "text", Required: true, Placeholder: "dev"},
		{Name: "workgroup_name", Label: "Serverless workgroup", Kind: "text", Help: "Redshift Serverless; or set the cluster below"},
		{Name: "cluster_identifier", Label: "Cluster", Kind: "text", Help: "Provisioned cluster id (with db_user or secret_arn)"},
		{Name: "db_user", Label: "Database user", Kind: "text", Help: "Provisioned: temporary credentials for this user"},
		{Name: "secret_arn", Label: "Secret ARN", Kind: "text", Help: "Or: a Secrets Manager secret with the user and password"},
		{Name: "region", Label: "Region", Kind: "text", Placeholder: "from the connection"},
		{Name: "conn_id", Label: "Connection", Kind: "conn", Help: "Default: aws_default (keys, IAM role, or assume role)"},
	}
}

// loadFields: the options shared with the BigQuery load node.
func loadFields(engine string) []NodeField {
	cast := "safe (default): TRY_CAST, values that don't convert become NULL. strict: CAST, the load fails on them."
	types := []string{"varchar", "bigint", "integer", "double", "decimal(18,2)", "decimal(38,9)", "boolean", "date", "timestamp"}
	sqlHelp := "Optional, per column: a new name, and a type (pick one) or your own SQL: a function like my_db.to_date, or an expression with {col}."
	if engine == "redshift" {
		cast = "Redshift has no safe cast: values that don't convert fail the load."
		types = []string{"varchar(256)", "varchar(65535)", "bigint", "integer", "float8", "decimal(18,2)", "boolean", "date", "timestamp", "timestamptz", "super"}
		sqlHelp += " End an expression with ::type (e.g. f({col})::date) when the column may be new in the table."
	}
	f := []NodeField{
		{Name: "mode", Label: "Mode", Kind: "select", Options: []string{"", "append", "replace", "merge"}, Help: "append (default): historical, every run adds its rows. replace: only the latest load. merge: update the rows whose keys match, insert the others."},
		{Name: "column_map", Label: "Columns: rename / convert", Kind: "coldef", Columns: "src", Options: types, Help: sqlHelp},
	}
	if engine == "athena" {
		f = append(f, NodeField{Name: "cast_mode", Label: "Type conversion", Kind: "select", Options: []string{"", "safe", "strict"}, Help: cast})
	}
	return append(f,
		NodeField{Name: "merge_keys", Label: "Merge keys", Kind: "colpick", Columns: "out", Help: "merge: the columns that identify a row (names after renaming)"},
		NodeField{Name: "when_matched", Label: "When the key exists", Kind: "select", Options: []string{"", "update", "skip"}, Help: "merge: update the row (default) or keep the old one (insert new keys only)"},
		NodeField{Name: "conditions", Label: "Only rows where", Kind: "conds", Columns: "src", Help: "Optional filters on the source rows. Values can use templates, e.g. {{ .ds }}."},
		NodeField{Name: "condition_mode", Label: "Match", Kind: "select", Options: []string{"", "all", "any"}, Help: "all conditions (AND, default) or any of them (OR)"},
		NodeField{Name: "metadata", Label: "Metadata columns", Kind: "checks", Options: []string{"path", "inserted_at", "job_id", "job_date"}, Help: "path = the s3:// file of the row; inserted_at = load time; job_id = load time as yyyymmddHHMMSS; job_date = load date"},
		NodeField{Name: "metadata_prefix", Label: "Metadata prefix", Kind: "text", Placeholder: "metadata_"},
		NodeField{Name: "metadata_names", Label: "Metadata column names", Kind: "colmap", Columns: "fixed:path,inserted_at,job_id,job_date", Help: "Optional full names, e.g. inserted_at → ingest_at"},
		NodeField{Name: "inserted_at", Label: "inserted_at time", Kind: "select", Options: []string{"", "utc", "local"}, Help: "utc (default) or local time, as a TIMESTAMP"},
		NodeField{Name: "job_date", Label: "job_date", Kind: "select", Options: []string{"", "local", "utc"}},
		NodeField{Name: "timezone", Label: "Local time zone", Kind: "text", Placeholder: "Asia/Jakarta", Help: "Default: the server's time zone. Also used for job_id."},
	)
}

func awsWarehouseNodes() []NodeType {
	ath, rs := athenaConnFields(), redshiftConnFields()
	cat := func(a ...[]NodeField) []NodeField {
		var out []NodeField
		for _, x := range a {
			out = append(out, x...)
		}
		return out
	}
	tablesFields := func(ph string) []NodeField {
		return []NodeField{
			{Name: "tables", Label: "Tables", Kind: "list", Required: true, Help: "One per line: " + ph + ". Templates work, e.g. staging.orders_{{ .ds_nodash }}"},
			{Name: "fail_if_missing", Label: "Fail if a table doesn't exist", Kind: "bool", Help: "Default: skip it"},
		}
	}
	return []NodeType{
		{Type: "athena_query", Operator: "athena_query", Label: "Athena SQL", Category: "Transform", Icon: "bolt",
			Description: "Run SQL on Amazon Athena. Rows of a SELECT are the node's output.",
			Fields: cat([]NodeField{
				{Name: "sql", Label: "SQL", Kind: "code", Lang: "sql", Required: true, Help: "Templates work, e.g. WHERE d = '{{ .ds }}'"},
				{Name: "limit", Label: "Max rows", Kind: "number", Placeholder: "no limit"},
			}, ath)},
		{Type: "athena_external_table", Operator: "athena_external_table", Label: "Athena external table", Category: "Destinations", Icon: "bq", NeedsInput: true,
			Description: "Create an Athena (Glue) external table over the folder of the file the S3 node before it wrote. Column names follow the file (lowercase, as Glue needs).",
			Fields: cat([]NodeField{
				{Name: "table", Label: "Table", Kind: "text", Required: true, Placeholder: "database.table", Help: "Replaced on every run. Athena reads every file in the folder."},
				{Name: "default_type", Label: "Column types", Kind: "select", Options: []string{"", "STRING", "auto"}, Help: "STRING (default): every column string. auto: from the data."},
				{Name: "column_types", Label: "Type per column", Kind: "colmap", Columns: "gcs", Options: []string{"", "string", "bigint", "int", "double", "decimal(18,2)", "boolean", "date", "timestamp"}},
				{Name: "if_not_exists", Label: "Keep the table if it exists", Kind: "bool", Help: "Default: drop and create again"},
				{Name: "rows_from", Label: "S3 node", Kind: "upstream", Help: "Default: the only connected node before this one"},
			}, ath)},
		{Type: "athena_load_table", Operator: "athena_load_table", Label: "Athena table (load / merge)", Category: "Destinations", Icon: "dbin", NeedsInput: true,
			Description: "Load the table of the node before it (e.g. the Athena external table) into an Iceberg table: keep history, keep only the latest load, or merge. Adds metadata columns.",
			Fields: cat([]NodeField{
				{Name: "table", Label: "Table", Kind: "text", Required: true, Placeholder: "database.table", Help: "An Iceberg table, created on the first run"},
				{Name: "table_location", Label: "Table data location", Kind: "text", Required: true, Placeholder: "s3://bucket/warehouse/", Help: "The table's files go to <location>/<table>/"},
			}, loadFields("athena"), []NodeField{
				{Name: "partitioning", Label: "Partition by", Kind: "colpick", Columns: "out+meta", Help: "First load only. A timestamp column is partitioned by day."},
				{Name: "rows_from", Label: "Source node", Kind: "upstream", Help: "Default: the only connected node before this one"},
				{Name: "source_table", Label: "Source table", Kind: "text", Placeholder: "database.table", Help: "Instead of a node: load from this table"},
			}, ath)},
		{Type: "athena_delete_table", Operator: "athena_delete_table", Label: "Athena delete table", Category: "Destinations", Icon: "bq",
			Description: "Drop Athena tables. An Iceberg table's data is deleted too; an external table's files stay in S3.",
			Fields:      cat(tablesFields("database.table"), ath)},
		{Type: "redshift_query", Operator: "redshift_query", Label: "Redshift SQL", Category: "Transform", Icon: "bolt",
			Description: "Run SQL on Amazon Redshift (Data API). Rows of a SELECT are the node's output.",
			Fields: cat([]NodeField{
				{Name: "sql", Label: "SQL", Kind: "code", Lang: "sql", Required: true, Help: "Templates work, e.g. WHERE d = '{{ .ds }}'"},
				{Name: "limit", Label: "Max rows", Kind: "number", Placeholder: "no limit"},
			}, rs)},
		{Type: "redshift_load_table", Operator: "redshift_load_table", Label: "Redshift table (load / merge)", Category: "Destinations", Icon: "dbin", NeedsInput: true,
			Description: "COPY the file the S3 node before it wrote into a Redshift table: keep history, keep only the latest load, or merge. Adds metadata columns.",
			Fields: cat([]NodeField{
				{Name: "table", Label: "Table", Kind: "text", Required: true, Placeholder: "schema.table", Help: "Created on the first run"},
				{Name: "iam_role", Label: "IAM role for COPY", Kind: "text", Required: true, Placeholder: "arn:aws:iam::123456789012:role/redshift-s3", Help: "A role attached to the cluster that can read the bucket, or default"},
				{Name: "all_files", Label: "All files in the folder", Kind: "bool", Help: "COPY every file next to the one just written"},
			}, loadFields("redshift"), []NodeField{
				{Name: "dist_key", Label: "Distribution key", Kind: "colsel", Columns: "out+meta", Help: "First load only"},
				{Name: "sort_key", Label: "Sort key", Kind: "colpick", Columns: "out+meta", Help: "First load only, e.g. the job date"},
				{Name: "s3_region", Label: "Bucket region", Kind: "text", Help: "Only when the bucket is in another region than the cluster"},
				{Name: "rows_from", Label: "S3 node", Kind: "upstream", Help: "Default: the only connected node before this one"},
				{Name: "source_table", Label: "Source table", Kind: "text", Placeholder: "schema.table", Help: "Instead of a file: load from this table (a Spectrum table works too)"},
			}, rs)},
		{Type: "redshift_delete_table", Operator: "redshift_delete_table", Label: "Redshift delete table", Category: "Destinations", Icon: "bq",
			Description: "Drop Redshift tables.",
			Fields:      cat(tablesFields("schema.table"), rs)},
		{Type: "redshift_catalog_sync", Operator: "redshift_catalog_sync", Label: "Redshift catalog sync", Category: "Catalog", Icon: "bq",
			Description: "Read Redshift's metadata into the data catalog: tables and columns (SVV_TABLES / SVV_COLUMNS), schema changes, and the column lineage of the queries people ran since the last run (SYS_QUERY_HISTORY). Schedule it, e.g. hourly.",
			Fields: cat([]NodeField{
				{Name: "schemas", Label: "Schemas", Kind: "list", Help: "One per line. Default: every user schema of the database"},
				{Name: "lookback_hours", Label: "First run: hours of queries", Kind: "number", Placeholder: "168", Help: "Redshift keeps about 7 days of query history: run the sync at least daily"},
				{Name: "max_queries", Label: "Max queries per run", Kind: "number", Placeholder: "5000", Help: "The rest is read by the next run"},
				{Name: "no_queries", Label: "Tables and columns only", Kind: "bool", Help: "No query lineage (no access to SYS_QUERY_HISTORY needed)"},
			}, rs)},
	}
}
