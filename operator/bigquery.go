package operator

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type BigQueryOperator struct {
	TaskID          string
	Query           string
	ProjectID       string
	Location        string
	CredentialsFile string
	Logf            func(format string, args ...any)
}

// Execute runs the query and waits for it. ctx cancellation (task timeout or
// scheduler shutdown) cancels the wait.
func (op *BigQueryOperator) Execute(ctx context.Context) error {
	if op.ProjectID == "" {
		return fmt.Errorf("project_id is empty (set params.project_id, dag params, or connection)")
	}
	if op.Query == "" {
		return fmt.Errorf("query is empty")
	}
	op.Logf("Executing query on project %s:\n%s", op.ProjectID, op.Query)

	var opts []option.ClientOption
	if op.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(op.CredentialsFile))
	}
	client, err := bigquery.NewClient(ctx, op.ProjectID, opts...)
	if err != nil {
		return fmt.Errorf("create bigquery client: %w", err)
	}
	defer client.Close()

	q := client.Query(op.Query)
	if op.Location != "" {
		q.Location = op.Location
	}
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("start query: %w", err)
	}
	op.Logf("BigQuery job id: %s", job.ID())
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait query: %w", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	op.Logf("Query finished successfully")
	return nil
}
