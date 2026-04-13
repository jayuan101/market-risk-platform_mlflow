## Batch Pipeline Flow

Trigger: S3 PutObject event on raw/ prefix → fires Lambda

Step Functions States:
  1. ValidateFile      → Python Lambda, checks schema + row count + nulls
  2. Choice            → If pass → TransformData, If fail → WriteRejected
  3. TransformData     → Python Lambda or Glue job (controlled)
  4. UpdateCatalog     → Glue partition registration (MSCK or add-partition API)
  5. WriteAuditRecord  → Lambda writes pipeline_run record to audit/
  6. NotifySuccess     → SNS alert
  7. WriteRejected     → Bad records → rejected/ prefix + SNS failure alert

## Failure Handling
  - Any state failure triggers SNS alert
  - CloudWatch alarm on Step Functions failed executions
  - Rejected records stored with reason code and run_id