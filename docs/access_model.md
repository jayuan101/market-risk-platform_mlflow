## Personas

| Persona        | IAM Role                  | Access                                      |
|----------------|---------------------------|---------------------------------------------|
| Data Engineer  | mrisk-engineer-role       | Full S3 raw+curated, Glue full, Athena full |
| Analyst        | mrisk-analyst-role        | S3 curated read-only, Athena read-only      |
| Auditor        | mrisk-auditor-role        | S3 audit read-only, Athena audit DB only    |
| Pipeline/Exec  | mrisk-pipeline-role       | Used by Lambda + Step Functions only        |

## Lake Formation
  - Raw DB: engineer only
  - Curated DB: engineer + analyst
  - Audit DB: engineer + auditor