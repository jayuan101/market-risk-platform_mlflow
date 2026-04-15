param(
    [string]$task = "help"
)

$env:PYTHONPATH = "src"

switch ($task) {
    "setup"             { pip install -r requirements.txt }
    "test"              { pytest tests/ -v }
    "ingest-raw"        { python src/ingestion/upload_raw.py }
    "register-raw"      { python src/ingestion/register_raw_tables.py }
    "quality-local"     { python src/quality/run_quality_local.py }
    "curated"           { python src/transform/build_curated.py }
    "register-curated"  { python src/transform/register_curated_tables.py }
    "gold"              { python src/transform/build_gold.py }
    "register-gold"     { python src/transform/register_gold_tables.py }
    "help" {
        Write-Host "Available tasks:"
        Write-Host "  setup             - install dependencies"
        Write-Host "  ingest-raw        - upload raw CSVs to S3"
        Write-Host "  register-raw      - register raw Glue tables (mrisk_raw_db)"
        Write-Host "  quality-local     - run DQ checks, write passed/failed CSVs"
        Write-Host "  curated           - build curated Parquet, upload to S3 silver prefix"
        Write-Host "  register-curated  - register curated Glue tables + repair partitions (mrisk_curated_db)"
        Write-Host "  gold              - build gold Parquet from curated, upload to S3 gold prefix"
        Write-Host "  register-gold     - register gold Glue tables + repair partitions (mrisk_gold_db)"
        Write-Host "  test              - run pytest suite"
    }
    default { Write-Host "Unknown task: $task. Run with -task help" }
}
