import pandas as pd
import hashlib
import json
import os
from datetime import datetime

BASE      = os.path.dirname(os.path.abspath(__file__))
CLEAN_DIR = os.path.join(BASE, "clean")
files     = [f for f in os.listdir(CLEAN_DIR) if f.endswith(".csv")]
manifests = []

for fname in files:
    fpath = os.path.join(CLEAN_DIR, fname)
    df    = pd.read_csv(fpath)

    with open(fpath, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    manifests.append({
        "file_name":        fname,
        "row_count":        len(df),
        "column_count":     len(df.columns),
        "checksum_md5":     checksum,
        "generated_at":     datetime.utcnow().isoformat() + "Z",
        "batch_id":         "BATCH-20240115-001",
        "source_system":    "MOCK-GENERATOR"
    })

manifest_path = os.path.join(BASE, "manifest.json")
with open(manifest_path, "w") as f:
    json.dump(manifests, f, indent=2)

print(f"Manifest written to {manifest_path}")
for m in manifests:
    print(f"  {m['file_name']}: {m['row_count']} rows | checksum: {m['checksum_md5']}")
