# Databricks notebook source
# MAGIC %md
# MAGIC # Generic Reference Data Loader — Fast-Track Ingestion
# MAGIC
# MAGIC **One parameterized job for ALL reference datasets.** Driven entirely by a
# MAGIC `refdata.reference_data_registry` row — onboarding a new dataset is a registry
# MAGIC entry + JSON extract config + schema contract, **never a code change**.
# MAGIC
# MAGIC Design: `Reference_Data_Fast_Track_Ingestion_HLD.md` (§4.1, §5, §6, §7, §10)
# MAGIC
# MAGIC **Pipeline (gate-before-publish):**
# MAGIC ```
# MAGIC extract → schema-contract (HARD FAIL) → PII tripwire (HARD BLOCK) → DQ
# MAGIC   ── pass ─▶ CREATE OR REPLACE live table + audit cols + annual snapshot + register + notify
# MAGIC   ── fail ─▶ move file → _rejected/ + load_history(REJECTED) + notify owner+steward
# MAGIC             (LIVE TABLE + SNAPSHOTS UNTOUCHED)
# MAGIC ```
# MAGIC
# MAGIC **Conventions honoured:** no `mergeSchema`; SPN OAuth (no PAT); scoped UC grants;
# MAGIC reuses `Generic_Excel_Extractor`; reference data is non-PII (Presidio = tripwire, not mask).
# MAGIC
# MAGIC > Skeleton: functions with clear contracts + `TODO` for environment wiring.

# COMMAND ----------

# MAGIC %md ### 0. Parameters

# COMMAND ----------

dbutils.widgets.text("dataset_id", "", "Dataset ID (registry key)")
dbutils.widgets.text("source_file_path", "", "Quarantine file path (from File-Arrival Trigger)")
dbutils.widgets.text("triggered_by", "file_arrival", "Trigger source")

DATASET_ID       = dbutils.widgets.get("dataset_id").strip()
SOURCE_FILE_PATH = dbutils.widgets.get("source_file_path").strip()
TRIGGERED_BY     = dbutils.widgets.get("triggered_by").strip()

assert DATASET_ID, "dataset_id is required"

# COMMAND ----------

import hashlib, json, time
from dataclasses import dataclass, field
from datetime import datetime, timezone

RUN_ID    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
LOADER_UPN = spark.sql("SELECT current_user() AS u").collect()[0]["u"]   # processing SPN
NOW_UTC   = lambda: datetime.now(timezone.utc)
t0        = time.time()


class RejectError(Exception):
    """Raised by any gate. reason ∈ {SCHEMA_MISMATCH, PII_DETECTED, DQ_FAILED, EXTRACT_FAILED}."""
    def __init__(self, reason: str, detail: dict):
        self.reason, self.detail = reason, detail
        super().__init__(f"{reason}: {json.dumps(detail)[:500]}")

# COMMAND ----------

# MAGIC %md ### 1. Read registry config (drives everything)

# COMMAND ----------

@dataclass
class DatasetConfig:
    dataset_id: str
    domain: str
    entity: str
    source_format: str            # excel | csv | json
    quarantine_path: str
    rejected_path: str
    extract_config_ref: str
    schema_contract_ref: str
    dq_rules_ref: str
    pii_scan_enabled: bool
    target_catalog: str
    target_schema: str
    snapshot_policy: str          # history_partition | dated_table
    timetravel_retention_days: int
    owner_upn: str
    steward_upn: str

    @property
    def target_table(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.domain}_{self.entity}"

    @property
    def history_table(self) -> str:
        return f"{self.target_table}_history"


def read_registry(dataset_id: str) -> DatasetConfig:
    """Read the registry row from sql-dap-common-prd-uks-01 via JDBC (SPN OAuth, no PAT).

    TODO(env): wire JDBC URL + AAD-token auth from the SPN; secrets via Key Vault scope.
        url = "jdbc:sqlserver://sql-dap-common-prd-uks-01.database.windows.net:1433;database=...;"
        df  = (spark.read.format("sqlserver").option("url", url)
                    .option("query", f"SELECT * FROM refdata.reference_data_registry "
                                     f"WHERE dataset_id = '{dataset_id}' AND active = 1")
                    .option("accessToken", aad_token_for_spn()).load())
    """
    df = _registry_query(dataset_id)
    rows = df.collect()
    if not rows:
        raise ValueError(f"No active registry row for dataset_id='{dataset_id}'")
    r = rows[0].asDict()
    rejected = r.get("rejected_path") or f"{r['quarantine_path'].rstrip('/')}/_rejected/"
    return DatasetConfig(
        dataset_id=r["dataset_id"], domain=r["domain"], entity=r["entity"],
        source_format=r["source_format"], quarantine_path=r["quarantine_path"],
        rejected_path=rejected, extract_config_ref=r.get("extract_config_ref"),
        schema_contract_ref=r["schema_contract_ref"], dq_rules_ref=r.get("dq_rules_ref"),
        pii_scan_enabled=bool(r["pii_scan_enabled"]),
        target_catalog=r["target_catalog"], target_schema=r["target_schema"],
        snapshot_policy=r["snapshot_policy"],
        timetravel_retention_days=int(r["timetravel_retention_days"]),
        owner_upn=r["owner_upn"], steward_upn=r.get("steward_upn"))


cfg = read_registry(DATASET_ID)
print(f"Loaded config for {cfg.dataset_id} → target {cfg.target_table}")

# COMMAND ----------

# MAGIC %md ### 2. Extract — reuse Generic Excel Extractor / native readers (no bespoke code)

# COMMAND ----------

def extract(cfg: DatasetConfig, file_path: str):
    """Return a Spark DataFrame from the quarantine file.

    Excel → reuse Generic_Excel_Extractor with the dataset's JSON config (§22.6.10 / §2a-ii).
    CSV/JSON → native readers (inferSchema OFF — schema comes from the contract, §7).
    """
    try:
        if cfg.source_format == "excel":
            # TODO(env): call the shared extractor notebook; it writes CSV/Parquet to a temp UC path
            out = dbutils.notebook.run(
                "/Shared/reference_data/Generic_Excel_Extractor", 1800,
                {"excel_file_path": file_path,
                 "config_file_path": cfg.extract_config_ref,
                 "output_volume_path": f"{cfg.quarantine_path.rstrip('/')}/_extracted/",
                 "output_format": "parquet"})
            return spark.read.parquet(json.loads(out)["output_path"])
        elif cfg.source_format == "csv":
            return (spark.read.option("header", True).option("inferSchema", False)
                         .csv(file_path))
        elif cfg.source_format == "json":
            return spark.read.option("multiline", True).json(file_path)
        raise ValueError(f"Unsupported source_format {cfg.source_format}")
    except Exception as e:
        raise RejectError("EXTRACT_FAILED", {"error": str(e)[:500]})


df = extract(cfg, SOURCE_FILE_PATH)
row_count, col_count = df.count(), len(df.columns)
print(f"Extracted {row_count} rows × {col_count} cols")

# COMMAND ----------

# MAGIC %md ### 3. Gate A — Schema contract (HARD FAIL, never mergeSchema, §7)

# COMMAND ----------

def check_schema_contract(cfg: DatasetConfig, df):
    """Validate against the registered pandera/GE contract. Mismatch → HARD FAIL.

    Detects renamed / reordered / extra / missing columns and type drift. A genuine
    schema change requires the owner to update the contract first (reviewed) — drift
    is surfaced and approved, never auto-absorbed.

    Returns: schema_changed (bool) for load_history, computed vs the previous SUCCESS contract.
    TODO(env): load contract module from cfg.schema_contract_ref; run pandera schema.validate.
    """
    contract = _load_contract(cfg.schema_contract_ref)          # pandera DataFrameSchema
    errors = _validate_against_contract(df, contract)           # [] if clean
    if errors:
        raise RejectError("SCHEMA_MISMATCH",
                          {"expected": contract.summary(), "violations": errors})
    return _contract_differs_from_published(cfg, contract)      # bool


schema_changed = check_schema_contract(cfg, df)

# COMMAND ----------

# MAGIC %md ### 4. Gate B — PII tripwire (Presidio, HARD BLOCK, §6)

# COMMAND ----------

def pii_tripwire(cfg: DatasetConfig, df):
    """Reference data must contain NO PII. Any detection → HARD BLOCK (reject before publish).

    This is a tripwire, NOT a mask: a hit means the dataset is mis-classified and belongs
    on the Electralink masked path, not in reference_data. Scan runs in quarantine so PII
    never reaches the governed catalog or Delta history.

    TODO(env): Presidio AnalyzerEngine over a bounded sample of string columns;
        flag entities (EMAIL, PHONE, PERSON, UK_NINO, address, etc.) above threshold.
    """
    if not cfg.pii_scan_enabled:
        return
    findings = _presidio_scan(df, sample_rows=5000)             # [{column, entity, score}]
    high = [f for f in findings if f["score"] >= 0.7]
    if high:
        raise RejectError("PII_DETECTED",
                          {"entities": high[:50], "policy": "reference_data must be non-PII"})


pii_tripwire(cfg, df)

# COMMAND ----------

# MAGIC %md ### 5. Gate C — Data quality

# COMMAND ----------

def check_dq(cfg: DatasetConfig, df):
    """Row-count bounds, null thresholds, domain rules from cfg.dq_rules_ref. Fail → reject."""
    if not cfg.dq_rules_ref:
        return
    failures = _run_dq_rules(df, cfg.dq_rules_ref)              # [] if clean
    if failures:
        raise RejectError("DQ_FAILED", {"failed_rules": failures})


check_dq(cfg, df)

# COMMAND ----------

# MAGIC %md ### 6. Publish (clean only) — overwrite + audit cols + annual snapshot (§5.3, §5.4)

# COMMAND ----------

from pyspark.sql import functions as F

def add_audit_columns(cfg: DatasetConfig, df, file_path: str):
    file_bytes = dbutils.fs.head(file_path, 0)  # placeholder; TODO(env): stream sha256 of the file
    sha = _sha256_of_file(file_path)
    version_label = NOW_UTC().strftime("v%Y")    # yearly label; quarterly → vYYYYQn if needed
    return (df.withColumn("source_file_name",     F.lit(file_path.split("/")[-1]))
              .withColumn("source_file_sha256",   F.lit(sha))
              .withColumn("loaded_at_utc",        F.lit(NOW_UTC().isoformat()))
              .withColumn("loaded_by_upn",        F.lit(LOADER_UPN))
              .withColumn("version_effective_from", F.lit(version_label))), sha, version_label


def publish(cfg: DatasetConfig, df, file_path: str):
    """CREATE OR REPLACE live table, set retention, then write the immutable annual snapshot.

    No mergeSchema — the contract already locked the schema. Gold/views inherit automatically.
    """
    enriched, sha, version_label = add_audit_columns(cfg, df, file_path)

    # 6a. Live table — full overwrite (Delta time-travel = in-year rollback)
    (enriched.write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true")   # contract-validated swap, NOT mergeSchema
             .saveAsTable(cfg.target_table))
    spark.sql(f"ALTER TABLE {cfg.target_table} "
              f"SET TBLPROPERTIES (delta.deletedFileRetentionDuration = "
              f"'interval {cfg.timetravel_retention_days} days')")

    # 6b. Annual snapshot — durable cross-year rollback (VACUUM-independent)
    if cfg.snapshot_policy == "history_partition":
        (enriched.write.format("delta").mode("append")
                 .partitionBy("version_effective_from")
                 .option("mergeSchema", "false")
                 .saveAsTable(cfg.history_table))
        snapshot_ref = f"{cfg.history_table}::{version_label}"
    else:  # dated_table
        dated = f"{cfg.target_table}_{version_label}"
        enriched.write.format("delta").mode("overwrite").saveAsTable(dated)
        snapshot_ref = dated

    delta_version = (spark.sql(f"DESCRIBE HISTORY {cfg.target_table} LIMIT 1")
                          .collect()[0]["version"])
    return {"sha256": sha, "version_label": version_label,
            "snapshot_ref": snapshot_ref, "delta_version": delta_version}

# COMMAND ----------

# MAGIC %md ### 7. Register + notify + orchestrate (the gate-before-publish controller)

# COMMAND ----------

def write_load_history(cfg, status, **kw):
    """Append one row to refdata.reference_data_load_history (SUCCESS | REJECTED | ERROR).
    TODO(env): JDBC INSERT to sql-dap-common-prd-uks-01 (SPN OAuth)."""
    row = {"dataset_id": cfg.dataset_id, "run_id": RUN_ID, "triggered_by": TRIGGERED_BY,
           "status": status, "loaded_by_upn": LOADER_UPN,
           "latency_ms": int((time.time() - t0) * 1000),
           "started_at_utc": NOW_UTC().isoformat(), **kw}
    _history_insert(row)
    return row


def notify(recipients, subject, body):
    """Owner/steward notification on REJECT and on schema-contract change (§9).
    TODO(env): Logic App / ACS email via Function App webhook; no SMTP from cluster."""
    _send_notification([r for r in recipients if r], subject, body)


def update_registry_state(cfg, status, version_label=None):
    """Stamp last_attempt/last_success/last_status/current_version_label on the registry row."""
    _registry_update(cfg.dataset_id, status, version_label, NOW_UTC().isoformat())


# ---- Controller -------------------------------------------------------------------------
try:
    result = publish(cfg, df, SOURCE_FILE_PATH)               # gates A/B/C already passed above
    write_load_history(cfg, "SUCCESS",
                       source_file_name=SOURCE_FILE_PATH.split("/")[-1],
                       source_file_sha256=result["sha256"], row_count=row_count,
                       column_count=col_count, schema_changed=schema_changed,
                       published_table=cfg.target_table, snapshot_ref=result["snapshot_ref"],
                       delta_version=result["delta_version"])
    update_registry_state(cfg, "SUCCESS", result["version_label"])
    if schema_changed:
        notify([cfg.owner_upn, cfg.steward_upn],
               f"[DAP RefData] Schema contract changed: {cfg.target_table}",
               "The schema contract changed on the latest successful load. Review the contract version.")
    print(f"✅ Published {cfg.target_table} (Delta v{result['delta_version']}, snapshot {result['snapshot_ref']})")

except RejectError as rej:
    # Live table + snapshots UNTOUCHED. Move file to _rejected/, log, notify.
    dest = f"{cfg.rejected_path.rstrip('/')}/{NOW_UTC().strftime('%Y%m%dT%H%M%S')}_{SOURCE_FILE_PATH.split('/')[-1]}"
    dbutils.fs.mv(SOURCE_FILE_PATH, dest)
    write_load_history(cfg, "REJECTED", reject_reason=rej.reason,
                       reject_detail=json.dumps(rej.detail)[:4000],
                       source_file_name=SOURCE_FILE_PATH.split("/")[-1],
                       row_count=row_count, column_count=col_count)
    update_registry_state(cfg, "REJECTED")
    notify([cfg.owner_upn, cfg.steward_upn],
           f"[DAP RefData] REJECTED ({rej.reason}): {cfg.target_table}",
           f"Load rejected — reason {rej.reason}. File moved to {dest}. Live table unchanged.\n\n"
           f"Detail: {json.dumps(rej.detail)[:1500]}")
    raise   # surface failure to the job run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wiring checklist (the `TODO(env)` / `_helpers`)
# MAGIC | Helper | What to implement |
# MAGIC |---|---|
# MAGIC | `_registry_query` / `_registry_update` / `_history_insert` | JDBC to `sql-dap-common-prd-uks-01`, SPN OAuth token (no PAT) |
# MAGIC | `_load_contract` / `_validate_against_contract` / `_contract_differs_from_published` | pandera/GE contract load + validate + diff vs published |
# MAGIC | `_presidio_scan` | Presidio AnalyzerEngine over sampled string cols; entity threshold |
# MAGIC | `_run_dq_rules` | DQ rule engine from `dq_rules_ref` (YAML) |
# MAGIC | `_sha256_of_file` | stream the quarantine file, return hex digest |
# MAGIC | `_send_notification` | Function App webhook → Logic App / ACS email |
# MAGIC | `Generic_Excel_Extractor` path | confirm `/Shared/reference_data/Generic_Excel_Extractor` |
# MAGIC
# MAGIC **File-Arrival Trigger:** configure one Databricks Job per dataset (or a single job with a
# MAGIC dispatcher) with a *File arrival* trigger on each `…_quarantine_vol/<entity>/` path, passing
# MAGIC `dataset_id` + `source_file_path`. No Service Bus.
