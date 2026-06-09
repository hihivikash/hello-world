/* =====================================================================================
   Reference Data Fast-Track Ingestion — Governance tables (DDL)
   Target DB : sql-dap-common-prd-uks-01  (Azure SQL Database, T-SQL)
   Schema    : refdata   (keeps reference-data governance separate from Serve API tables)
   Related   : Reference_Data_Fast_Track_Ingestion_HLD.md §5.2, §5.4, §9
               Policy §22.6 (reference_data_registry + reference_data_load_history)
   Notes     : Registry DRIVES the single parameterized loader. One row per dataset.
               load_history is append-only audit of every load attempt (SUCCESS/REJECTED).
   ===================================================================================== */

------------------------------------------------------------------------------------------
-- 0. Schema
------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'refdata')
    EXEC('CREATE SCHEMA refdata');
GO

------------------------------------------------------------------------------------------
-- 1. reference_data_registry  — one row per reference dataset; the loader's config source
------------------------------------------------------------------------------------------
IF OBJECT_ID('refdata.reference_data_registry', 'U') IS NOT NULL
    DROP TABLE refdata.reference_data_registry;
GO

CREATE TABLE refdata.reference_data_registry
(
    -- Identity --------------------------------------------------------------------------
    dataset_id              NVARCHAR(100)   NOT NULL,   -- PK, e.g. 'eia_ecuk_energy_consumption'
    domain                  NVARCHAR(60)    NOT NULL,   -- → reference_data.<domain>_<entity>
    entity                  NVARCHAR(60)    NOT NULL,
    display_name            NVARCHAR(200)   NULL,
    description             NVARCHAR(1000)  NULL,

    -- Source ----------------------------------------------------------------------------
    source_type             NVARCHAR(20)    NOT NULL,   -- api | sharepoint | mailbox | manual
    source_config           NVARCHAR(MAX)   NULL,       -- JSON: endpoint / Graph drive+path / mailbox rule
    source_format           NVARCHAR(20)    NOT NULL,   -- excel | csv | json

    -- Landing / quarantine --------------------------------------------------------------
    quarantine_path         NVARCHAR(400)   NOT NULL,   -- …/<domain>_quarantine_vol/<entity>/
    rejected_path           NVARCHAR(400)   NULL,       -- defaults to <quarantine_path>/_rejected/

    -- Processing config (refs, version-controlled in repo) ------------------------------
    extract_config_ref      NVARCHAR(400)   NULL,       -- excel_extractor_configs/<entity>.json (Excel only)
    schema_contract_ref     NVARCHAR(400)   NOT NULL,   -- pandera/GE contract path  (HARD-FAIL gate)
    dq_rules_ref            NVARCHAR(400)   NULL,       -- DQ rule set path
    pii_scan_enabled        BIT             NOT NULL DEFAULT 1,   -- Presidio tripwire (default ON)

    -- Publish target & rollback ---------------------------------------------------------
    target_catalog          NVARCHAR(60)    NOT NULL DEFAULT 'prod_catalog',
    target_schema           NVARCHAR(60)    NOT NULL DEFAULT 'reference_data',
    snapshot_policy         NVARCHAR(20)    NOT NULL DEFAULT 'history_partition', -- history_partition | dated_table
    timetravel_retention_days INT           NOT NULL DEFAULT 90,   -- live-table deletedFileRetentionDuration

    -- Ownership & notification ----------------------------------------------------------
    owner_upn               NVARCHAR(200)   NOT NULL,
    steward_upn             NVARCHAR(200)   NULL,
    cadence                 NVARCHAR(20)    NOT NULL,   -- quarterly | yearly | adhoc
    sla_days                INT             NULL,       -- freshness SLA for dashboard alerting

    -- State -----------------------------------------------------------------------------
    active                  BIT             NOT NULL DEFAULT 1,
    last_success_at_utc     DATETIME2(3)    NULL,
    last_attempt_at_utc     DATETIME2(3)    NULL,
    last_status             NVARCHAR(20)    NULL,       -- SUCCESS | REJECTED | RUNNING
    current_version_label   NVARCHAR(40)    NULL,       -- e.g. 'v2026' / Delta version published

    -- Audit (row-level) -----------------------------------------------------------------
    created_at_utc          DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    created_by              NVARCHAR(200)   NULL,
    updated_at_utc          DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_by              NVARCHAR(200)   NULL,

    CONSTRAINT PK_reference_data_registry PRIMARY KEY CLUSTERED (dataset_id),
    CONSTRAINT UQ_refdata_domain_entity   UNIQUE (domain, entity),
    CONSTRAINT CK_refdata_source_type     CHECK (source_type     IN ('api','sharepoint','mailbox','manual')),
    CONSTRAINT CK_refdata_source_format   CHECK (source_format   IN ('excel','csv','json')),
    CONSTRAINT CK_refdata_snapshot_policy CHECK (snapshot_policy IN ('history_partition','dated_table')),
    CONSTRAINT CK_refdata_cadence         CHECK (cadence         IN ('quarterly','yearly','adhoc'))
);
GO

CREATE INDEX IX_refdata_registry_active  ON refdata.reference_data_registry (active) INCLUDE (cadence, last_success_at_utc);
GO

------------------------------------------------------------------------------------------
-- 2. reference_data_load_history  — append-only audit of EVERY load attempt
------------------------------------------------------------------------------------------
IF OBJECT_ID('refdata.reference_data_load_history', 'U') IS NOT NULL
    DROP TABLE refdata.reference_data_load_history;
GO

CREATE TABLE refdata.reference_data_load_history
(
    load_id                 BIGINT          IDENTITY(1,1) NOT NULL,
    dataset_id              NVARCHAR(100)   NOT NULL,
    run_id                  NVARCHAR(100)   NULL,        -- Databricks job run id
    triggered_by            NVARCHAR(40)    NULL,        -- file_arrival | manual | scheduled

    -- File identity ---------------------------------------------------------------------
    source_file_name        NVARCHAR(400)   NULL,
    source_file_sha256      CHAR(64)        NULL,
    file_size_bytes         BIGINT          NULL,

    -- Outcome ---------------------------------------------------------------------------
    status                  NVARCHAR(20)    NOT NULL,    -- SUCCESS | REJECTED | ERROR
    reject_reason           NVARCHAR(40)    NULL,        -- SCHEMA_MISMATCH | PII_DETECTED | DQ_FAILED | EXTRACT_FAILED
    reject_detail           NVARCHAR(MAX)   NULL,        -- JSON: failing columns / PII entities / DQ rule ids

    -- Metrics ---------------------------------------------------------------------------
    row_count               BIGINT          NULL,
    column_count            INT             NULL,
    schema_changed          BIT             NULL,        -- contract changed vs previous successful load
    published_table         NVARCHAR(300)   NULL,        -- fully-qualified table written (SUCCESS only)
    snapshot_ref            NVARCHAR(300)   NULL,        -- partition value / dated table written
    delta_version           BIGINT          NULL,        -- Delta version of the published commit
    latency_ms              BIGINT          NULL,

    -- Identity & timing -----------------------------------------------------------------
    loaded_by_upn           NVARCHAR(200)   NULL,        -- processing SPN UPN
    started_at_utc          DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    finished_at_utc         DATETIME2(3)    NULL,

    CONSTRAINT PK_reference_data_load_history PRIMARY KEY CLUSTERED (load_id),
    CONSTRAINT FK_refdata_history_registry
        FOREIGN KEY (dataset_id) REFERENCES refdata.reference_data_registry (dataset_id),
    CONSTRAINT CK_refdata_history_status CHECK (status IN ('SUCCESS','REJECTED','ERROR'))
);
GO

CREATE INDEX IX_refdata_history_dataset ON refdata.reference_data_load_history (dataset_id, started_at_utc DESC);
CREATE INDEX IX_refdata_history_status  ON refdata.reference_data_load_history (status, started_at_utc DESC);
GO

------------------------------------------------------------------------------------------
-- 3. Helper view — latest load per dataset (freshness + reject-rate dashboard, HLD §9)
------------------------------------------------------------------------------------------
IF OBJECT_ID('refdata.vw_reference_data_freshness', 'V') IS NOT NULL
    DROP VIEW refdata.vw_reference_data_freshness;
GO

CREATE VIEW refdata.vw_reference_data_freshness AS
WITH last_ok AS (
    SELECT dataset_id, MAX(finished_at_utc) AS last_success_at_utc
    FROM   refdata.reference_data_load_history
    WHERE  status = 'SUCCESS'
    GROUP  BY dataset_id
),
attempts AS (
    SELECT dataset_id,
           COUNT(*)                                              AS attempts_90d,
           SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END)  AS rejects_90d
    FROM   refdata.reference_data_load_history
    WHERE  started_at_utc >= DATEADD(DAY, -90, SYSUTCDATETIME())
    GROUP  BY dataset_id
)
SELECT  r.dataset_id,
        r.domain,
        r.entity,
        r.cadence,
        r.sla_days,
        r.owner_upn,
        o.last_success_at_utc,
        DATEDIFF(DAY, o.last_success_at_utc, SYSUTCDATETIME())   AS days_since_success,
        CASE WHEN r.sla_days IS NOT NULL
                  AND DATEDIFF(DAY, o.last_success_at_utc, SYSUTCDATETIME()) > r.sla_days
             THEN 'STALE — SLA breached' ELSE 'OK' END          AS freshness_status,
        a.attempts_90d,
        a.rejects_90d
FROM    refdata.reference_data_registry r
LEFT JOIN last_ok  o ON o.dataset_id = r.dataset_id
LEFT JOIN attempts a ON a.dataset_id = r.dataset_id
WHERE   r.active = 1;
GO

------------------------------------------------------------------------------------------
-- 4. Seed example — EIA ECUK energy consumption (yearly, Excel via SharePoint)
------------------------------------------------------------------------------------------
INSERT INTO refdata.reference_data_registry
    (dataset_id, domain, entity, display_name, description,
     source_type, source_config, source_format,
     quarantine_path, extract_config_ref, schema_contract_ref, dq_rules_ref,
     snapshot_policy, owner_upn, steward_upn, cadence, sla_days, created_by)
VALUES
    ('eia_ecuk_energy_consumption', 'energy', 'ecuk_consumption',
     'ECUK Energy Consumption (EIA)',
     'Annual UK energy consumption reference table, full overwrite each release.',
     'sharepoint',
     N'{"site":"DAP-RefData","drive":"Documents","path":"/ECUK/latest.xlsx"}',
     'excel',
     'prod_catalog.reference_data_landing.energy_quarantine_vol/ecuk_consumption/',
     'excel_extractor_configs/eia_ecuk_energy_consumption.json',
     'schema_contracts/ecuk_consumption.py',
     'dq_rules/ecuk_consumption.yml',
     'history_partition',
     'energy.owner@company.com', 'data.steward@company.com',
     'yearly', 400, 'architecture@company.com');
GO

PRINT 'refdata governance tables created and seeded.';
GO
