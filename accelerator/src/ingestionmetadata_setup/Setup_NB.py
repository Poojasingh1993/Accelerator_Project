# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,📖 One-Time Setup - Bootstrap Instructions
# MAGIC %md
# MAGIC # 🏗️ Setup & Bootstrap Notebook
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook contains **ALL ONE-TIME setup operations** that must be run once per environment (dev, staging, prod) to initialize the complete ingestion framework infrastructure.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ IMPORTANT: Run Once Per Environment
# MAGIC
# MAGIC **DO NOT include this notebook in scheduled jobs!**
# MAGIC
# MAGIC This notebook:
# MAGIC * **Creates schemas and volumes** - Raw, Bronze, Metadata schemas
# MAGIC * **Creates configuration tables** - Environment settings and parameters
# MAGIC * **Creates metadata tables** - Source definitions and ingestion rules
# MAGIC * **Creates audit infrastructure** - Audit log and file validation audit tables
# MAGIC * **Defines helper classes** - AuditLogger and FileValidator for ingestion tracking
# MAGIC * **Is NOT idempotent** - Some cells use DROP TABLE IF EXISTS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 When to Run This Notebook
# MAGIC
# MAGIC ### **Initial Setup (New Environment)**
# MAGIC 1. Set the environment widget to `dev`, `staging`, or `prod`
# MAGIC 2. Run all cells in order
# MAGIC 3. Verify schemas, volumes, tables, and audit infrastructure are created
# MAGIC
# MAGIC ### **Re-initialization (Clean Slate)**
# MAGIC 1. Run cleanup from [Maintenance_Utils](#notebook-556516444120050) first
# MAGIC 2. Run this notebook to recreate all infrastructure
# MAGIC 3. Upload source files to volumes
# MAGIC
# MAGIC ### **Configuration Updates**
# MAGIC * Re-run specific cells to update config or metadata
# MAGIC * Be careful with DROP TABLE statements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 What's Inside
# MAGIC
# MAGIC ### **Cell 1: Environment Bootstrap**
# MAGIC **Type:** Widget Setup  
# MAGIC * Creates environment selector dropdown
# MAGIC * Sets ENVIRONMENT and METADATA_CATALOG variables
# MAGIC * Must run first!
# MAGIC
# MAGIC ### **Cell 2: Setup Environment Structure**
# MAGIC **Type:** Schema & Volume Creation  
# MAGIC * Creates `{environment}.raw` schema
# MAGIC * Creates `{environment}.bronze` schema
# MAGIC * Creates `{environment}.metadata` schema
# MAGIC * Creates volumes: source_files, checkpoints, schemas
# MAGIC
# MAGIC ### **Cell 3: Environment Configuration Table**
# MAGIC **Type:** Configuration Table Creation  
# MAGIC * Creates `{environment}.metadata.ingestion_config`
# MAGIC * Inserts environment-specific parameters
# MAGIC * Defines volume paths, catalog/schema settings
# MAGIC
# MAGIC ### **Cell 4: Metadata Table & Source Definitions**
# MAGIC **Type:** Metadata Table Creation  
# MAGIC * Creates `{environment}.metadata.ingestion_metadata`
# MAGIC * Inserts 19 source configurations
# MAGIC * Defines file formats, paths, transformation rules
# MAGIC
# MAGIC ### **Cell 5: Create Audit Log Table**
# MAGIC **Type:** Audit Infrastructure  
# MAGIC * Creates `{environment}.metadata.audit_log`
# MAGIC * Tracks all ingestion runs with 34+ fields
# MAGIC * Captures timing, record counts, status, errors
# MAGIC
# MAGIC ### **Cell 6: AuditLogger Class Definition**
# MAGIC **Type:** Helper Class  
# MAGIC * Python class for easy audit logging
# MAGIC * Provides `log_ingestion()` method
# MAGIC * Used by Ingestion_NB
# MAGIC
# MAGIC ### **Cell 7: Create File Validation Audit Table**
# MAGIC **Type:** Validation Infrastructure  
# MAGIC * Creates `{environment}.metadata.file_validation_audit`
# MAGIC * Tracks pre-ingestion file discovery and validation
# MAGIC * Logs validation checks, file counts, sizes
# MAGIC
# MAGIC ### **Cell 8: FileValidator Class Definition**
# MAGIC **Type:** Helper Class  
# MAGIC * Python class for file validation
# MAGIC * Provides `validate_source_files()` method
# MAGIC * Used by Ingestion_NB before processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ After Running This Notebook
# MAGIC
# MAGIC 1. **Upload source files** to `/Volumes/{environment}/raw/source_files/`
# MAGIC 2. **Organize by subfolder** (category/, product/, etc.)
# MAGIC 3. **Run [Ingestion_NB](#notebook-1378092773026306)** for scheduled data ingestion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Next Steps
# MAGIC
# MAGIC * **For production runs**: Use [Ingestion_NB](#notebook-1378092773026306)
# MAGIC * **For analytics/maintenance**: Use [Maintenance_Utils](#notebook-556516444120050)
# MAGIC * **For audit analytics**: Query `{environment}.metadata.audit_log` or use [Maintenance_Utils](#notebook-556516444120050)

# COMMAND ----------

# DBTITLE 1,🌍 Environment Bootstrap - Set Once, Deploy Anywhere
# ===============================================
# ENVIRONMENT BOOTSTRAP - ZERO CODE CHANGES FOR DEPLOYMENT
# This is the ONLY place that determines the environment
# All other code reads from this variable
# ===============================================

# Create environment selector widget
dbutils.widgets.dropdown(
    "environment", 
    "dev",                           # Default environment
    ["dev", "staging", "prod"],     # Available environments
    "Environment"
)

# Get selected environment
ENVIRONMENT = dbutils.widgets.get("environment")

# Set catalog and schema based on environment
METADATA_CATALOG = ENVIRONMENT  # Each environment gets its own catalog
METADATA_SCHEMA = "metadata"    # Metadata schema is consistent across environments

print("="*80)
print("🌍 ENVIRONMENT BOOTSTRAP")
print("="*80)
print(f"Selected Environment: {ENVIRONMENT}")
print(f"Metadata Location: {METADATA_CATALOG}.{METADATA_SCHEMA}")
print()
print("📋 This configuration will be used for:")
print(f"  • Reading ingestion_config from: {METADATA_CATALOG}.{METADATA_SCHEMA}.ingestion_config")
print(f"  • Reading ingestion_metadata from: {METADATA_CATALOG}.{METADATA_SCHEMA}.ingestion_metadata")
print(f"  • All paths and targets are dynamically built from config table")
print("="*80)
print()
print("💡 To switch environments:")
print("   1. Change the widget dropdown above")
print("   2. Re-run all cells")
print("   3. No code changes needed!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,00a - 🏗️ Setup Environment Structure (Raw Schema & Volume)
# ===============================================
# Setup Environment Structure - Raw Schema & Volume
# Creates the raw schema and volume for source files
# 100% dynamic based on bootstrap environment
# ===============================================

print("="*80)
print("🏗️ ENVIRONMENT STRUCTURE SETUP")
print("="*80)
print(f"Environment: {ENVIRONMENT}")
print(f"Target Catalog: {METADATA_CATALOG}")
print()

# 0. Create Catalog if not exists
print(f"📁 Creating catalog: {METADATA_CATALOG}")

try:
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {METADATA_CATALOG}
        COMMENT 'Environment catalog for {ENVIRONMENT} - contains raw, bronze, and metadata schemas'
    """)
    print(f"  ✓ Catalog created: {METADATA_CATALOG}")
except Exception as e:
    print(f"  ⚠ Catalog may already exist: {str(e)}")

print()

# 1. Create Raw Schema for source files
raw_schema = f"{METADATA_CATALOG}.raw"
print(f"📁 Creating raw schema: {raw_schema}")

try:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {raw_schema}
        COMMENT 'Raw source files - landing zone for all incoming data'
    """)
    print(f"  ✓ Schema created: {raw_schema}")
except Exception as e:
    print(f"  ⚠ Schema may already exist: {str(e)}")

# 2. Create Bronze Schema for processed data
bronze_schema = f"{METADATA_CATALOG}.bronze"
print(f"\n📁 Creating bronze schema: {bronze_schema}")

try:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {bronze_schema}
        COMMENT 'Bronze layer - raw ingested data with minimal transformation'
    """)
    print(f"  ✓ Schema created: {bronze_schema}")
except Exception as e:
    print(f"  ⚠ Schema may already exist: {str(e)}")

# 3. Create Metadata Schema
metadata_schema_full = f"{METADATA_CATALOG}.{METADATA_SCHEMA}"
print(f"\n📁 Creating metadata schema: {metadata_schema_full}")

try:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {metadata_schema_full}
        COMMENT 'Metadata and configuration tables for ingestion framework'
    """)
    print(f"  ✓ Schema created: {metadata_schema_full}")
except Exception as e:
    print(f"  ⚠ Schema may already exist: {str(e)}")

# 4. Create Volumes under raw schema
print(f"\n📦 Creating volumes...")

# Source files volume
source_volume = f"{METADATA_CATALOG}.raw.source_files"
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {source_volume}
        COMMENT 'Landing zone for all source data files'
    """)
    print(f"  ✓ Volume created: {source_volume}")
    print(f"    Path: /Volumes/{ENVIRONMENT}/raw/source_files")
except Exception as e:
    print(f"  ⚠ Volume creation: {str(e)}")

# 5. Create Volumes under bronze schema
# Checkpoints volume
checkpoint_volume = f"{METADATA_CATALOG}.bronze.checkpoints"
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {checkpoint_volume}
        COMMENT 'Auto Loader streaming checkpoints'
    """)
    print(f"  ✓ Volume created: {checkpoint_volume}")
    print(f"    Path: /Volumes/{ENVIRONMENT}/bronze/checkpoints")
except Exception as e:
    print(f"  ⚠ Volume creation: {str(e)}")

# Schemas volume
schemas_volume = f"{METADATA_CATALOG}.bronze.schemas"
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {schemas_volume}
        COMMENT 'Auto Loader schema tracking and evolution'
    """)
    print(f"  ✓ Volume created: {schemas_volume}")
    print(f"    Path: /Volumes/{ENVIRONMENT}/bronze/schemas")
except Exception as e:
    print(f"  ⚠ Volume creation: {str(e)}")

print("\n" + "="*80)
print("✅ ENVIRONMENT STRUCTURE COMPLETE")
print("="*80)
print()
print("📋 Summary:")
print(f"  • Raw Schema: {raw_schema}")
print(f"  • Bronze Schema: {bronze_schema}")
print(f"  • Metadata Schema: {metadata_schema_full}")
print(f"  • Source Volume: /Volumes/{ENVIRONMENT}/raw/source_files")
print(f"  • Checkpoint Volume: /Volumes/{ENVIRONMENT}/bronze/checkpoints")
print(f"  • Schema Volume: /Volumes/{ENVIRONMENT}/bronze/schemas")
print()
print("💡 Next Steps:")
print("  1. Copy/upload source files to /Volumes/{}/raw/source_files".format(ENVIRONMENT))
print("  2. Organize files by entity folder (category/, product/, etc.)")
print("  3. Run configuration and metadata setup cells")
print("="*80)

# COMMAND ----------

# DBTITLE 1,00b - 🔧 Environment Configuration (Parametrized)
# ===============================================
# Environment Configuration Table
# Parametrize deployment settings for easy promotion across environments
# Uses environment from bootstrap cell - NO HARDCODED CATALOG!
# ===============================================

# Build table name dynamically from bootstrap environment
config_table = f"{METADATA_CATALOG}.{METADATA_SCHEMA}.ingestion_config"

print(f"Creating configuration table: {config_table}")

spark.sql(f"""
DROP TABLE IF EXISTS {config_table}
""")

spark.sql(f"""
CREATE TABLE {config_table} (
  config_key STRING,
  config_value STRING,
  description STRING,
  environment STRING,
  is_active BOOLEAN,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
)
COMMENT 'Environment configuration for ingestion framework'
""")

print(f"✓ Table created: {config_table}")

# Insert environment-specific parameters
# NOTE: These values use ENVIRONMENT variable from bootstrap
spark.sql(f"""
INSERT INTO {config_table} VALUES
-- Catalog & Schema Settings (using ENVIRONMENT variable from bootstrap)
('target_catalog', '{ENVIRONMENT}', 'Default target catalog for ingestion', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),
('target_schema', 'bronze', 'Default target schema for bronze layer', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),

-- Volume Base Paths (using ENVIRONMENT variable for catalog reference)
('source_volume_base', '/Volumes/{ENVIRONMENT}/raw/source_files', 'Base path for source data files', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),
('schema_volume_base', '/Volumes/{ENVIRONMENT}/bronze/schemas', 'Base path for Auto Loader schema tracking', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),
('checkpoint_volume_base', '/Volumes/{ENVIRONMENT}/bronze/checkpoints', 'Base path for streaming checkpoints', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),

-- Environment Identifier
('environment', '{ENVIRONMENT}', 'Current environment (dev/staging/prod)', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),

-- Optional: Environment-specific settings
('max_files_per_trigger', '1000', 'Max files to process per trigger', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp()),
('shuffle_partitions', '200', 'Spark shuffle partitions', '{ENVIRONMENT}', true, current_timestamp(), current_timestamp())
""")

print(f"✓ Inserted configuration for environment: {ENVIRONMENT}")
print(f"\n📋 Viewing configuration from: {config_table}\n")

# View configuration
display(spark.sql(f"""
SELECT config_key, config_value, description, environment 
FROM {config_table} 
WHERE is_active = true
ORDER BY config_key
"""))

# COMMAND ----------

# DBTITLE 1,00c - Create Metadata Table & Insert Sources
# ===============================================
# Parametrized Metadata Table
# Stores only relative paths - full paths built dynamically from config
# Uses environment from bootstrap cell - NO HARDCODED CATALOG!
# ===============================================

# Build table name dynamically from bootstrap environment
metadata_table = f"{METADATA_CATALOG}.{METADATA_SCHEMA}.ingestion_metadata"

print(f"Creating metadata table: {metadata_table}")

spark.sql(f"""
DROP TABLE IF EXISTS {metadata_table}
""")

spark.sql(f"""
CREATE TABLE {metadata_table} (
  source_id INT,
  source_name STRING,
  source_type STRING,
  source_subfolder STRING,              -- Relative path (e.g., 'category', 'order_header_delta')
  target_table STRING,                  -- Table name only (e.g., 'raw_category')
  file_format STRING,
  delimiter STRING,
  recursive_lookup BOOLEAN,
  merge_schema BOOLEAN,
  flatten_schema BOOLEAN,
  explode_arrays BOOLEAN,
  additional_options STRING,
  is_active BOOLEAN,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
)
COMMENT 'Ingestion source metadata - configuration for all data sources'
""")

print(f"✓ Table created: {metadata_table}")

# Insert sources with all source_id's active
spark.sql(f"""
INSERT INTO {metadata_table} VALUES
(1, 'ecommerce_category', 'FILE', 'category', 'raw_category', 'csv', ',', true, true, false, false, '{{"header": "true", "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}}', true, current_timestamp(), current_timestamp()),
(2, 'ecommerce_subcategory', 'FILE', 'subcategory', 'raw_subcategory', 'csv', '\t', true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(3, 'ecommerce_brand', 'FILE', 'brand', 'raw_brand', 'json', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(4, 'ecommerce_supplier', 'FILE', 'supplier', 'raw_supplier', 'xml', NULL, true, true, true, false, '{{"rowTag": "item"}}', true, current_timestamp(), current_timestamp()),
(5, 'ecommerce_product', 'FILE', 'product', 'raw_product', 'parquet', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(6, 'ecommerce_store', 'FILE', 'store', 'raw_store', 'parquet', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(7, 'ecommerce_employee', 'FILE', 'employee', 'raw_employee', 'csv', "|", true, true, false, false, '{{"header":"true"}}', true, current_timestamp(), current_timestamp()),
(8, 'ecommerce_customer', 'FILE', 'customer_nested', 'raw_customer', 'json', NULL, true, true, true, true, '{{}}', true, current_timestamp(), current_timestamp()),
(9, 'ecommerce_promotion', 'FILE', 'promotion', 'raw_promotion', 'orc', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(10, 'ecommerce_payment_method', 'FILE', 'payment_method', 'raw_payment_method', 'avro', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(11, 'ecommerce_courier', 'FILE', 'courier', 'raw_courier', 'xlsx', ',', true, true, false, false, '{{"header": "true", "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}}', true, current_timestamp(), current_timestamp()),
(12, 'ecommerce_inventory', 'FILE', 'inventory', 'raw_inventory', 'parquet', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(13, 'ecommerce_inventory_transaction', 'FILE', 'inventory_transaction', 'raw_inventory_transaction', 'json', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(14, 'ecommerce_order_header', 'FILE', 'order_header_delta', 'raw_order_header', 'delta', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(15, 'ecommerce_order_item', 'FILE', 'order_item', 'raw_order_item', 'parquet', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(16, 'ecommerce_payment', 'FILE', 'payment', 'raw_payment', 'json', NULL, true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp()),
(17, 'ecommerce_shipment', 'FILE', 'shipment', 'raw_shipment', 'xml', NULL, true, true, true, false, '{{"rowTag": "item"}}', true, current_timestamp(), current_timestamp()),
(18, 'ecommerce_return', 'FILE', 'return', 'raw_return', 'fw', NULL, true, true, false, false, '{{"column_positions": [{{"name": "return_id", "start": 0, "length": 36}}, {{"name": "order_item_id", "start": 40, "length": 36}}, {{"name": "return_reason", "start": 80, "length": 20}}, {{"name": "return_status", "start": 100, "length": 15}}]}}', true, current_timestamp(), current_timestamp()),
(19, 'ecommerce_refund', 'FILE', 'refund', 'raw_refund', 'csv', '\t', true, true, false, false, '{{}}', true, current_timestamp(), current_timestamp())
""")

print(f"✓ Inserted 19 source configurations")
print(f"\n📋 Viewing metadata from: {metadata_table}\n")

# Display the metadata
display(spark.sql(f"SELECT * FROM {metadata_table} ORDER BY source_id"))

# COMMAND ----------

# DBTITLE 1,00d - 📊 Create Audit Log Table
# ===============================================
# Create Audit Log Table for Ingestion Tracking
# Run this ONCE to set up the audit infrastructure
# ===============================================

from datetime import datetime

# Environment variables already set in Cell 2
print("="*100)
print("📊 CREATING AUDIT LOG TABLE")
print("="*100)
print(f"Environment: {ENVIRONMENT}")
print(f"Timestamp: {datetime.now()}")
print("="*100)

# Build audit table name
audit_table = f"{METADATA_CATALOG}.{METADATA_SCHEMA}.audit_log"

print(f"\n📋 Audit Table: {audit_table}\n")

# Create comprehensive audit log table
create_audit_table_sql = f"""
CREATE TABLE IF NOT EXISTS {audit_table}
(
    -- Run Identification
    run_id STRING COMMENT 'Unique identifier for each ingestion run',
    run_timestamp TIMESTAMP COMMENT 'When the ingestion run started',
    
    -- Source Information
    source_id INT COMMENT 'Source ID from metadata table',
    source_name STRING COMMENT 'Name of the source system/file',
    source_type STRING COMMENT 'Type of source (file, API, database, etc.)',
    source_path STRING COMMENT 'Full path to source data',
    file_format STRING COMMENT 'Format of source files (CSV, JSON, Parquet, etc.)',
    
    -- Target Information
    target_table STRING COMMENT 'Full name of target bronze table',
    target_layer STRING COMMENT 'Data layer (bronze, silver, gold)',
    
    -- File Processing Details
    file_count INT COMMENT 'Number of files processed',
    file_names STRING COMMENT 'Comma-separated list of file names (max 500 chars)',
    file_size_mb DECIMAL(18,2) COMMENT 'Total size of files processed in MB',
    
    -- Record Counts
    records_read BIGINT COMMENT 'Number of records read from source',
    records_written BIGINT COMMENT 'Number of records written to target',
    records_failed BIGINT COMMENT 'Number of records that failed to load',
    records_duplicate BIGINT COMMENT 'Number of duplicate records identified',
    
    -- Execution Timing
    start_time TIMESTAMP COMMENT 'Ingestion start time',
    end_time TIMESTAMP COMMENT 'Ingestion end time',
    duration_seconds DECIMAL(18,2) COMMENT 'Total execution time in seconds',
    
    -- Status and Quality
    status STRING COMMENT 'Overall status: SUCCESS, FAILED, PARTIAL, WARNING',
    validation_status STRING COMMENT 'Validation result: MATCH, MISMATCH, SKIPPED, ERROR',
    data_quality_score DECIMAL(5,2) COMMENT 'Data quality score (0-100)',
    
    -- Error Handling
    error_message STRING COMMENT 'Error message if status is FAILED',
    error_code STRING COMMENT 'Error code for categorization',
    error_count INT COMMENT 'Number of errors encountered',
    
    -- Configuration
    ingestion_mode STRING COMMENT 'Mode: full, incremental, merge',
    environment STRING COMMENT 'Environment: dev, staging, prod',
    
    -- Metadata
    user_name STRING COMMENT 'User who triggered the ingestion',
    cluster_id STRING COMMENT 'Cluster ID where ingestion ran',
    notebook_path STRING COMMENT 'Path to notebook that performed ingestion',
    
    -- Audit Metadata
    created_at TIMESTAMP COMMENT 'When this audit record was created',
    updated_at TIMESTAMP COMMENT 'When this audit record was last updated'
)
USING DELTA
COMMENT 'Audit log for data ingestion pipeline - tracks all ingestion runs, file processing, and validation results'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
"""

try:
    # Create the audit table
    spark.sql(create_audit_table_sql)
    print("✅ Audit log table created successfully!\n")
    
    # Display table details
    print("📊 Table Schema:\n")
    table_desc = spark.sql(f"DESCRIBE TABLE {audit_table}").toPandas()
    display(table_desc)
    
    # Check if table has existing records
    record_count = spark.table(audit_table).count()
    print(f"\n📈 Current record count: {record_count:,}\n")
    
    print("\n" + "="*100)
    print("✨ Audit log table setup complete!")
    print("="*100)
    
except Exception as e:
    print(f"❌ Error creating audit log table: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,00e - 📝 AuditLogger Class Definition
# ===============================================
# Audit Logger Helper Class
# Provides easy methods to log ingestion runs
# ===============================================

import uuid
from datetime import datetime
from typing import Optional

class AuditLogger:
    """
    Helper class to log ingestion audit records
    """
    
    def __init__(self, spark, audit_table: str, environment: str):
        self.spark = spark
        self.audit_table = audit_table
        self.environment = environment
        self.run_id = str(uuid.uuid4())
        self.run_timestamp = datetime.now()
    
    def log_ingestion(
        self,
        source_id: int,
        source_name: str,
        source_type: str,
        source_path: str,
        file_format: str,
        target_table: str,
        target_layer: str,
        file_count: int,
        file_names: str,
        file_size_mb: float,
        records_read: int,
        records_written: int,
        records_failed: int,
        records_duplicate: int,
        start_time: datetime,
        end_time: datetime,
        status: str,
        validation_status: str,
        data_quality_score: float = None,
        error_message: str = None,
        error_code: str = None,
        error_count: int = 0,
        ingestion_mode: str = 'full',
        user_name: str = None,
        cluster_id: str = None,
        notebook_path: str = None
    ):
        """
        Insert an audit record for an ingestion run
        
        Args:
            source_id: Source ID from metadata table
            source_name: Name of the source
            source_type: Type of source (file, API, database)
            source_path: Full path to source
            file_format: Format (CSV, JSON, etc.)
            target_table: Full target table name
            target_layer: Data layer (bronze, silver, gold)
            file_count: Number of files processed
            file_names: Comma-separated file names (truncate if > 500 chars)
            file_size_mb: Total file size in MB
            records_read: Records read from source
            records_written: Records written to target
            records_failed: Failed records
            records_duplicate: Duplicate records
            start_time: Ingestion start time
            end_time: Ingestion end time
            status: SUCCESS, FAILED, PARTIAL, WARNING
            validation_status: MATCH, MISMATCH, SKIPPED, ERROR
            data_quality_score: Quality score 0-100 (optional)
            error_message: Error details if failed (optional)
            error_code: Error code for categorization (optional)
            error_count: Number of errors (optional)
            ingestion_mode: full, incremental, merge (default: full)
            user_name: User who triggered (optional, will use current_user())
            cluster_id: Cluster ID (optional)
            notebook_path: Notebook path (optional)
        """
        
        # Calculate duration
        duration_seconds = (end_time - start_time).total_seconds()
        
        # Truncate file_names if too long
        if len(file_names) > 500:
            file_names = file_names[:497] + '...'
        
        # Get user name if not provided
        if user_name is None:
            user_name = self.spark.sql("SELECT current_user() as user").collect()[0]['user']
        
        # Escape error_message for SQL
        if error_message is None:
            error_message_sql = 'NULL'
        else:
            escaped_error_msg = error_message.replace("'", "''")
            error_message_sql = f"'{escaped_error_msg}'"
        
        error_code_sql = 'NULL' if error_code is None else f"'{error_code}'"
        cluster_id_sql = 'NULL' if cluster_id is None else f"'{cluster_id}'"
        notebook_path_sql = 'NULL' if notebook_path is None else f"'{notebook_path}'"
        
        # Build INSERT statement
        insert_sql = f'''
        INSERT INTO {self.audit_table} 
        VALUES (
            '{self.run_id}',
            timestamp'{self.run_timestamp}',
            {source_id},
            '{source_name}',
            '{source_type}',
            '{source_path}',
            '{file_format}',
            '{target_table}',
            '{target_layer}',
            {file_count},
            '{file_names}',
            {file_size_mb},
            {records_read},
            {records_written},
            {records_failed},
            {records_duplicate},
            timestamp'{start_time}',
            timestamp'{end_time}',
            {duration_seconds},
            '{status}',
            '{validation_status}',
            {data_quality_score if data_quality_score is not None else 'NULL'},
            {error_message_sql},
            {error_code_sql},
            {error_count},
            '{ingestion_mode}',
            '{self.environment}',
            '{user_name}',
            {cluster_id_sql},
            {notebook_path_sql},
            current_timestamp(),
            current_timestamp()
        ) '''
        
        try:
            self.spark.sql(insert_sql)
            return True
        except Exception as e:
            print(f"⚠️ Warning: Failed to log audit record: {str(e)}")
            return False
    
    def log_batch(
        self,
        audit_records: list
    ):
        """
        Insert multiple audit records at once
        
        Args:
            audit_records: List of dictionaries with audit record fields
        """
        from pyspark.sql import Row
        
        try:
            # Convert to Spark DataFrame
            rows = [Row(**record) for record in audit_records]
            df = self.spark.createDataFrame(rows)
            
            # Append to audit table
            df.write.mode("append").saveAsTable(self.audit_table)
            return True
        except Exception as e:
            print(f"⚠️ Warning: Failed to log batch audit records: {str(e)}")
            return False

print("="*100)
print("✅ AUDITLOGGER CLASS DEFINED")
print("="*100)
print("\n💡 This class is now available for use in Ingestion_NB.")
print("="*100)

# COMMAND ----------

# DBTITLE 1,00f - 🔍 Create File Validation Audit Table
# ===============================================
# Create File Validation Audit Table
# Tracks pre-ingestion file discovery and validation
# ===============================================

from datetime import datetime

# Environment variables already set in Cell 2
print("="*100)
print("🔍 CREATING FILE VALIDATION AUDIT TABLE")
print("="*100)
print(f"Environment: {ENVIRONMENT}")
print(f"Timestamp: {datetime.now()}")
print("="*100)

# Build validation audit table name
validation_table = f"{METADATA_CATALOG}.{METADATA_SCHEMA}.file_validation_audit"

print(f"\n📋 Validation Table: {validation_table}\n")

# Create file validation audit table
create_validation_table_sql = f"""
CREATE TABLE IF NOT EXISTS {validation_table}
(
    -- Run Identification
    validation_id STRING COMMENT 'Unique identifier for each validation run',
    validation_timestamp TIMESTAMP COMMENT 'When the validation was performed',
    run_id STRING COMMENT 'Associated ingestion run ID (if linked)',
    
    -- Source Information
    source_id INT COMMENT 'Source ID from metadata table',
    source_name STRING COMMENT 'Name of the source',
    source_type STRING COMMENT 'Type of source (FILE, RDBMS, API)',
    source_path STRING COMMENT 'Full path to source location',
    file_format STRING COMMENT 'Expected file format',
    
    -- File Discovery Details
    files_found BOOLEAN COMMENT 'Whether any files were found at source path',
    file_count INT COMMENT 'Number of files discovered',
    file_list STRING COMMENT 'Comma-separated list of discovered files (truncated at 1000 chars)',
    total_size_mb DECIMAL(18,2) COMMENT 'Total size of discovered files in MB',
    
    -- Validation Checks
    check_path_exists BOOLEAN COMMENT 'Source path exists',
    check_files_exist BOOLEAN COMMENT 'Files found in path',
    check_format_valid BOOLEAN COMMENT 'Files match expected format',
    check_not_empty BOOLEAN COMMENT 'Files are not empty (size > 0)',
    
    -- Error Details
    error_message STRING COMMENT 'Error message if validation failed',
    error_code STRING COMMENT 'Error code if applicable',
    
    -- Metadata
    environment STRING COMMENT 'Environment where validation ran',
    user_name STRING COMMENT 'User who triggered validation',
    notebook_path STRING COMMENT 'Path to notebook that ran validation',
    
    -- Timestamps
    created_date TIMESTAMP COMMENT 'Record creation timestamp',
    updated_date TIMESTAMP COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Pre-ingestion file validation audit log - tracks file discovery and validation before ingestion'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2'
)
"""

try:
    spark.sql(create_validation_table_sql)
    print("✅ File validation audit table created successfully!\n")
    print("📊 Table Schema:\n")
    display(spark.sql(f"DESCRIBE {validation_table}"))
    
except Exception as e:
    print(f"❌ Error creating validation audit table: {str(e)}")
    raise

print("\n" + "="*100)
print("✨ File validation infrastructure ready!")
print("="*100)

# COMMAND ----------

# DBTITLE 1,00g - 🛡️ FileValidator Class Definition
# ===============================================
# FileValidator Helper Class
# Provides methods to validate files before ingestion
# ===============================================

import uuid
from datetime import datetime
from typing import Dict, List, Optional
import os

class FileValidator:
    """
    Helper class to validate source files before ingestion
    """
    
    def __init__(self, spark, validation_table: str, environment: str, run_id: str = None):
        self.spark = spark
        self.validation_table = validation_table
        self.environment = environment
        self.run_id = run_id  # Optional link to ingestion run
        self.validation_timestamp = datetime.now()
    
    def validate_source_files(
        self,
        source_id: int,
        source_name: str,
        source_type: str,
        source_path: str,
        file_format: str,
        recursive: bool = True
    ) -> Dict:
        """
        Validate that source files exist and can be read
        
        Args:
            source_id: Source ID from metadata table
            source_name: Name of the source
            source_type: Type of source (FILE, RDBMS, API)
            source_path: Full path to source files
            file_format: Expected file format
            recursive: Whether to look recursively for files
        
        Returns:
            Dict with validation results
        """
        validation_id = str(uuid.uuid4())
        
        # Initialize validation result
        result = {
            'validation_id': validation_id,
            'source_id': source_id,
            'source_name': source_name,
            'files_found': False,
            'file_count': 0,
            'check_path_exists': False,
            'check_files_exist': False,
            'check_format_valid': False,
            'check_not_empty': False
        }
        
        try:
            # Check 1: Path exists
            try:
                dbutils.fs.ls(source_path)
                result['check_path_exists'] = True
            except Exception as e:
                result['error_message'] = str(e)
                self._log_validation(source_id, source_name, source_type, source_path, file_format, result)
                return result
            
            # Check 2: Files exist
            files = []
            file_sizes = []
            file_timestamps = []
            
            try:
                file_list = dbutils.fs.ls(source_path)
                
                # Filter for files (not directories) and optionally by format
                for file_info in file_list:
                    file_path = file_info.path
                    file_name = file_info.name
                    
                    # Skip directories
                    if file_name.endswith('/'):
                        continue
                    
                    # Skip checkpoint and schema directories
                    if any(skip in file_path for skip in ['_checkpoint', '_schemas', '.checkpoint']):
                        continue
                    
                    # Add file to list
                    files.append(file_name)
                    file_sizes.append(file_info.size)
                    file_timestamps.append(file_info.modificationTime / 1000)  # Convert to seconds
                
                result['file_count'] = len(files)
                result['files_found'] = len(files) > 0
                result['check_files_exist'] = len(files) > 0
                
                if len(files) == 0:
                    self._log_validation(source_id, source_name, source_type, source_path, file_format, result)
                    return result
                
            except Exception as e:
                result['error_message'] = str(e)
                self._log_validation(source_id, source_name, source_type, source_path, file_format, result)
                return result
            
            # Check 3: Files are not empty
            total_size_bytes = sum(file_sizes)
            total_size_mb = total_size_bytes / (1024 * 1024)
            result['total_size_mb'] = round(total_size_mb, 2)
            result['check_not_empty'] = total_size_bytes > 0
            
            if total_size_bytes == 0:
                self._log_validation(source_id, source_name, source_type, source_path, file_format, result)
                return result
            
            # Build file list (truncate if too long)
            file_list_str = ', '.join(files[:20])  # First 20 files
            if len(files) > 20:
                file_list_str += f" ... and {len(files) - 20} more"
            if len(file_list_str) > 1000:
                file_list_str = file_list_str[:997] + '...'
            result['file_list'] = file_list_str
            
            # File timestamps
            if file_timestamps:
                result['oldest_file_timestamp'] = datetime.fromtimestamp(min(file_timestamps))
                result['newest_file_timestamp'] = datetime.fromtimestamp(max(file_timestamps))
            
        except Exception as e:
            result['error_message'] = str(e)
        
        # Log to audit table
        self._log_validation(source_id, source_name, source_type, source_path, file_format, result)
        
        return result
    
    def _log_validation(
        self,
        source_id: int,
        source_name: str,
        source_type: str,
        source_path: str,
        file_format: str,
        result: Dict
    ):
        """
        Log validation results to audit table
        """
        try:
            # Get user name
            user_name = self.spark.sql("SELECT current_user() as user").collect()[0]['user']
            
            # Get notebook path
            try:
                notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            except:
                notebook_path = 'N/A'
            
            # Escape string values for SQL
            if 'error_message' in result:
                escaped_error_msg = result['error_message'].replace("'", "''")
                error_message_sql = f"'{escaped_error_msg}'"
            else:
                error_message_sql = 'NULL'
            
            run_id_sql = 'NULL' if self.run_id is None else f"'{self.run_id}'"
            file_list_sql = 'NULL' if 'file_list' not in result else f"'{result['file_list']}'"
            oldest_timestamp_sql = 'NULL' if 'oldest_file_timestamp' not in result else f"timestamp'{result['oldest_file_timestamp']}'"
            newest_timestamp_sql = 'NULL' if 'newest_file_timestamp' not in result else f"timestamp'{result['newest_file_timestamp']}'"
            error_code_sql = 'NULL' if 'error_code' not in result else f"'{result['error_code']}'"
            
            # Build INSERT statement
            insert_sql = f"""
            INSERT INTO {self.validation_table}
            VALUES (
                '{result['validation_id']}',
                timestamp'{self.validation_timestamp}',
                {run_id_sql},
                {source_id},
                '{source_name}',
                '{source_type}',
                '{source_path}',
                '{file_format}',
                {result.get('files_found', False)},
                {result.get('file_count', 0)},
                {file_list_sql},
                {result.get('total_size_mb', 0.0)},
                {oldest_timestamp_sql},
                {newest_timestamp_sql},
                {result.get('check_path_exists', False)},
                {result.get('check_files_exist', False)},
                {result.get('check_format_valid', False)},
                {result.get('check_not_empty', False)},
                {error_message_sql},
                {error_code_sql},
                '{self.environment}',
                '{user_name}',
                '{notebook_path}',
                current_timestamp(),
                current_timestamp()
            )
            """
            
            self.spark.sql(insert_sql)
            
        except Exception as e:
            print(f"⚠️ Warning: Failed to log validation result: {str(e)}")

print("="*100)
print("✅ FILEVALIDATOR CLASS DEFINED")
print("="*100)
print("\n💡 This class is now available for use in Ingestion_NB.")
print("="*100)
