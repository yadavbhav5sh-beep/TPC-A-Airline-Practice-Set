from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

BRONZE_TABLE = f"{CATALOG}.bronze.route"

ROUTE_TABLE = f"{CATALOG}.silver.route"
QUARANTINE_TABLE = f"{CATALOG}.silver.route_quarantine"


# ─────────────────────────────────────────────
# 1. Standardize routes (S1-S10)
# ─────────────────────────────────────────────
def standardize_routes(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1:  route key trim; empty -> NULL
    S2:  route key parsed strictly to INT (non-numeric -> NULL)
    S3:  route name trim; empty -> NULL
    S4:  aircraft trim; empty -> NULL
    S5:  airline brand trim; empty -> NULL
    S6:  route type trim + uppercase; empty -> NULL
    S7:  capacity parsed strictly to INT (non-numeric -> NULL)
    S8:  cabin config trim + uppercase; empty -> NULL
    S9:  base price parsed strictly to DECIMAL (non-numeric -> NULL)
    S10: comment trim; empty -> NULL
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1: keep the raw text key for quarantine traceability.
    df = df.withColumn(
        "route_key_raw",
        F.when(
            F.trim(F.col("ro_routeid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ro_routeid").cast("string"))
        )
    )

    # S2: strict integer parse - anything else becomes NULL
    # so the primary key rule (R1) can reject it.
    df = df.withColumn(
        "route_key",
        F.when(
            F.col("route_key_raw").rlike("^-?\\d+$"),
            F.col("route_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S3
    df = df.withColumn(
        "route_name",
        F.when(
            F.trim(F.col("ro_name").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ro_name").cast("string"))
        )
    )

    # S4
    df = df.withColumn(
        "aircraft",
        F.when(
            F.trim(F.col("ro_aircraft").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ro_aircraft").cast("string"))
        )
    )

    # S5
    df = df.withColumn(
        "airline_brand",
        F.when(
            F.trim(F.col("ro_airline_brand").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ro_airline_brand").cast("string"))
        )
    )

    # S6
    df = df.withColumn(
        "route_type",
        F.when(
            F.trim(F.col("ro_routetype").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("ro_routetype").cast("string")))
        )
    )

    # S7: strict integer parse - anything else becomes NULL.
    df = df.withColumn(
        "route_capacity",
        F.when(
            F.trim(F.col("ro_capacity").cast("string"))
                .rlike("^-?\\d+$"),
            F.trim(F.col("ro_capacity").cast("string"))
                .cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S8
    df = df.withColumn(
        "cabin_config",
        F.when(
            F.trim(F.col("ro_cabinconfig").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("ro_cabinconfig").cast("string")))
        )
    )

    # S9: strict numeric parse - anything else becomes NULL.
    df = df.withColumn(
        "base_price",
        F.when(
            F.trim(F.col("ro_baseprice").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("ro_baseprice").cast("string"))
                .cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # S10
    df = df.withColumn(
        "route_comment",
        F.when(
            F.trim(F.col("ro_comment").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ro_comment").cast("string"))
        )
    )

    # Keep _ingestion_at until deduplication has completed.
    return df.drop(
        "_rescued_data",
        "_source_file",
        "pipeline_run_id"
    )


# ─────────────────────────────────────────────
# 2. Rank versions per route key (C2)
# ─────────────────────────────────────────────
def rank_route_versions(df: DataFrame) -> DataFrame:
    """
    Rank every version of each key by _ingestion_at (newest = 1).

    Duplicates are NOT silently dropped: the newest version
    survives in Silver, every older version of a duplicated
    key is quarantined (rule R2).
    """

    window_spec = (
        Window
        .partitionBy("route_key_raw")
        .orderBy(F.col("_ingestion_at").desc_nulls_last())
    )

    return df.withColumn("_rn", F.row_number().over(window_spec))


# ─────────────────────────────────────────────
# 3. Data quality rules (R1-R2)
# ─────────────────────────────────────────────
def validate_routes(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Reject a record if ANY rule fails. Every failed rule is
    collected into rejection_reason as a comma-separated list.

    R1 PRIMARY_KEY_NULL       route key missing or blank
    R2 PRIMARY_KEY_DUPLICATE  older duplicate version of a key
                              (the newest version of the key survives)
    """

    checked = (
        df
        # R1
        .withColumn(
            "_r1",
            F.col("route_key_raw").isNull()
        )
        # R2: older versions of a non-null duplicated key.
        # The newest version (_rn = 1) stays valid.
        .withColumn(
            "_r2",
            F.col("route_key_raw").isNotNull()
            & (F.col("_rn") > 1)
        )
        # Comma-separated list of EVERY failed rule for this record.
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ",",
                F.when(F.col("_r1"), F.lit("PRIMARY_KEY_NULL")),
                F.when(F.col("_r2"), F.lit("PRIMARY_KEY_DUPLICATE"))
            )
        )
        .drop("_r1", "_r2")
    )

    invalid_df = checked.filter(
        F.length(F.col("rejection_reason")) > 0
    )

    valid_df = checked.filter(
        F.length(F.col("rejection_reason")) == 0
    )

    return valid_df, invalid_df


# ─────────────────────────────────────────────
# 4. Create output tables only if missing
# ─────────────────────────────────────────────
def create_tables_if_not_exists():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ROUTE_TABLE} (
            route_key INT,
            route_name STRING,
            aircraft STRING,
            airline_brand STRING,
            route_type STRING,
            route_capacity INT,
            cabin_config STRING,
            base_price DECIMAL(12,2),
            route_comment STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            route_key STRING,
            rejection_reason STRING,
            raw_record STRING,
            quarantined_at TIMESTAMP
        ) USING DELTA
    """)


# ─────────────────────────────────────────────
# 5. Prepare valid and quarantine outputs
# ─────────────────────────────────────────────
def prepare_outputs(
    valid_df: DataFrame,
    invalid_df: DataFrame
) -> tuple[DataFrame, DataFrame]:

    routes_out = valid_df.select(
        "route_key",
        "route_name",
        "aircraft",
        "airline_brand",
        "route_type",
        "route_capacity",
        "cabin_config",
        "base_price",
        "route_comment"
    )

    # NULL keys must stay joinable, so coalesce to a sentinel.
    quarantine_out = invalid_df.select(
        F.coalesce(
            F.col("route_key_raw"), F.lit("<NULL>")
        ).alias("route_key"),
        "rejection_reason",
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return routes_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE routes and remove stale invalid rows
# ─────────────────────────────────────────────
def merge_routes(
    ranked_df: DataFrame,
    routes_out: DataFrame
):
    """
    A route key must be in Silver or fully rejected, never both.
    If no valid version of a key remains, remove its old Silver row.
    """

    ranked_df.select("route_key").distinct().createOrReplaceTempView(
        "_processed_ids"
    )

    routes_out.select("route_key").distinct().createOrReplaceTempView(
        "_valid_route_ids"
    )

    spark.sql(f"""
        MERGE INTO {ROUTE_TABLE} AS t
        USING (
            SELECT p.route_key
            FROM _processed_ids p
            LEFT ANTI JOIN _valid_route_ids v
              ON p.route_key = v.route_key
        ) AS s
        ON t.route_key = s.route_key
        WHEN MATCHED THEN DELETE
    """)

    routes_out.createOrReplaceTempView("_routes_source")

    spark.sql(f"""
        MERGE INTO {ROUTE_TABLE} AS t
        USING _routes_source AS s
        ON t.route_key = s.route_key

        WHEN MATCHED AND (
            NOT (t.route_name <=> s.route_name)
            OR NOT (t.aircraft <=> s.aircraft)
            OR NOT (t.airline_brand <=> s.airline_brand)
            OR NOT (t.route_type <=> s.route_type)
            OR NOT (t.route_capacity <=> s.route_capacity)
            OR NOT (t.cabin_config <=> s.cabin_config)
            OR NOT (t.base_price <=> s.base_price)
            OR NOT (t.route_comment <=> s.route_comment)
        )
        THEN UPDATE SET
            t.route_name = s.route_name,
            t.aircraft = s.aircraft,
            t.airline_brand = s.airline_brand,
            t.route_type = s.route_type,
            t.route_capacity = s.route_capacity,
            t.cabin_config = s.cabin_config,
            t.base_price = s.base_price,
            t.route_comment = s.route_comment

        WHEN NOT MATCHED THEN INSERT (
            route_key, route_name, aircraft, airline_brand, route_type,
            route_capacity, cabin_config, base_price, route_comment
        )
        VALUES (
            s.route_key, s.route_name, s.aircraft, s.airline_brand, s.route_type,
            s.route_capacity, s.cabin_config, s.base_price, s.route_comment
        )
    """)


# ─────────────────────────────────────────────
# 7. MERGE quarantine and recover valid routes
# ─────────────────────────────────────────────
def merge_quarantine(
    ranked_df: DataFrame,
    quarantine_out: DataFrame
):
    """
    Keep one quarantine record per REJECTED RECORD (matched on
    raw_record), so multiple older duplicates of the same key are
    all preserved. Remove quarantine records for keys that no
    longer produce any rejected record.
    """

    ranked_df.select(
        F.coalesce(
            F.col("route_key_raw"), F.lit("<NULL>")
        ).alias("route_key")
    ).distinct().createOrReplaceTempView("_processed_ids")

    quarantine_out.select("route_key").distinct().createOrReplaceTempView(
        "_rejected_ids"
    )

    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE}
        WHERE route_key IN (
            SELECT p.route_key
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.route_key = r.route_key
        )
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.raw_record = s.raw_record

        WHEN MATCHED AND (
            NOT (t.route_key <=> s.route_key)
            OR NOT (t.rejection_reason <=> s.rejection_reason)
        )
        THEN UPDATE SET
            t.route_key = s.route_key,
            t.rejection_reason = s.rejection_reason,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            route_key, rejection_reason, raw_record, quarantined_at
        )
        VALUES (
            s.route_key, s.rejection_reason, s.raw_record, s.quarantined_at
        )
    """)


# ─────────────────────────────────────────────
# 8. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize first
    standardized_df = standardize_routes(bronze_df)

    # Rank versions per key (newest = 1)
    ranked_df = rank_route_versions(standardized_df)

    # Validate: newest version of each key survives,
    # older duplicates and null keys are rejected.
    valid_df, invalid_df = validate_routes(ranked_df)

    # Prepare output DataFrames
    routes_out, quarantine_out = prepare_outputs(valid_df, invalid_df)

    # Create tables once
    create_tables_if_not_exists()

    # Incremental target updates
    merge_routes(ranked_df, routes_out)
    merge_quarantine(ranked_df, quarantine_out)

    print("=" * 45)
    print("Bronze rows:", bronze_df.count())
    print("Distinct route keys:",
          ranked_df.select(
              F.coalesce(
                  F.col("route_key_raw"), F.lit("<NULL>")
              )
          ).distinct().count()
    )
    print("Valid routes:", routes_out.count())
    print("Rejected routes:", quarantine_out.count())
    print("=" * 45)


if __name__ == "__main__":
    main()