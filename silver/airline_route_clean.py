from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

BRONZE_TABLE = f"{CATALOG}.bronze.airline_route"
AIRLINES_TABLE = f"{CATALOG}.silver.airline"
ROUTES_TABLE = f"{CATALOG}.silver.route"

AIRLINE_ROUTE_TABLE = f"{CATALOG}.silver.airline_route"
QUARANTINE_TABLE = f"{CATALOG}.silver.airline_route_quarantine"

# Business rule: operating cost must be greater than 0.
OPERATING_COST_MIN_EXCLUSIVE = 0


# ─────────────────────────────────────────────
# 1. Standardize airline routes (S1-S7)
# ─────────────────────────────────────────────
def standardize_airline_routes(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1: airline key trim; empty -> NULL
    S2: airline key parsed strictly to INT (non-numeric -> NULL)
    S3: route key trim; empty -> NULL
    S4: route key parsed strictly to INT (non-numeric -> NULL)
    S5: available seats parsed strictly to INT (non-numeric -> NULL)
    S6: operating cost parsed strictly to DECIMAL (non-numeric -> NULL)
    S7: comment trim; empty -> NULL
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1: keep the raw text keys for quarantine traceability.
    df = df.withColumn(
        "airline_key_raw",
        F.when(
            F.trim(F.col("ar_airlineid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ar_airlineid").cast("string"))
        )
    )

    # S2: strict integer parse - anything else becomes NULL
    # so the primary key rule (R1) can reject it.
    df = df.withColumn(
        "airline_key",
        F.when(
            F.col("airline_key_raw").rlike("^-?\\d+$"),
            F.col("airline_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S3
    df = df.withColumn(
        "route_key_raw",
        F.when(
            F.trim(F.col("ar_routeid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ar_routeid").cast("string"))
        )
    )

    # S4
    df = df.withColumn(
        "route_key",
        F.when(
            F.col("route_key_raw").rlike("^-?\\d+$"),
            F.col("route_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S5: strict integer parse - anything else becomes NULL.
    df = df.withColumn(
        "available_seats",
        F.when(
            F.trim(F.col("ar_available_seats").cast("string"))
                .rlike("^-?\\d+$"),
            F.trim(F.col("ar_available_seats").cast("string"))
                .cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S6: strict numeric parse - anything else becomes NULL
    # so the numeric range rule (R5) can reject it.
    df = df.withColumn(
        "operating_cost",
        F.when(
            F.trim(F.col("ar_operatingcost").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("ar_operatingcost").cast("string"))
                .cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # S7
    df = df.withColumn(
        "airline_route_comment",
        F.when(
            F.trim(F.col("ar_comment").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("ar_comment").cast("string"))
        )
    )

    # Keep _ingestion_at until deduplication has completed.
    return df.drop(
        "_rescued_data",
        "_source_file",
        "pipeline_run_id"
    )


# ─────────────────────────────────────────────
# 2. Rank versions per composite key (C2)
# ─────────────────────────────────────────────
def rank_airline_route_versions(df: DataFrame) -> DataFrame:
    """
    Rank every version of each composite key (airline + route)
    by _ingestion_at (newest = 1).

    Duplicates are NOT silently dropped: the newest version
    survives in Silver, every older version of a duplicated
    composite key is quarantined (rule R2).
    """

    window_spec = (
        Window
        .partitionBy("airline_key_raw", "route_key_raw")
        .orderBy(F.col("_ingestion_at").desc_nulls_last())
    )

    return df.withColumn("_rn", F.row_number().over(window_spec))


# ─────────────────────────────────────────────
# 3. Data quality rules (R1-R5)
# ─────────────────────────────────────────────
def validate_airline_routes(
    df: DataFrame,
    airlines_df: DataFrame,
    routes_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject a record if ANY rule fails. Every failed rule is
    collected into rejection_reason as a comma-separated list.

    R1 PRIMARY_KEY_NULL        airline or route key missing/blank
    R2 PRIMARY_KEY_DUPLICATE   older duplicate version of the composite
                              key (the newest version survives)
    R3 UNKNOWN_AIRLINE         airline key not found in silver.airline
    R4 UNKNOWN_ROUTE           route key not found in silver.route
    R5 OPERATING_COST_RANGE    operating cost missing, zero or negative
    """

    # Valid references from the cleaned Silver tables.
    known_airlines = (
        airlines_df
        .select(F.col("airline_key").alias("_known_airline"))
        .filter(F.col("_known_airline").isNotNull())
        .distinct()
    )

    known_routes = (
        routes_df
        .select(F.col("route_key").alias("_known_route"))
        .filter(F.col("_known_route").isNotNull())
        .distinct()
    )

    checked = (
        df
        # R1: either part of the composite key missing.
        .withColumn(
            "_r1",
            F.col("airline_key_raw").isNull()
            | F.col("route_key_raw").isNull()
        )
        # R2: older versions of a fully present composite key.
        # The newest version (_rn = 1) stays valid.
        .withColumn(
            "_r2",
            F.col("airline_key_raw").isNotNull()
            & F.col("route_key_raw").isNotNull()
            & (F.col("_rn") > 1)
        )
        .join(
            known_airlines,
            F.col("airline_key") == F.col("_known_airline"),
            "left"
        )
        .join(
            known_routes,
            F.col("route_key") == F.col("_known_route"),
            "left"
        )
        # R3: referential integrity - airline key must exist
        # in silver.airline.
        .withColumn(
            "_r3",
            F.col("airline_key_raw").isNull()
            | F.col("_known_airline").isNull()
        )
        # R4: referential integrity - route key must exist
        # in silver.route.
        .withColumn(
            "_r4",
            F.col("route_key_raw").isNull()
            | F.col("_known_route").isNull()
        )
        # R5: numeric range - operating cost must be > 0.
        # Unparseable (NULL) values are rejected as well.
        .withColumn(
            "_r5",
            F.col("operating_cost").isNull()
            | (F.col("operating_cost")
               <= F.lit(OPERATING_COST_MIN_EXCLUSIVE))
        )
        # Comma-separated list of EVERY failed rule for this record.
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ",",
                F.when(F.col("_r1"), F.lit("PRIMARY_KEY_NULL")),
                F.when(F.col("_r2"), F.lit("PRIMARY_KEY_DUPLICATE")),
                F.when(F.col("_r3"), F.lit("UNKNOWN_AIRLINE")),
                F.when(F.col("_r4"), F.lit("UNKNOWN_ROUTE")),
                F.when(F.col("_r5"), F.lit("OPERATING_COST_RANGE"))
            )
        )
        .drop("_r1", "_r2", "_r3", "_r4", "_r5",
             "_known_airline", "_known_route")
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
        CREATE TABLE IF NOT EXISTS {AIRLINE_ROUTE_TABLE} (
            airline_key INT,
            route_key INT,
            available_seats INT,
            operating_cost DECIMAL(12,2),
            airline_route_comment STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            airline_key STRING,
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

    airline_routes_out = valid_df.select(
        "airline_key",
        "route_key",
        "available_seats",
        "operating_cost",
        "airline_route_comment"
    )

    # NULL keys must stay joinable, so coalesce to a sentinel.
    quarantine_out = invalid_df.select(
        F.coalesce(
            F.col("airline_key_raw"), F.lit("<NULL>")
        ).alias("airline_key"),
        F.coalesce(
            F.col("route_key_raw"), F.lit("<NULL>")
        ).alias("route_key"),
        "rejection_reason",
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return airline_routes_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE airline routes and remove stale rows
# ─────────────────────────────────────────────
def merge_airline_routes(
    ranked_df: DataFrame,
    airline_routes_out: DataFrame
):
    """
    A composite key must be in Silver or fully rejected,
    never both. If no valid version of a key remains,
    remove its old Silver row.
    """

    ranked_df.select("airline_key", "route_key").distinct() \
        .createOrReplaceTempView("_processed_ids")

    airline_routes_out.select("airline_key", "route_key").distinct() \
        .createOrReplaceTempView("_valid_ids")

    spark.sql(f"""
        MERGE INTO {AIRLINE_ROUTE_TABLE} AS t
        USING (
            SELECT p.airline_key, p.route_key
            FROM _processed_ids p
            LEFT ANTI JOIN _valid_ids v
              ON p.airline_key = v.airline_key
             AND p.route_key = v.route_key
        ) AS s
        ON t.airline_key = s.airline_key
       AND t.route_key = s.route_key
        WHEN MATCHED THEN DELETE
    """)

    airline_routes_out.createOrReplaceTempView("_airline_routes_source")

    spark.sql(f"""
        MERGE INTO {AIRLINE_ROUTE_TABLE} AS t
        USING _airline_routes_source AS s
        ON t.airline_key = s.airline_key
       AND t.route_key = s.route_key

        WHEN MATCHED AND (
            NOT (t.available_seats <=> s.available_seats)
            OR NOT (t.operating_cost <=> s.operating_cost)
            OR NOT (t.airline_route_comment <=> s.airline_route_comment)
        )
        THEN UPDATE SET
            t.available_seats = s.available_seats,
            t.operating_cost = s.operating_cost,
            t.airline_route_comment = s.airline_route_comment

        WHEN NOT MATCHED THEN INSERT (
            airline_key, route_key, available_seats,
            operating_cost, airline_route_comment
        )
        VALUES (
            s.airline_key, s.route_key, s.available_seats,
            s.operating_cost, s.airline_route_comment
        )
    """)


# ─────────────────────────────────────────────
# 7. MERGE quarantine and recover valid rows
# ─────────────────────────────────────────────
def merge_quarantine(
    ranked_df: DataFrame,
    quarantine_out: DataFrame
):
    """
    Keep one quarantine record per REJECTED RECORD (matched on
    raw_record), so multiple older duplicates of the same composite
    key are all preserved. Remove quarantine records for composite
    keys that no longer produce any rejected record.
    """

    ranked_df.select(
        F.coalesce(
            F.col("airline_key_raw"), F.lit("<NULL>")
        ).alias("airline_key"),
        F.coalesce(
            F.col("route_key_raw"), F.lit("<NULL>")
        ).alias("route_key")
    ).distinct().createOrReplaceTempView("_processed_ids")

    quarantine_out.select("airline_key", "route_key").distinct() \
        .createOrReplaceTempView("_rejected_ids")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING (
            SELECT p.airline_key, p.route_key
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.airline_key = r.airline_key
             AND p.route_key = r.route_key
        ) AS s
        ON t.airline_key = s.airline_key
       AND t.route_key = s.route_key
        WHEN MATCHED THEN DELETE
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.raw_record = s.raw_record

        WHEN MATCHED AND (
            NOT (t.airline_key <=> s.airline_key)
            OR NOT (t.route_key <=> s.route_key)
            OR NOT (t.rejection_reason <=> s.rejection_reason)
        )
        THEN UPDATE SET
            t.airline_key = s.airline_key,
            t.route_key = s.route_key,
            t.rejection_reason = s.rejection_reason,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            airline_key, route_key, rejection_reason,
            raw_record, quarantined_at
        )
        VALUES (
            s.airline_key, s.route_key, s.rejection_reason,
            s.raw_record, s.quarantined_at
        )
    """)


# ─────────────────────────────────────────────
# 8. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize first
    standardized_df = standardize_airline_routes(bronze_df)

    # Rank versions per composite key (newest = 1)
    ranked_df = rank_airline_route_versions(standardized_df)

    # Read dependencies
    airlines_df = spark.read.table(AIRLINES_TABLE)
    routes_df = spark.read.table(ROUTES_TABLE)

    # Validate: newest version of each composite key survives,
    # older duplicates and null keys are rejected alongside
    # unknown airlines/routes and invalid operating costs.
    valid_df, invalid_df = validate_airline_routes(
        ranked_df, airlines_df, routes_df
    )

    # Prepare output DataFrames
    airline_routes_out, quarantine_out = prepare_outputs(
        valid_df, invalid_df
    )

    # Create tables once
    create_tables_if_not_exists()

    # Incremental target updates
    merge_airline_routes(ranked_df, airline_routes_out)
    merge_quarantine(ranked_df, quarantine_out)

    print("=" * 45)
    print("Bronze rows:", bronze_df.count())
    print("Valid airline routes:", airline_routes_out.count())
    print("Rejected airline routes:", quarantine_out.count())
    print("=" * 45)


if __name__ == "__main__":
    main()