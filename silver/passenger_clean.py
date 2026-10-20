from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

BRONZE_TABLE = f"{CATALOG}.bronze.passenger"
NATIONS_TABLE = f"{CATALOG}.silver.nation"

PASSENGER_TABLE = f"{CATALOG}.silver.passenger"
QUARANTINE_TABLE = f"{CATALOG}.silver.passenger_quarantine"


# ─────────────────────────────────────────────
# 1. Standardize passengers (S1-S9)
# ─────────────────────────────────────────────
def standardize_passengers(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1: passenger key trim; empty -> NULL
    S2: passenger key parsed strictly to INT (non-numeric -> NULL)
    S3: nation key trim + strict INT parse (non-numeric -> NULL)
    S4: name trim; empty -> NULL
    S5: address trim; empty -> NULL
    S6: phone trim; empty -> NULL
    S7: loyalty points parsed strictly to DECIMAL (non-numeric -> NULL)
    S8: travel purpose trim + uppercase; empty -> NULL
    S9: comment trim; empty -> NULL
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1: keep the raw text key for quarantine traceability.
    df = df.withColumn(
        "passenger_key_raw",
        F.when(
            F.trim(F.col("pa_passengerid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("pa_passengerid").cast("string"))
        )
    )

    # S2: strict integer parse - anything else becomes NULL
    # so the primary key rule (R1) can reject it.
    df = df.withColumn(
        "passenger_key",
        F.when(
            F.col("passenger_key_raw").rlike("^-?\\d+$"),
            F.col("passenger_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S3: nation key, used by the referential integrity rule (R3).
    df = df.withColumn(
        "nation_key_raw",
        F.when(
            F.trim(F.col("pa_nationkey").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("pa_nationkey").cast("string"))
        )
    )

    df = df.withColumn(
        "nation_key",
        F.when(
            F.col("nation_key_raw").rlike("^-?\\d+$"),
            F.col("nation_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S4
    df = df.withColumn(
        "passenger_name",
        F.when(
            F.trim(F.col("pa_name").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("pa_name").cast("string"))
        )
    )

    # S5
    df = df.withColumn(
        "passenger_address",
        F.when(
            F.trim(F.col("pa_address").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("pa_address").cast("string"))
        )
    )

    # S6
    df = df.withColumn(
        "passenger_phone",
        F.when(
            F.trim(F.col("pa_phone").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("pa_phone").cast("string"))
        )
    )

    # S7: strict numeric parse - anything else becomes NULL.
    df = df.withColumn(
        "loyalty_points",
        F.when(
            F.trim(F.col("pa_loyaltypoints").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("pa_loyaltypoints").cast("string"))
                .cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # S8
    df = df.withColumn(
        "travel_purpose",
        F.when(
            F.trim(F.col("pa_travelpurpose").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("pa_travelpurpose").cast("string")))
        )
    )

    # S9
    df = df.withColumn(
        "passenger_comment",
        F.when(
            F.trim(F.col("pa_comment").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("pa_comment").cast("string"))
        )
    )

    # Keep _ingestion_at until deduplication has completed.
    return df.drop(
        "_rescued_data",
        "_source_file",
        "pipeline_run_id"
    )


# ─────────────────────────────────────────────
# 2. Rank versions per passenger key (C2)
# ─────────────────────────────────────────────
def rank_passenger_versions(df: DataFrame) -> DataFrame:
    """
    Rank every version of each key by _ingestion_at (newest = 1).

    Unlike region/nation/airline, duplicates are NOT silently dropped:
    the newest version survives in Silver, every older version of a
    duplicated key is quarantined (rule R2).
    """

    window_spec = (
        Window
        .partitionBy("passenger_key_raw")
        .orderBy(F.col("_ingestion_at").desc_nulls_last())
    )

    return df.withColumn("_rn", F.row_number().over(window_spec))


# ─────────────────────────────────────────────
# 3. Data quality rules (R1-R3)
# ─────────────────────────────────────────────
def validate_passengers(
    df: DataFrame,
    nations_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject a record if ANY rule fails. Every failed rule is
    collected into rejection_reason as a comma-separated list.

    R1 PRIMARY_KEY_NULL       passenger key missing or blank
    R2 PRIMARY_KEY_DUPLICATE   older duplicate version of a key
                              (the newest version of the key survives)
    R3 UNKNOWN_NATION         nation key missing or not found in silver.nation
    """

    # Valid nation references from the cleaned Silver nation table.
    known_nations = (
        nations_df
        .select(F.col("nation_key").alias("_known_nation"))
        .filter(F.col("_known_nation").isNotNull())
        .distinct()
    )

    checked = (
        df
        # R1
        .withColumn(
            "_r1",
            F.col("passenger_key_raw").isNull()
        )
        # R2: older versions of a non-null duplicated key.
        # The newest version (_rn = 1) stays valid.
        .withColumn(
            "_r2",
            F.col("passenger_key_raw").isNotNull()
            & (F.col("_rn") > 1)
        )
        .join(
            known_nations,
            F.col("nation_key") == F.col("_known_nation"),
            "left"
        )
        # R3: referential integrity - nation key must exist in silver.nation.
        .withColumn(
            "_r3",
            F.col("nation_key_raw").isNull()
            | F.col("_known_nation").isNull()
        )
        # Comma-separated list of EVERY failed rule for this record.
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ",",
                F.when(F.col("_r1"), F.lit("PRIMARY_KEY_NULL")),
                F.when(F.col("_r2"), F.lit("PRIMARY_KEY_DUPLICATE")),
                F.when(F.col("_r3"), F.lit("UNKNOWN_NATION"))
            )
        )
        .drop("_r1", "_r2", "_r3", "_known_nation")
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
        CREATE TABLE IF NOT EXISTS {PASSENGER_TABLE} (
            passenger_key INT,
            passenger_name STRING,
            passenger_address STRING,
            nation_key INT,
            passenger_phone STRING,
            loyalty_points DECIMAL(12,2),
            travel_purpose STRING,
            passenger_comment STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            passenger_key STRING,
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

    passengers_out = valid_df.select(
        "passenger_key",
        "passenger_name",
        "passenger_address",
        "nation_key",
        "passenger_phone",
        "loyalty_points",
        "travel_purpose",
        "passenger_comment"
    )

    # NULL keys must stay joinable, so coalesce to a sentinel.
    quarantine_out = invalid_df.select(
        F.coalesce(
            F.col("passenger_key_raw"), F.lit("<NULL>")
        ).alias("passenger_key"),
        "rejection_reason",
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return passengers_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE passengers and remove stale invalid rows
# ─────────────────────────────────────────────
def merge_passengers(
    ranked_df: DataFrame,
    passengers_out: DataFrame
):
    """
    A passenger key must be in Silver or fully rejected, never both.
    If no valid version of a key remains, remove its old Silver row.
    """

    ranked_df.select("passenger_key").distinct().createOrReplaceTempView(
        "_processed_ids"
    )

    passengers_out.select("passenger_key").distinct().createOrReplaceTempView(
        "_valid_passenger_ids"
    )

    spark.sql(f"""
        MERGE INTO {PASSENGER_TABLE} AS t
        USING (
            SELECT p.passenger_key
            FROM _processed_ids p
            LEFT ANTI JOIN _valid_passenger_ids v
              ON p.passenger_key = v.passenger_key
        ) AS s
        ON t.passenger_key = s.passenger_key
        WHEN MATCHED THEN DELETE
    """)

    passengers_out.createOrReplaceTempView("_passengers_source")

    spark.sql(f"""
        MERGE INTO {PASSENGER_TABLE} AS t
        USING _passengers_source AS s
        ON t.passenger_key = s.passenger_key

        WHEN MATCHED AND (
            NOT (t.passenger_name <=> s.passenger_name)
            OR NOT (t.passenger_address <=> s.passenger_address)
            OR NOT (t.nation_key <=> s.nation_key)
            OR NOT (t.passenger_phone <=> s.passenger_phone)
            OR NOT (t.loyalty_points <=> s.loyalty_points)
            OR NOT (t.travel_purpose <=> s.travel_purpose)
            OR NOT (t.passenger_comment <=> s.passenger_comment)
        )
        THEN UPDATE SET
            t.passenger_name = s.passenger_name,
            t.passenger_address = s.passenger_address,
            t.nation_key = s.nation_key,
            t.passenger_phone = s.passenger_phone,
            t.loyalty_points = s.loyalty_points,
            t.travel_purpose = s.travel_purpose,
            t.passenger_comment = s.passenger_comment

        WHEN NOT MATCHED THEN INSERT (
            passenger_key, passenger_name, passenger_address, nation_key,
            passenger_phone, loyalty_points, travel_purpose, passenger_comment
        )
        VALUES (
            s.passenger_key, s.passenger_name, s.passenger_address, s.nation_key,
            s.passenger_phone, s.loyalty_points, s.travel_purpose, s.passenger_comment
        )
    """)


# ─────────────────────────────────────────────
# 7. MERGE quarantine and recover valid passengers
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
            F.col("passenger_key_raw"), F.lit("<NULL>")
        ).alias("passenger_key")
    ).distinct().createOrReplaceTempView("_processed_ids")

    quarantine_out.select("passenger_key").distinct().createOrReplaceTempView(
        "_rejected_ids"
    )

    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE}
        WHERE passenger_key IN (
            SELECT p.passenger_key
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.passenger_key = r.passenger_key
        )
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.raw_record = s.raw_record

        WHEN MATCHED AND (
            NOT (t.passenger_key <=> s.passenger_key)
            OR NOT (t.rejection_reason <=> s.rejection_reason)
        )
        THEN UPDATE SET
            t.passenger_key = s.passenger_key,
            t.rejection_reason = s.rejection_reason,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            passenger_key, rejection_reason, raw_record, quarantined_at
        )
        VALUES (
            s.passenger_key, s.rejection_reason, s.raw_record, s.quarantined_at
        )
    """)


# ─────────────────────────────────────────────
# 8. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize first
    standardized_df = standardize_passengers(bronze_df)

    # Rank versions per key (newest = 1)
    ranked_df = rank_passenger_versions(standardized_df)

    # Read dependencies
    nations_df = spark.read.table(NATIONS_TABLE)

    # Validate: newest version of each key survives,
    # older duplicates are rejected alongside null keys
    # and unknown nations.
    valid_df, invalid_df = validate_passengers(ranked_df, nations_df)

    # Prepare output DataFrames
    passengers_out, quarantine_out = prepare_outputs(valid_df, invalid_df)

    # Create tables once
    create_tables_if_not_exists()

    # Incremental target updates
    merge_passengers(ranked_df, passengers_out)
    merge_quarantine(ranked_df, quarantine_out)

    print("=" * 45)
    print("Bronze rows:", bronze_df.count())
    print("Distinct passenger keys:",
          ranked_df.select(
              F.coalesce(
                  F.col("passenger_key_raw"), F.lit("<NULL>")
              )
          ).distinct().count()
    )
    print("Valid passengers:", passengers_out.count())
    print("Rejected passengers:", quarantine_out.count())
    print("=" * 45)

if __name__ == "__main__":
    main()