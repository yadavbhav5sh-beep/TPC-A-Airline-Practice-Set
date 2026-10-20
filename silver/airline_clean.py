from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

BRONZE_TABLE = f"{CATALOG}.bronze.airline"
NATIONS_TABLE = f"{CATALOG}.silver.nation"

AIRLINE_TABLE = f"{CATALOG}.silver.airline"
QUARANTINE_TABLE = f"{CATALOG}.silver.airline_quarantine"


# ─────────────────────────────────────────────
# 1. Standardize airlines (S1-S8)
# ─────────────────────────────────────────────
def standardize_airlines(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1: airline key trim; empty -> NULL
    S2: airline key parsed strictly to INT (non-numeric -> NULL)
    S3: nation key trim + strict INT parse (non-numeric -> NULL)
    S4: name trim; empty -> NULL
    S5: address trim; empty -> NULL
    S6: phone trim; empty -> NULL
    S7: rating parsed strictly to DECIMAL (non-numeric -> NULL)
    S8: comment trim; empty -> NULL
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1: keep the raw text key for quarantine traceability.
    df = df.withColumn(
        "airline_key_raw",
        F.when(
            F.trim(F.col("al_airlineid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("al_airlineid").cast("string"))
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

    # S3: nation key, used by the referential integrity rule (R3).
    df = df.withColumn(
        "nation_key_raw",
        F.when(
            F.trim(F.col("al_nationkey").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("al_nationkey").cast("string"))
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
        "airline_name",
        F.when(
            F.trim(F.col("al_name").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("al_name").cast("string"))
        )
    )

    # S5
    df = df.withColumn(
        "airline_address",
        F.when(
            F.trim(F.col("al_address").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("al_address").cast("string"))
        )
    )

    # S6
    df = df.withColumn(
        "airline_phone",
        F.when(
            F.trim(F.col("al_phone").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("al_phone").cast("string"))
        )
    )

    # S7: strict numeric parse - anything else becomes NULL.
    df = df.withColumn(
        "airline_rating",
        F.when(
            F.trim(F.col("al_rating").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("al_rating").cast("string"))
                .cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # S8
    df = df.withColumn(
        "airline_comment",
        F.when(
            F.trim(F.col("al_comment").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("al_comment").cast("string"))
        )
    )

    # Keep _ingestion_at until deduplication has completed.
    return df.drop(
        "_rescued_data",
        "_source_file",
        "pipeline_run_id"
    )

# ─────────────────────────────────────────────
# 2. Latest version per airline key (C2)
# ─────────────────────────────────────────────
def latest_airline_versions(df: DataFrame) -> DataFrame:
    """
    Validate only the most recently ingested version of each key.

    _ingestion_at is used as the ordering key.
    """

    window_spec = (
        Window
        .partitionBy("airline_key_raw")
        .orderBy(F.col("_ingestion_at").desc_nulls_last())
    )

    return (
        df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ─────────────────────────────────────────────
# 3. Data quality rules (R1-R3)
# ─────────────────────────────────────────────
def validate_airlines(
    df: DataFrame,
    nations_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject a record if ANY rule fails. Every failed rule is
    collected into rejection_reason as a comma-separated list.

    R1 PRIMARY_KEY_NULL       airline key missing or blank
    R2 PRIMARY_KEY_DUPLICATE  airline key occurs more than once in the batch
    R3 UNKNOWN_NATION         nation key missing or not found in silver.nation
    """

    # Valid nation references from the cleaned Silver nation table.
    known_nations = (
        nations_df
        .select(F.col("nation_key").alias("_known_nation"))
        .filter(F.col("_known_nation").isNotNull())
        .distinct()
    )

    # How many rows share this key in this batch.
    duplicate_count = F.count(F.lit(1)).over(
        Window.partitionBy("airline_key_raw")
    )

    checked = (
        df.withColumn("_key_dup_count", duplicate_count)
        # R1
        .withColumn(
            "_r1",
            F.col("airline_key_raw").isNull()
        )
        # R2: only non-null keys can be duplicates.
        .withColumn(
            "_r2",
            F.col("airline_key_raw").isNotNull()
            & (F.col("_key_dup_count") > 1)
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
        .drop("_key_dup_count", "_r1", "_r2", "_r3", "_known_nation")
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
        CREATE TABLE IF NOT EXISTS {AIRLINE_TABLE} (
            airline_key INT,
            airline_name STRING,
            airline_address STRING,
            nation_key INT,
            airline_phone STRING,
            airline_rating DECIMAL(12,2),
            airline_comment STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            airline_key STRING,
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

    airlines_out = valid_df.select(
        "airline_key",
        "airline_name",
        "airline_address",
        "nation_key",
        "airline_phone",
        "airline_rating",
        "airline_comment"
    )

    # NULL keys must stay joinable, so coalesce to a sentinel.
    quarantine_out = invalid_df.select(
        F.coalesce(
            F.col("airline_key_raw"), F.lit("<NULL>")
        ).alias("airline_key"),
        "rejection_reason",
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return airlines_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE airlines and remove stale invalid rows
# ─────────────────────────────────────────────
def merge_airlines(
    latest_df: DataFrame,
    airlines_out: DataFrame
):
    """
    An airline must be in Silver or quarantine, never both.
    If its latest version is rejected, remove its old Silver row.
    """

    latest_df.select("airline_key").distinct().createOrReplaceTempView(
        "_latest_airline_ids"
    )

    airlines_out.select("airline_key").distinct().createOrReplaceTempView(
        "_valid_airline_ids"
    )

    spark.sql(f"""
        MERGE INTO {AIRLINE_TABLE} AS t
        USING (
            SELECT l.airline_key
            FROM _latest_airline_ids l
            LEFT ANTI JOIN _valid_airline_ids v
              ON l.airline_key = v.airline_key
        ) AS s
        ON t.airline_key = s.airline_key
        WHEN MATCHED THEN DELETE
    """)

    airlines_out.createOrReplaceTempView("_airlines_source")

    spark.sql(f"""
        MERGE INTO {AIRLINE_TABLE} AS t
        USING _airlines_source AS s
        ON t.airline_key = s.airline_key

        WHEN MATCHED AND (
            NOT (t.airline_name <=> s.airline_name)
            OR NOT (t.airline_address <=> s.airline_address)
            OR NOT (t.nation_key <=> s.nation_key)
            OR NOT (t.airline_phone <=> s.airline_phone)
            OR NOT (t.airline_rating <=> s.airline_rating)
            OR NOT (t.airline_comment <=> s.airline_comment)
        )
        THEN UPDATE SET
            t.airline_name = s.airline_name,
            t.airline_address = s.airline_address,
            t.nation_key = s.nation_key,
            t.airline_phone = s.airline_phone,
            t.airline_rating = s.airline_rating,
            t.airline_comment = s.airline_comment

        WHEN NOT MATCHED THEN INSERT (
            airline_key, airline_name, airline_address, nation_key,
            airline_phone, airline_rating, airline_comment
        )
        VALUES (
            s.airline_key, s.airline_name, s.airline_address, s.nation_key,
            s.airline_phone, s.airline_rating, s.airline_comment
        )
    """)

# ─────────────────────────────────────────────
# 7. MERGE quarantine and recover valid airlines
# ─────────────────────────────────────────────
def merge_quarantine(
    latest_df: DataFrame,
    quarantine_out: DataFrame
):
    """
    Keep one quarantine record per rejected key.
    Remove quarantine records for airlines now valid.
    """

    latest_df.select(
        F.coalesce(
            F.col("airline_key_raw"), F.lit("<NULL>")
        ).alias("airline_key")
    ).distinct().createOrReplaceTempView("_processed_ids")

    quarantine_out.select("airline_key").distinct().createOrReplaceTempView(
        "_rejected_ids"
    )

    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE}
        WHERE airline_key IN (
            SELECT p.airline_key
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.airline_key = r.airline_key
        )
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.airline_key = s.airline_key

        WHEN MATCHED AND (
            NOT (t.rejection_reason <=> s.rejection_reason)
            OR NOT (t.raw_record <=> s.raw_record)
        )
        THEN UPDATE SET
            t.rejection_reason = s.rejection_reason,
            t.raw_record = s.raw_record,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            airline_key, rejection_reason, raw_record, quarantined_at
        )
        VALUES (
            s.airline_key, s.rejection_reason, s.raw_record, s.quarantined_at
        )
    """)


# ─────────────────────────────────────────────
# 8. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize first
    standardized_df = standardize_airlines(bronze_df)

    # Keep latest version per airline key
    latest_df = latest_airline_versions(standardized_df)

    # Read dependencies
    nations_df = spark.read.table(NATIONS_TABLE)

    # Validate latest version only
    valid_df, invalid_df = validate_airlines(latest_df, nations_df)

    # Prepare output DataFrames
    airlines_out, quarantine_out = prepare_outputs(valid_df, invalid_df)

    # Create tables once
    create_tables_if_not_exists()

    # Incremental target updates
    merge_airlines(latest_df, airlines_out)
    merge_quarantine(latest_df, quarantine_out)

    print("=" * 45)
    print("Latest airline versions:", latest_df.count())
    print("Valid airlines:", airlines_out.count())
    print("Rejected airlines:", quarantine_out.count())
    print("=" * 45)

if __name__ == "__main__":
    main()