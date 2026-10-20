from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

BRONZE_TABLE = f"{CATALOG}.bronze.booking"
PASSENGERS_TABLE = f"{CATALOG}.silver.passenger"

BOOKING_TABLE = f"{CATALOG}.silver.booking"
QUARANTINE_TABLE = f"{CATALOG}.silver.booking_quarantine"

# Business rule: booking date must be between
# 1994-01-01 and 1998-12-31, both inclusive.
BOOKING_DATE_MIN = F.lit("1994-01-01").cast("date")
BOOKING_DATE_MAX = F.lit("1998-12-31").cast("date")


# ─────────────────────────────────────────────
# 1. Standardize bookings (S1-S11)
# ─────────────────────────────────────────────
def standardize_bookings(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1:  booking key trim; empty -> NULL
    S2:  booking key parsed strictly to INT (non-numeric -> NULL)
    S3:  passenger key trim; empty -> NULL
    S4:  passenger key parsed strictly to INT (non-numeric -> NULL)
    S5:  status trim + uppercase; empty -> NULL
    S6:  total price parsed strictly to DECIMAL (non-numeric -> NULL)
    S7:  booking date parsed strictly to DATE (invalid -> NULL)
    S8:  flight date parsed strictly to DATE (invalid -> NULL)
    S9:  booking priority trim + uppercase; empty -> NULL
    S10: agent trim; empty -> NULL
    S11: comment trim; empty -> NULL
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1: keep the raw text key for quarantine traceability.
    df = df.withColumn(
        "booking_key_raw",
        F.when(
            F.trim(F.col("bk_bookingid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("bk_bookingid").cast("string"))
        )
    )

    # S2: strict integer parse - anything else becomes NULL
    # so the primary key rule (R1) can reject it.
    df = df.withColumn(
        "booking_key",
        F.when(
            F.col("booking_key_raw").rlike("^-?\\d+$"),
            F.col("booking_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S3: passenger key, used by the referential integrity rule (R4).
    df = df.withColumn(
        "passenger_key_raw",
        F.when(
            F.trim(F.col("bk_passengerid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("bk_passengerid").cast("string"))
        )
    )

    # S4
    df = df.withColumn(
        "passenger_key",
        F.when(
            F.col("passenger_key_raw").rlike("^-?\\d+$"),
            F.col("passenger_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S5
    df = df.withColumn(
        "booking_status",
        F.when(
            F.trim(F.col("bk_status").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("bk_status").cast("string")))
        )
    )

    # S6: strict numeric parse - anything else becomes NULL.
    df = df.withColumn(
        "total_price",
        F.when(
            F.trim(F.col("bk_totalprice").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("bk_totalprice").cast("string"))
                .cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # S7: strict date parse - invalid dates become NULL so the
    # date range rule (R3) can reject them.
    df = df.withColumn(
        "booking_date",
        F.expr("try_to_date(trim(bk_bookingdate))")
    )

    # S8
    df = df.withColumn(
        "flight_date",
        F.expr("try_to_date(trim(bk_flightdate))")
    )

    # S9
    df = df.withColumn(
        "booking_priority",
        F.when(
            F.trim(F.col("bk_bookingpriority").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("bk_bookingpriority").cast("string")))
        )
    )

    # S10
    df = df.withColumn(
        "booking_agent",
        F.when(
            F.trim(F.col("bk_agent").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("bk_agent").cast("string"))
        )
    )

    # S11
    df = df.withColumn(
        "booking_comment",
        F.when(
            F.trim(F.col("bk_comment").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("bk_comment").cast("string"))
        )
    )

    # Keep _ingestion_at until deduplication has completed.
    return df.drop(
        "_rescued_data",
        "_source_file",
        "pipeline_run_id"
    )


# ─────────────────────────────────────────────
# 2. Rank versions per booking key (C2)
# ─────────────────────────────────────────────
def rank_booking_versions(df: DataFrame) -> DataFrame:
    """
    Rank every version of each key by _ingestion_at (newest = 1).

    Duplicates are NOT silently dropped: the newest version
    survives in Silver, every older version of a duplicated
    key is quarantined (rule R2).
    """

    window_spec = (
        Window
        .partitionBy("booking_key_raw")
        .orderBy(F.col("_ingestion_at").desc_nulls_last())
    )

    return df.withColumn("_rn", F.row_number().over(window_spec))


# ─────────────────────────────────────────────
# 3. Data quality rules (R1-R4)
# ─────────────────────────────────────────────
def validate_bookings(
    df: DataFrame,
    passengers_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject a record if ANY rule fails. Every failed rule is
    collected into rejection_reason as a comma-separated list.

    R1 PRIMARY_KEY_NULL      booking key missing or blank
    R2 PRIMARY_KEY_DUPLICATE older duplicate version of a key
                             (the newest version of the key survives)
    R3 BOOKING_DATE_RANGE    booking date missing/unparseable, or
                             outside 1994-01-01 .. 1998-12-31 (inclusive)
    R4 UNKNOWN_PASSENGER     passenger key missing or not found
                             in silver.passenger
    """

    # Valid passenger references from the cleaned Silver table.
    known_passengers = (
        passengers_df
        .select(F.col("passenger_key").alias("_known_passenger"))
        .filter(F.col("_known_passenger").isNotNull())
        .distinct()
    )

    checked = (
        df
        # R1
        .withColumn(
            "_r1",
            F.col("booking_key_raw").isNull()
        )
        # R2: older versions of a non-null duplicated key.
        # The newest version (_rn = 1) stays valid.
        .withColumn(
            "_r2",
            F.col("booking_key_raw").isNotNull()
            & (F.col("_rn") > 1)
        )
        # R3: date range - booking date must be within
        # [1994-01-01, 1998-12-31], both inclusive.
        # Unparseable (NULL) dates are rejected as well.
        .withColumn(
            "_r3",
            F.col("booking_date").isNull()
            | (F.col("booking_date") < BOOKING_DATE_MIN)
            | (F.col("booking_date") > BOOKING_DATE_MAX)
        )
        .join(
            known_passengers,
            F.col("passenger_key") == F.col("_known_passenger"),
            "left"
        )
        # R4: referential integrity - passenger key must exist
        # in silver.passenger.
        .withColumn(
            "_r4",
            F.col("passenger_key_raw").isNull()
            | F.col("_known_passenger").isNull()
        )
        # Comma-separated list of EVERY failed rule for this record.
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ",",
                F.when(F.col("_r1"), F.lit("PRIMARY_KEY_NULL")),
                F.when(F.col("_r2"), F.lit("PRIMARY_KEY_DUPLICATE")),
                F.when(F.col("_r3"), F.lit("BOOKING_DATE_RANGE")),
                F.when(F.col("_r4"), F.lit("UNKNOWN_PASSENGER"))
            )
        )
        .drop("_r1", "_r2", "_r3", "_r4", "_known_passenger")
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
        CREATE TABLE IF NOT EXISTS {BOOKING_TABLE} (
            booking_key INT,
            passenger_key INT,
            booking_status STRING,
            total_price DECIMAL(12,2),
            booking_date DATE,
            flight_date DATE,
            booking_priority STRING,
            booking_agent STRING,
            booking_comment STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            booking_key STRING,
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

    bookings_out = valid_df.select(
        "booking_key",
        "passenger_key",
        "booking_status",
        "total_price",
        "booking_date",
        "flight_date",
        "booking_priority",
        "booking_agent",
        "booking_comment"
    )

    # NULL keys must stay joinable, so coalesce to a sentinel.
    quarantine_out = invalid_df.select(
        F.coalesce(
            F.col("booking_key_raw"), F.lit("<NULL>")
        ).alias("booking_key"),
        "rejection_reason",
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return bookings_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE bookings and remove stale invalid rows
# ─────────────────────────────────────────────
def merge_bookings(
    ranked_df: DataFrame,
    bookings_out: DataFrame
):
    """
    A booking key must be in Silver or fully rejected, never both.
    If no valid version of a key remains, remove its old Silver row.
    """

    ranked_df.select("booking_key").distinct().createOrReplaceTempView(
        "_processed_ids"
    )

    bookings_out.select("booking_key").distinct().createOrReplaceTempView(
        "_valid_booking_ids"
    )

    spark.sql(f"""
        MERGE INTO {BOOKING_TABLE} AS t
        USING (
            SELECT p.booking_key
            FROM _processed_ids p
            LEFT ANTI JOIN _valid_booking_ids v
              ON p.booking_key = v.booking_key
        ) AS s
        ON t.booking_key = s.booking_key
        WHEN MATCHED THEN DELETE
    """)

    bookings_out.createOrReplaceTempView("_bookings_source")

    spark.sql(f"""
        MERGE INTO {BOOKING_TABLE} AS t
        USING _bookings_source AS s
        ON t.booking_key = s.booking_key

        WHEN MATCHED AND (
            NOT (t.passenger_key <=> s.passenger_key)
            OR NOT (t.booking_status <=> s.booking_status)
            OR NOT (t.total_price <=> s.total_price)
            OR NOT (t.booking_date <=> s.booking_date)
            OR NOT (t.flight_date <=> s.flight_date)
            OR NOT (t.booking_priority <=> s.booking_priority)
            OR NOT (t.booking_agent <=> s.booking_agent)
            OR NOT (t.booking_comment <=> s.booking_comment)
        )
        THEN UPDATE SET
            t.passenger_key = s.passenger_key,
            t.booking_status = s.booking_status,
            t.total_price = s.total_price,
            t.booking_date = s.booking_date,
            t.flight_date = s.flight_date,
            t.booking_priority = s.booking_priority,
            t.booking_agent = s.booking_agent,
            t.booking_comment = s.booking_comment

        WHEN NOT MATCHED THEN INSERT (
            booking_key, passenger_key, booking_status, total_price,
            booking_date, flight_date, booking_priority,
            booking_agent, booking_comment
        )
        VALUES (
            s.booking_key, s.passenger_key, s.booking_status, s.total_price,
            s.booking_date, s.flight_date, s.booking_priority,
            s.booking_agent, s.booking_comment
        )
    """)


# ─────────────────────────────────────────────
# 7. MERGE quarantine and recover valid bookings
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
            F.col("booking_key_raw"), F.lit("<NULL>")
        ).alias("booking_key")
    ).distinct().createOrReplaceTempView("_processed_ids")

    quarantine_out.select("booking_key").distinct().createOrReplaceTempView(
        "_rejected_ids"
    )

    spark.sql(f"""
        DELETE FROM {QUARANTINE_TABLE}
        WHERE booking_key IN (
            SELECT p.booking_key
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.booking_key = r.booking_key
        )
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.raw_record = s.raw_record

        WHEN MATCHED AND (
            NOT (t.booking_key <=> s.booking_key)
            OR NOT (t.rejection_reason <=> s.rejection_reason)
        )
        THEN UPDATE SET
            t.booking_key = s.booking_key,
            t.rejection_reason = s.rejection_reason,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            booking_key, rejection_reason, raw_record, quarantined_at
        )
        VALUES (
            s.booking_key, s.rejection_reason, s.raw_record, s.quarantined_at
        )
    """)


# ─────────────────────────────────────────────
# 8. Main pipeline
# ─────────────────────────────────────────────
def main():

    # Read Bronze
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Standardize first
    standardized_df = standardize_bookings(bronze_df)

    # Rank versions per key (newest = 1)
    ranked_df = rank_booking_versions(standardized_df)

    # Read dependencies
    passengers_df = spark.read.table(PASSENGERS_TABLE)

    # Validate: newest version of each key survives,
    # older duplicates and null keys are rejected alongside
    # out-of-range booking dates and unknown passengers.
    valid_df, invalid_df = validate_bookings(ranked_df, passengers_df)

    # Prepare output DataFrames
    bookings_out, quarantine_out = prepare_outputs(
        valid_df, invalid_df
    )

    # Create tables once
    create_tables_if_not_exists()

    # Incremental target updates
    merge_bookings(ranked_df, bookings_out)
    merge_quarantine(ranked_df, quarantine_out)

    print("=" * 45)
    print("Bronze rows:", bronze_df.count())
    print("Valid bookings:", bookings_out.count())
    print("Rejected bookings:", quarantine_out.count())
    print("=" * 45)


if __name__ == "__main__":
    main()