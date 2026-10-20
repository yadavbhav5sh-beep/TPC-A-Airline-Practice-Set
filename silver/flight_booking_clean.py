from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

BRONZE_TABLE = f"{CATALOG}.bronze.flight_booking"
BOOKINGS_TABLE = f"{CATALOG}.silver.booking"
ROUTES_TABLE = f"{CATALOG}.silver.route"
AIRLINES_TABLE = f"{CATALOG}.silver.airline"

FLIGHT_BOOKING_TABLE = f"{CATALOG}.silver.flight_booking"
QUARANTINE_TABLE = f"{CATALOG}.silver.flight_booking_quarantine"

# Business rules:
# - ticket price must be greater than 0
# - discount must be within the closed interval [0, 1]
# - seats must be 1 or more
# - departure date must be within 1994-01-01 .. 1998-12-31 (inclusive)
TICKET_PRICE_MIN_EXCLUSIVE = 0
DISCOUNT_MIN = 0
DISCOUNT_MAX = 1
SEATS_MIN = 1
DEPARTURE_DATE_MIN = F.lit("1994-01-01").cast("date")
DEPARTURE_DATE_MAX = F.lit("1998-12-31").cast("date")


# ─────────────────────────────────────────────
# 1. Standardize flight bookings (S1-S17)
# ─────────────────────────────────────────────
def standardize_flight_bookings(df: DataFrame) -> DataFrame:
    """
    Standardization happens before deduplication and validation.

    S1:  booking key trim; empty -> NULL
    S2:  booking key parsed strictly to INT (non-numeric -> NULL)
    S3:  line number trim; empty -> NULL
    S4:  line number parsed strictly to INT (non-numeric -> NULL)
    S5:  route key trim; empty -> NULL
    S6:  route key parsed strictly to INT (non-numeric -> NULL)
    S7:  airline key trim; empty -> NULL
    S8:  airline key parsed strictly to INT (non-numeric -> NULL)
    S9:  seats parsed strictly to INT (non-numeric -> NULL)
    S10: ticket price parsed strictly to DECIMAL (non-numeric -> NULL)
    S11: discount parsed strictly to DECIMAL (non-numeric -> NULL)
    S12: tax parsed strictly to DECIMAL (non-numeric -> NULL)
    S13: refund flag trim + uppercase; empty -> NULL
    S14: segment status trim + uppercase; empty -> NULL
    S15: departure / commit / arrival dates parsed strictly
         to DATE (invalid -> NULL)
    S16: cabin class trim + uppercase; empty -> NULL
    S17: comment trim; empty -> NULL
    """

    # Preserve the complete original Bronze row as JSON.
    df = df.withColumn(
        "raw_record",
        F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )

    # S1: keep the raw text keys for quarantine traceability.
    df = df.withColumn(
        "booking_key_raw",
        F.when(
            F.trim(F.col("fb_bookingid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("fb_bookingid").cast("string"))
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

    # S3
    df = df.withColumn(
        "line_number_raw",
        F.when(
            F.trim(F.col("fb_linenumber").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("fb_linenumber").cast("string"))
        )
    )

    # S4
    df = df.withColumn(
        "line_number",
        F.when(
            F.col("line_number_raw").rlike("^-?\\d+$"),
            F.col("line_number_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S5
    df = df.withColumn(
        "route_key_raw",
        F.when(
            F.trim(F.col("fb_routeid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("fb_routeid").cast("string"))
        )
    )

    # S6
    df = df.withColumn(
        "route_key",
        F.when(
            F.col("route_key_raw").rlike("^-?\\d+$"),
            F.col("route_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S7
    df = df.withColumn(
        "airline_key_raw",
        F.when(
            F.trim(F.col("fb_airlineid").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("fb_airlineid").cast("string"))
        )
    )

    # S8
    df = df.withColumn(
        "airline_key",
        F.when(
            F.col("airline_key_raw").rlike("^-?\\d+$"),
            F.col("airline_key_raw").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S9: strict integer parse - anything else becomes NULL
    # so the seats rule (R5) can reject it.
    df = df.withColumn(
        "seats",
        F.when(
            F.trim(F.col("fb_seats").cast("string"))
                .rlike("^-?\\d+$"),
            F.trim(F.col("fb_seats").cast("string"))
                .cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )

    # S10: strict numeric parse - anything else becomes NULL
    # so the ticket price rule (R3) can reject it.
    df = df.withColumn(
        "ticket_price",
        F.when(
            F.trim(F.col("fb_ticketprice").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("fb_ticketprice").cast("string"))
                .cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)"))
    )

    # S11: strict numeric parse - anything else becomes NULL
    # so the discount rule (R4) can reject it.
    df = df.withColumn(
        "discount",
        F.when(
            F.trim(F.col("fb_discount").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("fb_discount").cast("string"))
                .cast("decimal(6,5)")
        ).otherwise(F.lit(None).cast("decimal(6,5)"))
    )

    # S12
    df = df.withColumn(
        "tax",
        F.when(
            F.trim(F.col("fb_tax").cast("string"))
                .rlike("^-?\\d+(\\.\\d+)?$"),
            F.trim(F.col("fb_tax").cast("string"))
                .cast("decimal(6,5)")
        ).otherwise(F.lit(None).cast("decimal(6,5)"))
    )

    # S13
    df = df.withColumn(
        "refund_flag",
        F.when(
            F.trim(F.col("fb_refundflag").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("fb_refundflag").cast("string")))
        )
    )

    # S14
    df = df.withColumn(
        "segment_status",
        F.when(
            F.trim(F.col("fb_segmentstatus").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("fb_segmentstatus").cast("string")))
        )
    )

    # S15: strict date parse - invalid dates become NULL so
    # the date rules (R6-R8) can reject them.
    df = df.withColumn(
        "departure_date",
        F.expr("try_to_date(trim(fb_departuredate))")
    )

    df = df.withColumn(
        "commit_date",
        F.expr("try_to_date(trim(fb_commitdate))")
    )

    df = df.withColumn(
        "arrival_date",
        F.expr("try_to_date(trim(fb_arrivaldate))")
    )

    # S16
    df = df.withColumn(
        "cabin_class",
        F.when(
            F.trim(F.col("fb_cabinclass").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("fb_cabinclass").cast("string")))
        )
    )

    # S17
    df = df.withColumn(
        "flight_booking_comment",
        F.when(
            F.trim(F.col("fb_comment").cast("string")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("fb_comment").cast("string"))
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
def rank_flight_booking_versions(df: DataFrame) -> DataFrame:
    """
    Rank every version of each composite key (booking + line
    number) by _ingestion_at (newest = 1).

    Duplicates are NOT silently dropped: the newest version
    survives in Silver, every older version of a duplicated
    composite key is quarantined (rule R2).
    """

    window_spec = (
        Window
        .partitionBy("booking_key_raw", "line_number_raw")
        .orderBy(F.col("_ingestion_at").desc_nulls_last())
    )

    return df.withColumn("_rn", F.row_number().over(window_spec))


# ─────────────────────────────────────────────
# 3. Data quality rules (R1-R10)
# ─────────────────────────────────────────────
def validate_flight_bookings(
    df: DataFrame,
    bookings_df: DataFrame,
    routes_df: DataFrame,
    airlines_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject a record if ANY rule fails. Every failed rule is
    collected into rejection_reason as a comma-separated list.

    R1  PRIMARY_KEY_NULL          booking key or line number missing/blank
    R2  PRIMARY_KEY_DUPLICATE     older duplicate version of the composite
                                 key (the newest version survives)
    R3  TICKET_PRICE_RANGE        ticket price missing, zero or negative
    R4  DISCOUNT_RANGE            discount outside the closed [0, 1]
    R5  SEATS_RANGE               seats missing, zero or negative
    R6  DEPARTURE_DATE_RANGE      departure date missing/unparseable, or
                                 outside 1994-01-01 .. 1998-12-31 (inclusive)
    R7  DEPARTURE_BEFORE_BOOKING departure date is before the booking date
                                 in silver.booking
    R8  ARRIVAL_BEFORE_DEPARTURE arrival date missing, or before the
                                 departure date (equal dates are valid)
    R9  UNKNOWN_BOOKING          booking key not found in silver.booking
    R10 UNKNOWN_ROUTE            route key not found in silver.route
    R11 UNKNOWN_AIRLINE          airline key not found in silver.airline
    """

    # Booking key + booking date from the cleaned Silver table.
    known_bookings = (
        bookings_df
        .select(
            F.col("booking_key").alias("_booking_key"),
            F.col("booking_date").alias("_booking_date")
        )
        .filter(F.col("_booking_key").isNotNull())
    )

    # Valid route references from the cleaned Silver table.
    known_routes = (
        routes_df
        .select(F.col("route_key").alias("_known_route"))
        .filter(F.col("_known_route").isNotNull())
        .distinct()
    )

    # Valid airline references from the cleaned Silver table.
    known_airlines = (
        airlines_df
        .select(F.col("airline_key").alias("_known_airline"))
        .filter(F.col("_known_airline").isNotNull())
        .distinct()
    )

    checked = (
        df
        # R1: either part of the composite key missing.
        .withColumn(
            "_r1",
            F.col("booking_key_raw").isNull()
            | F.col("line_number_raw").isNull()
        )
        # R2: older versions of a fully present composite key.
        # The newest version (_rn = 1) stays valid.
        .withColumn(
            "_r2",
            F.col("booking_key_raw").isNotNull()
            & F.col("line_number_raw").isNotNull()
            & (F.col("_rn") > 1)
        )
        # R3: numeric range - ticket price must be > 0.
        .withColumn(
            "_r3",
            F.col("ticket_price").isNull()
            | (F.col("ticket_price")
               <= F.lit(TICKET_PRICE_MIN_EXCLUSIVE))
        )
        # R4: numeric range - discount must be within
        # the closed interval [0, 1].
        .withColumn(
            "_r4",
            F.col("discount").isNull()
            | (F.col("discount") < F.lit(DISCOUNT_MIN))
            | (F.col("discount") > F.lit(DISCOUNT_MAX))
        )
        # R5: numeric range - seats must be 1 or more.
        .withColumn(
            "_r5",
            F.col("seats").isNull()
            | (F.col("seats") < F.lit(SEATS_MIN))
        )
        # R6: date range - departure date must be within
        # [1994-01-01, 1998-12-31], both inclusive.
        .withColumn(
            "_r6",
            F.col("departure_date").isNull()
            | (F.col("departure_date") < DEPARTURE_DATE_MIN)
            | (F.col("departure_date") > DEPARTURE_DATE_MAX)
        )
        .join(
            known_bookings,
            F.col("booking_key") == F.col("_booking_key"),
            "left"
        )
        # R7: departure must not be before the booking date.
        .withColumn(
            "_r7",
            F.col("departure_date").isNotNull()
            & F.col("_booking_date").isNotNull()
            & (F.col("departure_date") < F.col("_booking_date"))
        )
        # R8: arrival must not be before departure.
        # Same-day flights (arrival == departure) are valid.
        .withColumn(
            "_r8",
            F.col("arrival_date").isNull()
            | (
                F.col("departure_date").isNotNull()
                & (F.col("arrival_date") < F.col("departure_date"))
            )
        )
        # R9: referential integrity - booking key must exist
        # in silver.booking.
        .withColumn(
            "_r9",
            F.col("booking_key_raw").isNull()
            | F.col("_booking_key").isNull()
        )
        .join(
            known_routes,
            F.col("route_key") == F.col("_known_route"),
            "left"
        )
        # R10: referential integrity - route key must exist
        # in silver.route.
        .withColumn(
            "_r10",
            F.col("route_key_raw").isNull()
            | F.col("_known_route").isNull()
        )
        .join(
            known_airlines,
            F.col("airline_key") == F.col("_known_airline"),
            "left"
        )
        # R11: referential integrity - airline key must exist
        # in silver.airline.
        .withColumn(
            "_r11",
            F.col("airline_key_raw").isNull()
            | F.col("_known_airline").isNull()
        )
        # Comma-separated list of EVERY failed rule for this record.
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ",",
                F.when(F.col("_r1"), F.lit("PRIMARY_KEY_NULL")),
                F.when(F.col("_r2"), F.lit("PRIMARY_KEY_DUPLICATE")),
                F.when(F.col("_r3"), F.lit("TICKET_PRICE_RANGE")),
                F.when(F.col("_r4"), F.lit("DISCOUNT_RANGE")),
                F.when(F.col("_r5"), F.lit("SEATS_RANGE")),
                F.when(F.col("_r6"), F.lit("DEPARTURE_DATE_RANGE")),
                F.when(F.col("_r7"), F.lit("DEPARTURE_BEFORE_BOOKING")),
                F.when(F.col("_r8"), F.lit("ARRIVAL_BEFORE_DEPARTURE")),
                F.when(F.col("_r9"), F.lit("UNKNOWN_BOOKING")),
                F.when(F.col("_r10"), F.lit("UNKNOWN_ROUTE")),
                F.when(F.col("_r11"), F.lit("UNKNOWN_AIRLINE"))
            )
        )
        .drop("_r1", "_r2", "_r3", "_r4", "_r5", "_r6", "_r7",
              "_r8", "_r9", "_r10", "_r11",
              "_booking_key", "_booking_date", "_known_route",
              "_known_airline")
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
        CREATE TABLE IF NOT EXISTS {FLIGHT_BOOKING_TABLE} (
            booking_key INT,
            line_number INT,
            route_key INT,
            airline_key INT,
            seats INT,
            ticket_price DECIMAL(12,2),
            discount DECIMAL(6,5),
            tax DECIMAL(6,5),
            refund_flag STRING,
            segment_status STRING,
            departure_date DATE,
            commit_date DATE,
            arrival_date DATE,
            cabin_class STRING,
            flight_booking_comment STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            booking_key STRING,
            line_number STRING,
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

    flight_bookings_out = valid_df.select(
        "booking_key",
        "line_number",
        "route_key",
        "airline_key",
        "seats",
        "ticket_price",
        "discount",
        "tax",
        "refund_flag",
        "segment_status",
        "departure_date",
        "commit_date",
        "arrival_date",
        "cabin_class",
        "flight_booking_comment"
    )

    # NULL keys must stay joinable, so coalesce to a sentinel.
    quarantine_out = invalid_df.select(
        F.coalesce(
            F.col("booking_key_raw"), F.lit("<NULL>")
        ).alias("booking_key"),
        F.coalesce(
            F.col("line_number_raw"), F.lit("<NULL>")
        ).alias("line_number"),
        "rejection_reason",
        "raw_record",
        F.current_timestamp().alias("quarantined_at")
    )

    return flight_bookings_out, quarantine_out


# ─────────────────────────────────────────────
# 6. MERGE flight bookings and remove stale rows
# ─────────────────────────────────────────────
def merge_flight_bookings(
    ranked_df: DataFrame,
    flight_bookings_out: DataFrame
):
    """
    A composite key must be in Silver or fully rejected,
    never both. If no valid version of a key remains,
    remove its old Silver row.
    """

    ranked_df.select("booking_key", "line_number").distinct() \
        .createOrReplaceTempView("_processed_ids")

    flight_bookings_out.select("booking_key", "line_number").distinct() \
        .createOrReplaceTempView("_valid_ids")

    spark.sql(f"""
        MERGE INTO {FLIGHT_BOOKING_TABLE} AS t
        USING (
            SELECT p.booking_key, p.line_number
            FROM _processed_ids p
            LEFT ANTI JOIN _valid_ids v
              ON p.booking_key = v.booking_key
             AND p.line_number = v.line_number
        ) AS s
        ON t.booking_key = s.booking_key
       AND t.line_number = s.line_number
        WHEN MATCHED THEN DELETE
    """)

    flight_bookings_out.createOrReplaceTempView("_flight_bookings_source")

    spark.sql(f"""
        MERGE INTO {FLIGHT_BOOKING_TABLE} AS t
        USING _flight_bookings_source AS s
        ON t.booking_key = s.booking_key
       AND t.line_number = s.line_number

        WHEN MATCHED AND (
            NOT (t.route_key <=> s.route_key)
            OR NOT (t.airline_key <=> s.airline_key)
            OR NOT (t.seats <=> s.seats)
            OR NOT (t.ticket_price <=> s.ticket_price)
            OR NOT (t.discount <=> s.discount)
            OR NOT (t.tax <=> s.tax)
            OR NOT (t.refund_flag <=> s.refund_flag)
            OR NOT (t.segment_status <=> s.segment_status)
            OR NOT (t.departure_date <=> s.departure_date)
            OR NOT (t.commit_date <=> s.commit_date)
            OR NOT (t.arrival_date <=> s.arrival_date)
            OR NOT (t.cabin_class <=> s.cabin_class)
            OR NOT (t.flight_booking_comment <=> s.flight_booking_comment)
        )
        THEN UPDATE SET
            t.route_key = s.route_key,
            t.airline_key = s.airline_key,
            t.seats = s.seats,
            t.ticket_price = s.ticket_price,
            t.discount = s.discount,
            t.tax = s.tax,
            t.refund_flag = s.refund_flag,
            t.segment_status = s.segment_status,
            t.departure_date = s.departure_date,
            t.commit_date = s.commit_date,
            t.arrival_date = s.arrival_date,
            t.cabin_class = s.cabin_class,
            t.flight_booking_comment = s.flight_booking_comment

        WHEN NOT MATCHED THEN INSERT (
            booking_key, line_number, route_key, airline_key,
            seats, ticket_price, discount, tax, refund_flag,
            segment_status, departure_date, commit_date,
            arrival_date, cabin_class, flight_booking_comment
        )
        VALUES (
            s.booking_key, s.line_number, s.route_key, s.airline_key,
            s.seats, s.ticket_price, s.discount, s.tax, s.refund_flag,
            s.segment_status, s.departure_date, s.commit_date,
            s.arrival_date, s.cabin_class, s.flight_booking_comment
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
            F.col("booking_key_raw"), F.lit("<NULL>")
        ).alias("booking_key"),
        F.coalesce(
            F.col("line_number_raw"), F.lit("<NULL>")
        ).alias("line_number")
    ).distinct().createOrReplaceTempView("_processed_ids")

    quarantine_out.select("booking_key", "line_number").distinct() \
        .createOrReplaceTempView("_rejected_ids")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING (
            SELECT p.booking_key, p.line_number
            FROM _processed_ids p
            LEFT ANTI JOIN _rejected_ids r
              ON p.booking_key = r.booking_key
             AND p.line_number = r.line_number
        ) AS s
        ON t.booking_key = s.booking_key
       AND t.line_number = s.line_number
        WHEN MATCHED THEN DELETE
    """)

    quarantine_out.createOrReplaceTempView("_quarantine_source")

    spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS t
        USING _quarantine_source AS s
        ON t.raw_record = s.raw_record

        WHEN MATCHED AND (
            NOT (t.booking_key <=> s.booking_key)
            OR NOT (t.line_number <=> s.line_number)
            OR NOT (t.rejection_reason <=> s.rejection_reason)
        )
        THEN UPDATE SET
            t.booking_key = s.booking_key,
            t.line_number = s.line_number,
            t.rejection_reason = s.rejection_reason,
            t.quarantined_at = s.quarantined_at

        WHEN NOT MATCHED THEN INSERT (
            booking_key, line_number, rejection_reason,
            raw_record, quarantined_at
        )
        VALUES (
            s.booking_key, s.line_number, s.rejection_reason,
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
    standardized_df = standardize_flight_bookings(bronze_df)

    # Rank versions per composite key (newest = 1)
    ranked_df = rank_flight_booking_versions(standardized_df)

    # Read dependencies
    bookings_df = spark.read.table(BOOKINGS_TABLE)
    routes_df = spark.read.table(ROUTES_TABLE)
    airlines_df = spark.read.table(AIRLINES_TABLE)

    # Validate: newest version of each composite key survives,
    # older duplicates and null keys are rejected alongside
    # numeric/date/range and referential integrity failures.
    valid_df, invalid_df = validate_flight_bookings(
        ranked_df, bookings_df, routes_df, airlines_df
    )

    # Prepare output DataFrames
    flight_bookings_out, quarantine_out = prepare_outputs(
        valid_df, invalid_df
    )

    # Create tables once
    create_tables_if_not_exists()

    # Incremental target updates
    merge_flight_bookings(ranked_df, flight_bookings_out)
    merge_quarantine(ranked_df, quarantine_out)

    print("=" * 45)
    print("Bronze rows:", bronze_df.count())
    print("Valid flight bookings:", flight_bookings_out.count())
    print("Rejected flight bookings:", quarantine_out.count())
    print("=" * 45)


if __name__ == "__main__":
    main()