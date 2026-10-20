from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ─────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────
CATALOG = "sk_dev"

# Silver sources (single source of truth)
REGIONS_TABLE = f"{CATALOG}.silver.region"
NATIONS_TABLE = f"{CATALOG}.silver.nation"
AIRLINES_TABLE = f"{CATALOG}.silver.airline"
PASSENGERS_TABLE = f"{CATALOG}.silver.passenger"
ROUTES_TABLE = f"{CATALOG}.silver.route"
BOOKINGS_TABLE = f"{CATALOG}.silver.booking"
FLIGHT_BOOKINGS_TABLE = f"{CATALOG}.silver.flight_booking"

# Gold outputs
DIM_PASSENGER_TABLE = f"{CATALOG}.gold.dim_passenger"
DIM_ROUTE_TABLE = f"{CATALOG}.gold.dim_route"
DIM_AIRLINE_TABLE = f"{CATALOG}.gold.dim_airline"
DIM_GEOGRAPHY_TABLE = f"{CATALOG}.gold.dim_geography"
FACT_BOOKINGS_TABLE = f"{CATALOG}.gold.fact_bookings"
KPI_VIEW = f"{CATALOG}.gold.vw_annual_booking_kpis"

# Business definition of net revenue per booking line:
# ticket price after discount, multiplied by seats booked.
NET_REVENUE_EXPR = "ROUND(fb.ticket_price * (1 - fb.discount) * fb.seats, 2)"


# ─────────────────────────────────────────────
# 2. Create Gold schema if missing
# ─────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")


# ─────────────────────────────────────────────
# 3. Shared geography mapping
# One row per nation, including its parent region
# ─────────────────────────────────────────────
def get_geography_df() -> DataFrame:

    nations_df = spark.read.table(NATIONS_TABLE)
    regions_df = spark.read.table(REGIONS_TABLE)

    return (
        nations_df
        .join(regions_df, "region_key", "left")
        .select(
            F.col("nation_key").alias("nation_id"),
            F.col("nation_name"),
            F.col("region_key").alias("region_id"),
            F.col("region_name")
        )
    )


# ─────────────────────────────────────────────
# 4. gold.dim_passenger
# One row per passenger + passenger geography
# ─────────────────────────────────────────────
def create_dim_passenger():

    passengers_df = spark.read.table(PASSENGERS_TABLE)
    geography_df = get_geography_df()

    dim_passenger_df = (
        passengers_df
        .join(
            geography_df,
            passengers_df["nation_key"] == geography_df["nation_id"],
            "left"
        )
        .select(
            F.col("passenger_key").alias("passenger_id"),
            F.col("passenger_name"),
            F.col("passenger_address"),
            F.col("passenger_phone"),
            F.col("loyalty_points"),
            F.col("travel_purpose"),
            F.col("passenger_comment"),
            # Passenger geography
            F.col("nation_id").alias("passenger_nation_id"),
            F.col("nation_name").alias("passenger_nation"),
            F.col("region_id").alias("passenger_region_id"),
            F.col("region_name").alias("passenger_region")
        )
    )

    (
        dim_passenger_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_PASSENGER_TABLE)
    )

    print("Created:", DIM_PASSENGER_TABLE)


# ─────────────────────────────────────────────
# 5. gold.dim_route
# One row per route
# ─────────────────────────────────────────────
def create_dim_route():

    routes_df = spark.read.table(ROUTES_TABLE)

    dim_route_df = routes_df.select(
        F.col("route_key").alias("route_id"),
        F.col("route_name"),
        F.col("aircraft"),
        F.col("airline_brand"),
        F.col("route_type"),
        F.col("route_capacity").alias("capacity"),
        F.col("cabin_config"),
        F.col("base_price"),
        F.col("route_comment")
    )

    (
        dim_route_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_ROUTE_TABLE)
    )

    print("Created:", DIM_ROUTE_TABLE)


# ─────────────────────────────────────────────
# 6. gold.dim_airline
# One row per airline + airline geography
# (kept separate from passenger geography)
# ─────────────────────────────────────────────
def create_dim_airline():

    airlines_df = spark.read.table(AIRLINES_TABLE)
    geography_df = get_geography_df()

    dim_airline_df = (
        airlines_df
        .join(
            geography_df,
            airlines_df["nation_key"] == geography_df["nation_id"],
            "left"
        )
        .select(
            F.col("airline_key").alias("airline_id"),
            F.col("airline_name"),
            F.col("airline_address"),
            F.col("airline_phone"),
            F.col("airline_rating"),
            F.col("airline_comment"),
            # Airline geography
            F.col("nation_id").alias("airline_nation_id"),
            F.col("nation_name").alias("airline_nation"),
            F.col("region_id").alias("airline_region_id"),
            F.col("region_name").alias("airline_region")
        )
    )

    (
        dim_airline_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_AIRLINE_TABLE)
    )

    print("Created:", DIM_AIRLINE_TABLE)


# ─────────────────────────────────────────────
# 7. gold.dim_geography
# One row per nation, including its parent region
# ─────────────────────────────────────────────
def create_dim_geography():

    nations_df = spark.read.table(NATIONS_TABLE)
    regions_df = spark.read.table(REGIONS_TABLE)

    dim_geography_df = (
        nations_df
        .join(regions_df, "region_key", "left")
        .select(
            F.col("nation_key").alias("nation_id"),
            F.col("nation_name"),
            F.col("region_key").alias("region_id"),
            F.col("region_name"),
            F.col("region_comment")
        )
    )

    (
        dim_geography_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DIM_GEOGRAPHY_TABLE)
    )

    print("Created:", DIM_GEOGRAPHY_TABLE)


# ─────────────────────────────────────────────
# 8. gold.fact_bookings
# One row per flight-segment booking line.
# Keys, measures and attributes for SQL analysis,
# dashboards and Genie. Passenger geography and
# airline geography are kept as separate columns.
# ─────────────────────────────────────────────
def create_fact_bookings():

    flight_bookings_df = spark.read.table(FLIGHT_BOOKINGS_TABLE)
    bookings_df = spark.read.table(BOOKINGS_TABLE)
    routes_df = spark.read.table(ROUTES_TABLE)
    airlines_df = spark.read.table(AIRLINES_TABLE)
    passengers_df = spark.read.table(PASSENGERS_TABLE)

    geography_df = get_geography_df()

    # Passenger geography: passenger -> nation -> region
    passenger_geography_df = (
        passengers_df
        .join(
            geography_df,
            passengers_df["nation_key"] == geography_df["nation_id"],
            "left"
        )
        .select(
            F.col("passenger_key").alias("p_passenger_id"),
            F.col("nation_name").alias("passenger_nation"),
            F.col("region_name").alias("passenger_region")
        )
    )

    # Airline geography: airline -> nation -> region
    airline_geography_df = (
        airlines_df
        .join(
            geography_df,
            airlines_df["nation_key"] == geography_df["nation_id"],
            "left"
        )
        .select(
            F.col("airline_key").alias("a_airline_id"),
            F.col("nation_name").alias("airline_nation"),
            F.col("region_name").alias("airline_region")
        )
    )

    fact_bookings_df = (
        flight_bookings_df.alias("fb")
        .join(
            bookings_df.alias("b"),
            F.col("fb.booking_key") == F.col("b.booking_key"),
            "left"
        )
        .join(
            routes_df.alias("r"),
            F.col("fb.route_key") == F.col("r.route_key"),
            "left"
        )
        .join(
            airlines_df.alias("al"),
            F.col("fb.airline_key") == F.col("al.airline_key"),
            "left"
        )
        .join(
            passenger_geography_df.alias("pg"),
            F.col("b.passenger_key") == F.col("pg.p_passenger_id"),
            "left"
        )
        .join(
            airline_geography_df.alias("ag"),
            F.col("fb.airline_key") == F.col("ag.a_airline_id"),
            "left"
        )
        .select(
            # Keys
            F.col("fb.booking_key").alias("booking_id"),
            F.col("fb.line_number").alias("line_number"),
            F.col("b.passenger_key").alias("passenger_id"),
            F.col("fb.route_key").alias("route_id"),
            F.col("fb.airline_key").alias("airline_id"),
            # Booking attributes
            F.col("b.booking_date").alias("booking_date"),
            F.col("b.booking_status").alias("booking_status"),
            F.col("b.booking_priority").alias("booking_priority"),
            F.col("b.booking_agent").alias("booking_agent"),
            # Route / airline attributes
            F.col("r.route_name").alias("route_name"),
            F.col("r.route_type").alias("route_type"),
            F.col("al.airline_name").alias("airline_name"),
            F.col("fb.cabin_class").alias("cabin_class"),
            # Flight attributes
            F.col("fb.departure_date").alias("departure_date"),
            F.col("fb.arrival_date").alias("arrival_date"),
            F.col("fb.commit_date").alias("commit_date"),
            # Measures
            F.col("fb.seats").alias("seats"),
            F.col("fb.ticket_price").alias("ticket_price"),
            F.col("fb.discount").alias("discount"),
            F.col("fb.tax").alias("tax"),
            F.expr(NET_REVENUE_EXPR).alias("net_revenue"),
            # Passenger geography (who is flying)
            F.col("pg.passenger_nation").alias("passenger_nation"),
            F.col("pg.passenger_region").alias("passenger_region"),
            # Airline geography (who operates the flight)
            F.col("ag.airline_nation").alias("airline_nation"),
            F.col("ag.airline_region").alias("airline_region")
        )
    )

    (
        fact_bookings_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(FACT_BOOKINGS_TABLE)
    )

    print("Created:", FACT_BOOKINGS_TABLE)


# ─────────────────────────────────────────────
# 9. gold.vw_annual_booking_kpis
# Annual net revenue, distinct bookings,
# active passengers and average booking value
# ─────────────────────────────────────────────
def create_kpi_view():

    spark.sql(f"""
        CREATE OR REPLACE VIEW {KPI_VIEW} AS
        SELECT
            YEAR(booking_date)           AS booking_year,
            ROUND(SUM(net_revenue), 2)   AS annual_net_revenue,
            COUNT(DISTINCT booking_id)   AS distinct_bookings,
            COUNT(DISTINCT passenger_id) AS active_passengers,
            ROUND(
                SUM(net_revenue) / COUNT(DISTINCT booking_id), 2
            )                            AS average_booking_value
        FROM {FACT_BOOKINGS_TABLE}
        GROUP BY YEAR(booking_date)
    """)

    print("Created:", KPI_VIEW)


# ─────────────────────────────────────────────
# 10. Reconciliation report
# Gold vs independent Silver calculation
# ─────────────────────────────────────────────
def run_reconciliation():

    print("=" * 60)
    print("RECONCILIATION REPORT - Gold vs Silver")
    print("=" * 60)

    # ── Fact row count ──
    gold_count = spark.sql(f"""
        SELECT COUNT(*) AS row_count
        FROM {FACT_BOOKINGS_TABLE}
    """).first()["row_count"]

    silver_count = spark.sql(f"""
        SELECT COUNT(*) AS row_count
        FROM {FLIGHT_BOOKINGS_TABLE}
    """).first()["row_count"]

    row_diff = gold_count - silver_count
    row_status = "PASS" if row_diff == 0 else "FAIL"

    print(f"Fact row count | Gold: {gold_count} | "
          f"Silver: {silver_count} | Diff: {row_diff} | {row_status}")

    # ── Net revenue total (independent Silver calculation) ──
    # Same measure definition as the fact table: rounded per line,
    # then summed - computed here directly from Silver tables.
    gold_revenue = spark.sql(f"""
        SELECT ROUND(SUM(net_revenue), 2) AS net_revenue
        FROM {FACT_BOOKINGS_TABLE}
    """).first()["net_revenue"]

    silver_revenue = spark.sql(f"""
        SELECT SUM(
                   ROUND(
                       fb.ticket_price * (1 - fb.discount) * fb.seats, 2
                   )
               ) AS net_revenue
        FROM {FLIGHT_BOOKINGS_TABLE} fb
    """).first()["net_revenue"]

    revenue_diff = round(
        float(gold_revenue or 0) - float(silver_revenue or 0), 2
    )
    revenue_status = "PASS" if revenue_diff == 0 else "FAIL"

    print(f"Net revenue | Gold: {gold_revenue} | "
          f"Silver: {silver_revenue} | Diff: {revenue_diff} | "
          f"{revenue_status}")

    # ── Dimension key uniqueness ──
    dimension_checks = [
        (DIM_PASSENGER_TABLE, "passenger_id"),
        (DIM_ROUTE_TABLE, "route_id"),
        (DIM_AIRLINE_TABLE, "airline_id"),
        (DIM_GEOGRAPHY_TABLE, "nation_id")
    ]

    for table, key in dimension_checks:
        stats = spark.sql(f"""
            SELECT
                COUNT(*)              AS total_rows,
                COUNT(DISTINCT {key}) AS distinct_keys
            FROM {table}
        """).first()

        is_unique = (
            stats["total_rows"] == stats["distinct_keys"]
        )
        status = "PASS" if is_unique else "FAIL"

        print(f"{table.split('.')[-1]:<16} | rows: "
              f"{stats['total_rows']} | distinct {key}: "
              f"{stats['distinct_keys']} | {status}")

    print("=" * 60)


# ─────────────────────────────────────────────
# 11. Main execution
# ─────────────────────────────────────────────
def main():

    create_dim_passenger()
    create_dim_route()
    create_dim_airline()
    create_dim_geography()
    create_fact_bookings()
    create_kpi_view()

    run_reconciliation()

    print("=" * 45)
    print("All Gold tables and KPI view created.")
    print("=" * 45)


if __name__ == "__main__":
    main()