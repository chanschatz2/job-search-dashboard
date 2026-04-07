import os
from pyspark.sql.functions import lit, min as spark_min, max as spark_max
import psycopg2

user = os.getenv("POSTGRES_USER", "jobtrends")
pwd = os.getenv("POSTGRES_PASSWORD", "jobtrends")
host = os.getenv("POSTGRES_HOST", "postgres")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "jobtrends")

# PostgreSQL connection properties
jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

connection_properties = {
    "user": user,
    "password": pwd,
    "driver": "org.postgresql.Driver"
}
pg_table = "jobs"

# Function passed to foreachBatch to write df to Postgres
# batch_df -> micro-batch of rows from stream
def write_to_postgres(batch_df, batch_id):
    # batch_id can be used for logging or ensuring idempotency
    print(f"Writing batch: {batch_id} to PostgreSQL...")

    # Create df matching jobs table schema
    jobs_df = batch_df.select(
        "event_id",
        "ingested_at",
        "company",
        "title",
        "location",
        lit(None).cast("string").alias("seniority"), # not derived yet so None
        "role_category",
        "techs",
        "description",
        "url",
    )

    jobs_df.write \
        .format("jdbc") \
        .mode("append") \
        .options(
            url=jdbc_url,
            dbtable=pg_table,
            user=connection_properties["user"],
            password=connection_properties["password"],
            driver=connection_properties["driver"]
        ) \
        .save()
    
# Helper for aggregate functions
# Deletes old rows & avoids duplicates for given table and time range before rewriting them
def _delete_window_range(table: str, window_size_sec: int, min_start, max_start):
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd
    )

    # Delete rows that already exist for this window time-frame
    # prevents duplicate primary key
    try:
        with conn: # transaction
            with conn.cursor() as cur:
                # Delete 
                cur.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE window_size_sec = %s
                      AND window_start >= %s
                      AND window_start <= %s
                    """,
                    (window_size_sec, min_start, max_start)
                )
    finally:
        conn.close()

# Write window for tech from this stream batch, passed to foreachBatch
def write_trend_tech(batch_df, batch_id):
    print(f"Writing trend_tech batch: {batch_id} ...")

    if len(batch_df.head(1)) == 0:
        return

    # Finds the earliest and latest window_start values in this batch
    # used to know what time range to delete before inserting
    bounds = batch_df.select(
        spark_min("window_start").alias("min_start"),
        spark_max("window_start").alias("max_start")
    ).collect()[0] # result is 1 row

    # Call helper to delete duplicate window (if exists)
    _delete_window_range(
        table="trend_tech",
        window_size_sec=300, # 5 minutes
        min_start=bounds["min_start"],
        max_start=bounds["max_start"]
    )

    # Write window to db
    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .options(
            url=jdbc_url,
            dbtable="trend_tech",
            user=connection_properties["user"],
            password=connection_properties["password"],
            driver=connection_properties["driver"]
        ) \
        .save()

# Write window for roles from this stream batch, passed to foreachBatch
def write_trend_role(batch_df, batch_id):
    print(f"Writing trend_role batch: {batch_id} ...")

    if len(batch_df.head(1)) == 0:
        return

    bounds = batch_df.select(
        spark_min("window_start").alias("min_start"),
        spark_max("window_start").alias("max_start")
    ).collect()[0]

    _delete_window_range(
        table="trend_role",
        window_size_sec=300,
        min_start=bounds["min_start"],
        max_start=bounds["max_start"]
    )

    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .options(
            url=jdbc_url,
            dbtable="trend_role",
            user=connection_properties["user"],
            password=connection_properties["password"],
            driver=connection_properties["driver"]
        ) \
        .save()