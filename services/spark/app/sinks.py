import os
from pyspark.sql.functions import lit

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
def write_to_postgres(batch_df, batch_id):
    # batch_id can be used for logging or ensuring idempotency
    print(f"Writing batch: {batch_id} to PostgreSQL...")

    jobs_df = batch_df.select(
        "event_id",
        "ingested_at",
        "company",
        "title",
        "location",
        lit(None).cast("string").alias("seniority"),
        "role_category",
        "techs",
        "description",
        "url",
    )
    
    #print("COLUMNS:", batch_df.columns)
    #batch_df.printSchema()

    #batch_df = batch_df.drop("source") # DROPPING source col for now (not in schema)

    #if batch_df.rdd.isEmpty():
    #    return

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