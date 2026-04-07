from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp, lower, coalesce, lit, window, explode
from schemas import event_schema
from sinks import write_to_postgres, write_trend_role, write_trend_tech
from transform import add_role_category, add_techs


def init_spark(name="JobKafkaPostgres") -> SparkSession:
    spark = SparkSession.builder.appName(name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "job_postings_raw") \
        .option("failOnDataLoss", "false") \
        .load()
        # NOTE: failOnDataLoss skips ahead of offset is missing / out of range, for testing only
    
    return df

def parse_events(df: DataFrame):
    # input kafka spark df has columns:
        # key, value (binary), topic, partition, offset, timestamp, etc
        # value -> structured JSON

    # Create new column "value" type string
    value_str = col("value").cast("string")

    # Parses column with JSON string using schema
    parsed = df.select(from_json(value_str, event_schema).alias("e"))

    # Flatten event fields + posting fields
    flat = parsed.select(
        col("e.event_id").alias("event_id"),
        to_timestamp(col("e.ingested_at")).alias("ingested_at"),
        #col("e.source").alias("source"), # DROPPING source UNTIL ADD TO SCHEMA
        col("e.posting.*"),  # expands posting_schema members
    )
    
    # Helper columns for .contains() later, drop before writing to jobs
    flat = flat.withColumn("title_l", lower(coalesce(col("title"), lit("")))) \
               .withColumn("desc_l", lower(coalesce(col("description"), lit(""))))
                # coalesce() picks non-null from either the col() or a col of just ""
    return flat

def main():
    spark = init_spark(name="JobKafkaPostgres")

    # Read raw kafka event stream
    raw = read_kafka_stream(spark)
    events = parse_events(raw)

    # Add columns of technologies + roles that are found in posting
    events = add_role_category(events)
    events = add_techs(events)

    #print(events.columns)
    #events.printSchema()

    # Write batches to Postgres

    # write all jobs to postgres sink
    jobs_query = events.writeStream.foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/checkpoints/jobs") \
        .start()
    
    # trend_tech explode tech array, then aggregate by 5-minute window
    tech_df = events.withColumn("tech", explode("techs")) \
        .groupBy(
            window(col("ingested_at"), "5 minutes"),
            col("tech")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit(300).alias("window_size_sec"),
            col("tech"),
            col("count")
        )
    
    # sink tech trends
    tech_query = tech_df.writeStream.foreachBatch(write_trend_tech) \
        .outputMode("update") \
        .option("checkpointLocation", "/checkpoints/trend_tech") \
        .start()

    # trend_role - aggregate by 5-minute window
    role_df = events.groupBy(
            window(col("ingested_at"), "5 minutes"),
            col("role_category")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit(300).alias("window_size_sec"),
            col("role_category"),
            col("count")
        )
    
    role_query = role_df.writeStream.foreachBatch(write_trend_role) \
        .outputMode("update") \
        .option("checkpointLocation", "/checkpoints/trend_role") \
        .start()
    
    jobs_query.awaitTermination()
    tech_query.awaitTermination()
    role_query.awaitTermination()


if __name__ == "__main__":
    main()

"""
docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select count(*) from jobs;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select event_id, company, title from jobs limit 5;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select * from jobs limit 5;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select event_id, company, techs  from jobs ORDER BY techs limit 5;"

"""