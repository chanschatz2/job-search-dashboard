from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp, lower, coalesce, lit
from schemas import event_schema
from sinks import write_to_postgres
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
    
    # Postgres
    events.writeStream.foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/checkpoints/jobs") \
        .start().awaitTermination()

if __name__ == "__main__":
    main()

"""
docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select count(*) from jobs;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select event_id, company, title from jobs limit 5;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select * from jobs limit 5;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select event_id, company, techs  from jobs ORDER BY techs limit 5;"

"""