from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from schemas import event_schema
from sinks import write_to_postgres

def init_spark(name="JobKafkaPostgres") -> SparkSession:
    spark = SparkSession.builder.appName(name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "job_postings_raw") \
        .load()
    
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
    
    return flat

def main():
    spark = init_spark(name="JobKafkaPostgres")

    raw = read_kafka_stream(spark)
    events = parse_events(raw)

    #events.writeStream.format("console").start().awaitTermination()

    # Postgres
    events.writeStream.foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/checkpoints/jobs") \
        .start().awaitTermination()

if __name__ == "__main__":
    main()

"""
docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select count(*) from jobs;"

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "select event_id, company, title from jobs limit 5;"

"""