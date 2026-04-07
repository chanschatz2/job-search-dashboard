from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Posting schema must match JSON shape 
posting_schema = StructType([
    StructField("company", StringType(), True),
    StructField("title", StringType(), True),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True),
    StructField("url", StringType(), True),
    #StructField("techs", ArrayType(StringType()), True),
    #StructField("seniority", StringType(), True),
    #StructField("role_category", StringType(), True),
])

event_schema = StructType([
    StructField("event_id", StringType(), False), # nullable = false i.e. not optional
    StructField("ingested_at", StringType(), False),
    StructField("source", StringType(), True),
    StructField("posting", posting_schema, False),
])