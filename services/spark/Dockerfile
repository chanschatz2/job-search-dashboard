FROM apache/spark:3.5.1

USER root

# Python deps for PySpark app later
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10

# (Optional) add Postgres JDBC driver now to avoid wasting Day 2
# Spark often needs the JDBC jar available for writing to Postgres.
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

USER spark
WORKDIR /opt/spark-app