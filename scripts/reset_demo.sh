#!/usr/bin/env bash
set -e

docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c \
"TRUNCATE TABLE trend_tech, trend_role, jobs RESTART IDENTITY CASCADE;"

rm -rf _spark_checkpoints/*

docker compose restart spark