#!/bin/bash

docker compose down
docker exec -it $(docker ps -qf name=postgres) psql -U jobtrends -d jobtrends -c "TRUNCATE jobs, trend_tech, trend_role;"
rm -rf _spark-checkpoints
docker compose up --build -d
