#!/usr/bin/env bash
docker-compose up -d spark-master
docker-compose scale spark-worker=2
docker exec -it spark-master python app/main.py