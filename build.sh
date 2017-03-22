#!/usr/bin/env bash
mkdir resources
wget "http://bailando.sims.berkeley.edu/enron/enron_with_categories.tar.gz" -O ./resources/enron_with_categories.tar.gz
gunzip ./resources/enron_with_categories.tar.gz
java -jar tar-to-seq/tar-to-seq.jar ./resources/enron_with_categories.tar ./resources/enron_mails.seq
docker-compose down
docker-compose build
docker-compose up -d
docker-compose scale spark-worker=2
docker exec -it spark-master python app/dataIO/prepare_data.py
