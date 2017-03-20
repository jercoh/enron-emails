#!/usr/bin/env bash
mkdir resources
wget "http://bailando.sims.berkeley.edu/enron/enron_with_categories.tar.gz" -O ./resources/enron_with_categories.tar.gz
gunzip ./resources/enron_with_categories.tar.gz
java -jar tar-to-seq/tar-to-seq.jar ./resources/enron_with_categories.tar ./resources/enron_mails.seq