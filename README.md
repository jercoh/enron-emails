# Enron emails analysis using Spark


## Overview

We’d like to answer the following questions about the email messages provided in the sample from Enron emails:

- __Recipients__: Which 3 recipients received the largest number of direct emails (emails that have exactly one recipient), and how many did each receive?
- __Senders__: Which 3 senders sent the largest number of broadcast emails (emails that have multiple recipients, including CCs and BCCs), and how many did each send?
- __Response times__: Find the 5 emails with the fastest response times. Please include file IDs, subject, sender, recipient, and response time. (For our purposes, a response is defined as a message from one of the recipients to the original sender whose subject line contains the subject of the original email as a substring, and the response time should be measured as the difference between when the original email was sent and when the response was sent.)


## Results
Results for the emails sample `enron_with_categories.tar.gz` can be found in [results.txt](results.txt)

To get a sense of the robustness of my code, I launched a Spark cluster on Google Cloud DataProc.

I stored the full Enron email corpus (+500K emails) on Google Cloud Storage HDFS and started a simple flask server (flask script here [app/server/flask_app.py](app/server/flask_app.py)) that executes the SQL queries.
The Spark cluster has the following configuration:

- 1 Master node (2CPUs - 7.5GB Memory) + 7 Workers nodes (4 CPUs - 15 GB Memory)

You can run the queries for the full Enron corpus by accessing these urls:

- http://35.185.42.227/direct-emails
- http://35.185.42.227/broadcast-emails
- http://35.185.42.227/response-times

## Local Run Instructions

### Dependencies
You need to have java, docker and docker-compose installed on your local machine.

I used [tar-to-seq](https://stuartsierra.com/2008/04/24/a-million-little-files), a third party library, in order to combine the email text files into one hadoop sequenceFile.

### Prepare the data

The data preparation step runs the following task:
- Download the enron email corpus to the `/resources folder`
- Combine all the text files into one Hadoop sequenceFile
- Parse the email text blobs using Spark and the built-in python email parser
- Save the resulting RDD into a collection of PickleFiles

```
sh ./build.sh
```

### Run queries

Run computation in a Spark cluster (1 master + 2 workers by default) ran in Docker and using Spark SQL.
The SQL queries and the schema definition can be found in [app/helpers/sql_helper.py](app/helpers/sql_helper.py)

    sh ./run.sh

The [docker-compose.yml](docker-compose.yml) file defines the `spark-master` and `spark-workers` containers.
By default each worker has `1 CPU` and `1 GB` of memory.
You can change the number of workers in [run.sh](run.sh) and their allocated resources in [docker-compose.yml](docker-compose.yml).

## Code structure

| file | Description    |
|---------|-----------------------|
| [app/main.py](app/main.py) | Main appplication. We define the spark context here, we read a pickleFile, transform the RDD into a dataframe and executes the SQL queries.|
| [app/dataIO/prepare_data.py](app/dataIO/prepare_data.py) | We parse the email text blobs using Spark and save the RDD into pickleFiles.|
| [app/helpers/parser_helper.py](app/helpers/parser_helper.py) | Helper functions used to parse text emails. |
| [app/helpers/sql_helper.py](app/helpers/sql_helper.py) | Helper functions for spark SQL. Methods return the SQL schema and SQL queries. |
