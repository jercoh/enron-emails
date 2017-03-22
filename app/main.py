import os
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from app.helpers import sql_helper


def main():
    source_file_path = os.environ.get('SOURCE_FILE') or 'resources/parsed/*'
    if source_file_path:
        # Cluster definition
        conf = SparkConf().setAppName('Enron emails').setMaster('spark://spark-master:7077')
        sc = SparkContext(conf=conf)

        # Create an RDD from the sequenceFile
        emails_rdd = sc.pickleFile(source_file_path)

        # Spark SQL schema definition
        spark = SparkSession(sc)
        schema = sql_helper.build_schema()
        df = emails_rdd.toDF(schema).cache()
        df.createOrReplaceTempView('emails')

        # Queries execution:
        # Direct emails
        spark.sql(sql_helper.direct_email_query()).show(truncate=False)
        #
        # Broadcast emails
        spark.sql(sql_helper.broadcast_email_query()).show(truncate=False)

        # Response times
        spark.sql(sql_helper.response_times_query()).show()


if __name__ == '__main__':
    main()
