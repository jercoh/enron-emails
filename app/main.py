import os
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from app.helpers import sql_helper


def parse_email(email):
    import parser_helper
    return parser_helper.string_to_dict(email)


def main():
    source_file_path = os.environ.get('SOURCE_FILE') or 'resources/enron_mail.seq'
    if source_file_path:
        # Cluster definition
        conf = SparkConf().setAppName('Enron emails').setMaster('spark://spark-master:7077')
        sc = SparkContext(conf=conf)
        # sc = SparkContext('local', 'Enron emails')

        # Make helper file available to all workers
        sc.addPyFile('app/helpers/parser_helper.py')

        # Create an RDD from the sequenceFile
        emails_rdd = sc.sequenceFile(source_file_path)\
            .map(lambda t: parse_email(t[1]))\
            .filter(lambda e: e is not None)\
            .persist(StorageLevel.MEMORY_ONLY)

        # Spark SQL schema definition
        spark = SparkSession(sc)
        schema = sql_helper.build_schema()
        df = emails_rdd.toDF(schema)
        df.printSchema()
        df.createOrReplaceTempView("emails")

        # Queries execution:
        # Direct emails
        spark.sql(sql_helper.direct_email_query()).show()
        #
        # Broadcast emails
        spark.sql(sql_helper.broadcast_email_query()).show()

        # Response times
        spark.sql(sql_helper.response_times_query()).show()


if __name__ == '__main__':
    main()
