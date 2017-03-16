import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from app.helpers import parser_helper
from app.helpers import sql_helper


def main():
    source_file_path = os.environ.get('SOURCE_FILE')
    if source_file_path:
        # local computation
        sc = SparkContext("local", "Enron emails")
        emails_rdd = sc.sequenceFile(source_file_path)\
            .map(lambda t: parser_helper.string_to_dict(t[1]))\
            .filter(lambda e: e is not None)\
            .cache()

        spark = SparkSession(sc)

        schema = sql_helper.build_schema()
        df = emails_rdd.toDF(schema)
        df.printSchema()
        df.createOrReplaceTempView("emails")

        # Direct emails
        spark.sql(sql_helper.direct_email_query()).show()

        # Broadcast emails
        spark.sql(sql_helper.broadcast_email_query()).show()

        # Response times
        spark.sql(sql_helper.response_times_query()).show()


if __name__ == '__main__':
    main()
