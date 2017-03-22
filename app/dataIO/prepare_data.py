import os
from pyspark import SparkContext
from pyspark import SparkConf


def parse_email(email):
    import parser_helper
    return parser_helper.get_transactions(email)


def main():
    source_file_path = os.environ.get('SOURCE_FILE') or 'resources/enron_mails.seq'
    if source_file_path:
        # Cluster definition
        conf = SparkConf().setAppName('Enron emails').setMaster('spark://spark-master:7077')
        sc = SparkContext(conf=conf)

        # Make helper file available to all workers
        sc.addPyFile('app/helpers/parser_helper.py')

        # Create an RDD from the sequenceFile
        emails_rdd = sc.sequenceFile(source_file_path, 'org.apache.hadoop.io.Text',
                                     'org.apache.hadoop.io.BytesWritable')\
            .map(lambda t: t[1].decode('utf-8', errors='ignore'))\
            .flatMap(lambda e: parse_email(e))\
            .repartition(20)

        emails_rdd.saveAsPickleFile('resources/parsed')


if __name__ == '__main__':
    main()
