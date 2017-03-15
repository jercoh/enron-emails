import os
from pyspark import SparkContext
from app.helpers import email_parser


def main():
    source_file_path = os.environ.get('SOURCE_FILE')
    if source_file_path:
        # local computation
        sc = SparkContext("local", "Enron emails")
        emails_rdd = sc.sequenceFile(source_file_path)\
            .map(lambda t: email_parser.string_to_dict(t[1]))\
            .filter(lambda e: e is not None)\
            .cache()

        # Direct emails
        direct_emails = emails_rdd\
            .filter(lambda e: len(e['to']) == 1)\
            .groupBy(lambda e: e['to'][0])\
            .mapValues(len)

        # Broadcast emails
        broadcast_emails = emails_rdd\
            .filter(lambda e: len(e['to']) + len(e['cc']) + len(e['bcc']) > 1)\
            .groupBy(lambda e: e['from'][0])\
            .mapValues(len)

        print(direct_emails.takeOrdered(3, lambda x: -x[1]))
        print(broadcast_emails.takeOrdered(3, lambda x: -x[1]))


if __name__ == '__main__':
    main()
