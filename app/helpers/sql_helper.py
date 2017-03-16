from pyspark.sql.types import *


def build_schema():
    fields = [
        StructField('id', StringType(), True),
        StructField('date', ArrayType(StringType()), True),
        StructField('from', ArrayType(StringType()), True),
        StructField('to', ArrayType(StringType()), True),
        StructField('cc', ArrayType(StringType()), True),
        StructField('bcc', ArrayType(StringType()), True),
        StructField('subject', StringType(), True)
    ]
    return StructType(fields)


def direct_email_query():
    return 'SELECT e.to, COUNT(id)  ' \
           'FROM emails e ' \
           'WHERE size(e.to) == 1 ' \
           'GROUP BY e.to ' \
           'ORDER BY COUNT(id) DESC ' \
           'LIMIT 3'


def broadcast_email_query():
    return 'SELECT e.from, COUNT(id)  ' \
           'FROM emails e ' \
           'WHERE size(e.to) + size(e.cc) + size(e.bcc) > 1 ' \
           'GROUP BY e.from ' \
           'ORDER BY COUNT(id) DESC ' \
           'LIMIT 3'
