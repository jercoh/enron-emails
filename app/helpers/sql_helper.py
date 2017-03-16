from pyspark.sql.types import *

def build_schema():
    fields = [
        StructField('id', StringType(), True),
        StructField('date', TimestampType(), True),
        StructField('from', StringType(), True),
        StructField('to', ArrayType(StringType()), True),
        StructField('cc', ArrayType(StringType()), True),
        StructField('bcc', ArrayType(StringType()), True),
        StructField('subject', StringType(), True)
    ]
    return StructType(fields)


def direct_email_query():
    return 'SELECT e.to[0] `recipient`, COUNT(id) `count of direct emails` ' \
           'FROM emails e ' \
           'WHERE size(e.to) == 1 ' \
           'GROUP BY e.to[0] ' \
           'ORDER BY COUNT(id) DESC ' \
           'LIMIT 3'


def broadcast_email_query():
    return 'SELECT e.from `sender`, COUNT(id) `count of broadcast emails` ' \
           'FROM emails e ' \
           'WHERE size(e.to) + size(e.cc) + size(e.bcc) > 1 ' \
           'GROUP BY e.from ' \
           'ORDER BY COUNT(id) DESC ' \
           'LIMIT 3'


def response_times_query():
    return 'SELECT e.id, e.subject `original subject`, le.subject `response subject`, e.from `sender`, e.to `recipient`, unix_timestamp(le.date) - unix_timestamp(e.date) `response time` ' \
           'FROM emails e ' \
           'INNER JOIN ' \
           'emails le ' \
           'ON locate(e.subject, le.subject) > 0 ' \
           'AND e.subject != "" ' \
           'AND le.subject != ""' \
           'AND (array_contains(le.to, e.from)) ' \
           'AND unix_timestamp(le.date) - unix_timestamp(e.date) > 0 ' \
           'ORDER BY `response time`' \
           'LIMIT 5'
