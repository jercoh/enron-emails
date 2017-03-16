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
    return '''
        SELECT
            e.to[0] `recipient`,
            COUNT(id) `count_direct_emails`
        FROM emails e
        WHERE size(e.to) + size(e.cc) + size(e.bcc) == 1
        GROUP BY e.to[0]
        ORDER BY COUNT(id) DESC
        LIMIT 3
    '''


def broadcast_email_query():
    return '''
        SELECT
            e.from `sender`,
            COUNT(id) `count_broadcast_emails`
        FROM emails e
        WHERE size(e.to) + size(e.cc) + size(e.bcc) > 1
        GROUP BY e.from
        ORDER BY COUNT(id) DESC
        LIMIT 3
    '''


def response_times_query():
    return '''
        SELECT
            Original.id,
            Original.subject `original_subject`,
            Response.subject `response_subject`,
            Original.from `sender`,
            Original.to `recipient`,
            unix_timestamp(Response.date) - unix_timestamp(Original.date) `response_time`
        FROM emails Original
           INNER JOIN emails Response
               ON (
                  locate(Original.subject, Response.subject) > 0
                  AND Original.subject != ""
                  AND Response.subject != ""
                  AND array_contains(Response.to, Original.from)
                  AND unix_timestamp(Response.date) - unix_timestamp(Original.date) > 0
               )
        ORDER BY `response_time`
        LIMIT 5
    '''
