from pyspark.sql.types import *


def build_schema():
    """Build a SQL schema

     Returns the following SQL schema:
     |-- id: string (nullable = true)
     |-- date: timestamp (nullable = true)
     |-- from: string (nullable = true)
     |-- to: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- cc: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- bcc: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- subject: string (nullable = true)

     Returns:
        StructType: Spark SQL schema

    """
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
    """Emails that have exactly one recipient.

    Returns a SQL query answering the following question:
    Which 3 recipients received the largest number of direct emails?

    Returns:
        str: SQL query string

    """
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
    """Emails that have multiple recipients, including CCs and BCCs.

    Returns a SQL query answering the following question:
    Which 3 senders sent the largest number of broadcast emails?

    Returns:
        str: SQL query string

    """
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
    """Emails ordered by response time.

    Returns a SQL query answering the following question:
    Find the 5 emails with the fastest response times.

    Returns:
        str: SQL query string

    """
    return '''
        SELECT
            Original.id,
            Original.subject `original_subject`,
            Response.subject `response_subject`,
            Original.from `sender`,
            Original.to `recipient`,
            unix_timestamp(Response.date) - unix_timestamp(Original.date) `response_time`
        FROM (
          SELECT *
          FROM emails
          WHERE subject != ""
        ) Original
           INNER JOIN emails Response
               ON (
                  locate(Original.subject, Response.subject) > 0
                  AND array_contains(Response.to, Original.from)
                  AND unix_timestamp(Response.date) - unix_timestamp(Original.date) > 0
               )
        ORDER BY `response_time`
        LIMIT 5
    '''
