from pyspark.sql.types import *


def build_schema():
    """Build a SQL schema

     Returns the following SQL schema:
     |-- id: string (nullable = true)
     |-- date: timestamp (nullable = true)
     |-- from: string (nullable = true)
     |-- recipients: array (nullable = true)
     |-- subject: string (nullable = true)

     Returns:
        StructType: Spark SQL schema

    """
    fields = [
        StructField('id', StringType(), True),
        StructField('date', TimestampType(), True),
        StructField('sender', StringType(), True),
        StructField('recipients', ArrayType(StringType()), True),
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
            e.recipients[0] `recipient`,
            COUNT(id) `count_direct_emails`
        FROM emails e
        WHERE size(e.recipients) == 1
        GROUP BY e.recipients[0]
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
            e.sender `sender`,
            COUNT(id) `count_broadcast_emails`
        FROM emails e
        WHERE size(e.recipients) > 1
        GROUP BY e.sender
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
            Original.sender `sender`,
            Original.recipients `recipients`,
            Response.sender `response_sender`,
            Response.recipients `response_recipients`,
            unix_timestamp(Response.date) - unix_timestamp(Original.date) `response_time`
        FROM (
          SELECT *
          FROM emails
          WHERE subject != ""
        ) Original
           INNER JOIN emails Response
               ON (
                  locate(Original.subject, Response.subject) > 0
                  AND array_contains(Response.recipients, Original.sender)
                  AND array_contains(Original.recipients, Response.sender)
                  AND unix_timestamp(Response.date) - unix_timestamp(Original.date) > 0
               )
        ORDER BY `response_time`
        LIMIT 5
    '''
