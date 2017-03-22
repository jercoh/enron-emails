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
        StructField('recipient', StringType(), True),
        StructField('recipients_count', IntegerType(), True),
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
            e.recipient `recipient`,
            COUNT(DISTINCT id) `count_direct_emails`
        FROM emails e
        WHERE e.recipients_count == 1
        GROUP BY e.recipient
        ORDER BY COUNT(DISTINCT id) DESC
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
            COUNT(DISTINCT id) `count_broadcast_emails`
        FROM emails e
        WHERE e.recipients_count > 1
        GROUP BY e.sender
        ORDER BY COUNT(DISTINCT id) DESC
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
            unix_timestamp(Response.date) - unix_timestamp(Original.date) `response_time`
        FROM (
          SELECT *
          FROM emails
          WHERE subject != ""
        ) Original
           INNER JOIN emails Response
               ON (
                  locate(Original.subject, Response.subject) > 0
                  AND Response.recipient == Original.sender
                  AND Original.recipient == Response.sender
                  AND unix_timestamp(Response.date) - unix_timestamp(Original.date) > 0
               )
        ORDER BY `response_time`
        LIMIT 5
    '''
