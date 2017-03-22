from app.flask import Flask, request, jsonify
import os
import subprocess
from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SparkSession

# Get Spark context
sc = SparkContext()
# Make helper file available to all workers using Google Cloud Storage
sc.addPyFile('PATH_TO_sql_helper.py')
import sql_helper
# Spark SQL schema definition
spark = SparkSession(sc)

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello world!"

@app.route('/direct-emails')
def direct_emails():
    result = spark.sql(sql_helper.direct_email_query()).collect()
    return jsonify(result)

@app.route('/response-times')
def response_times():
    result = spark.sql(sql_helper.response_times_query()).collect()
    return jsonify(result)

@app.route('/broadcast-emails')
def broadcast_emails():
    result = spark.sql(sql_helper.broadcast_email_query()).collect()
    return jsonify(result)


if __name__ == '__main__':
    # Create an RDD from the sequenceFile
    emails_rdd = sc.pickleFile('PATH_TO_PICKLEFILES_DATA')
    schema = sql_helper.build_schema()
    df = emails_rdd.toDF(schema).persist(StorageLevel.DISK_ONLY)
    df.createOrReplaceTempView('emails')
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)