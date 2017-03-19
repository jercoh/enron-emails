FROM fokkodriesprong/docker-pyspark:latest

# copy over code
RUN mkdir /app
RUN mkdir /resources

CMD ["/usr/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
