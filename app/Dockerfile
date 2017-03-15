FROM fokkodriesprong/docker-pyspark:latest

# copy over code
RUN mkdir /app
RUN mkdir /resources
ADD . /app/

CMD ["/usr/local/bin/python", "/app/main.py"]