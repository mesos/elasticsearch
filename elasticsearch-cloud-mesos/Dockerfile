FROM elasticsearch:1.4

ADD elasticsearch-cloud-mesos.zip /tmp/elasticsearch-cloud-mesos.zip

RUN plugin --install cloud-mesos --url file:/tmp/elasticsearch-cloud-mesos.zip