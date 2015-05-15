FROM elasticsearch:1.4.5

ADD build/docker/elasticsearch-cloud-mesos.zip /tmp/elasticsearch-cloud-mesos.zip

RUN plugin --install cloud-mesos --url file:/tmp/elasticsearch-cloud-mesos.zip
