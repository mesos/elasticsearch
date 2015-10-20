FROM mesos/elasticsearch-base:latest

ADD ./build/docker/elasticsearch-mesos-executor.jar /tmp/elasticsearch-mesos-executor.jar
ADD ./build/docker/start-executor.sh /tmp/start-executor.sh

ENTRYPOINT ["/tmp/start-executor.sh"]
