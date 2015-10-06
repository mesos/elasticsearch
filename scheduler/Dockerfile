FROM mesos/elasticsearch-base:latest

ADD ./build/docker/elasticsearch-mesos-scheduler.jar /tmp/elasticsearch-mesos-scheduler.jar
ADD ./build/docker/start-scheduler.sh /tmp/start-scheduler.sh

ENTRYPOINT ["/tmp/start-scheduler.sh"]
