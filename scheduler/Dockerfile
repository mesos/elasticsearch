FROM mesos/elasticsearch-base:latest

ADD ./build/docker/mesos-elasticsearch-scheduler.jar /tmp/mesos-elasticsearch-scheduler.jar
ADD ./build/docker/start-scheduler.sh /tmp/start-scheduler.sh

ENTRYPOINT ["/tmp/start-scheduler.sh"]
