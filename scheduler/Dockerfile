FROM mesosphere/mesos:0.20.1-1.0.ubuntu1404

ADD ./build/docker/elasticsearch-mesos-scheduler.jar /tmp/elasticsearch-mesos-scheduler.jar

ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib", "-jar", "/tmp/elasticsearch-mesos-scheduler.jar"]
