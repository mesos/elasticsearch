FROM java:8

ADD ./build/docker/elasticsearch-mesos-executor.jar /tmp/elasticsearch-mesos-executor.jar

ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib", "-jar", "/tmp/elasticsearch-mesos-scheduler.jar"]
