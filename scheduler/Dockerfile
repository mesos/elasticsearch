FROM mesosphere/mesos:0.22.1-1.0.ubuntu1404

ADD ./build/docker/elasticsearch-mesos-scheduler.jar /tmp/elasticsearch-mesos-scheduler.jar

ADD ./build/docker/elasticsearch-cloud-mesos.zip /tmp/elasticsearch-cloud-mesos.zip

ADD ./build/docker/elasticsearch-mesos-executor.jar /tmp/elasticsearch-mesos-executor.jar

WORKDIR /tmp

ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib", "-jar", "elasticsearch-mesos-scheduler.jar"]
