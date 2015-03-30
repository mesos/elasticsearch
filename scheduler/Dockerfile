FROM mesosphere/mesos:0.20.1-1.0.ubuntu1404

ADD es-scheduler.jar /tmp/es-scheduler.jar

ENTRYPOINT ["java", "-Djava.library.path=/`usr/local/lib", "-jar", "/tmp/es-scheduler.jar"]
