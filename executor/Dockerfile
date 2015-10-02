FROM mesosphere/mesos-slave:0.22.1-1.0.ubuntu1404

RUN apt-get update && apt-get install -y software-properties-common

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer && \
  rm -rf \
    /usr/lib/jvm/java-8-oracle/*src.zip \
    /usr/lib/jvm/java-8-oracle/lib/missioncontrol \
    /usr/lib/jvm/java-8-oracle/lib/visualvm \
    /usr/lib/jvm/java-8-oracle/lib/*javafx* \
    /usr/lib/jvm/java-8-oracle/jre/lib/plugin.jar \
    /usr/lib/jvm/java-8-oracle/jre/lib/ext/jfxrt.jar \
    /usr/lib/jvm/java-8-oracle/jre/bin/javaws \
    /usr/lib/jvm/java-8-oracle/jre/lib/javaws.jar \
    /usr/lib/jvm/java-8-oracle/jre/lib/desktop \
    /usr/lib/jvm/java-8-oracle/jre/plugin \
    /usr/lib/jvm/java-8-oracle/jre/lib/deploy* \
    /usr/lib/jvm/java-8-oracle/jre/lib/*javafx* \
    /usr/lib/jvm/java-8-oracle/jre/lib/*jfx* \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libdecora_sse.so \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libprism_*.so \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libfxplugins.so \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libglass.so \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libgstreamer-lite.so \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libjavafx*.so \
    /usr/lib/jvm/java-8-oracle/jre/lib/amd64/libjfx*.so

ADD ./build/docker/elasticsearch-mesos-executor.jar /tmp/elasticsearch-mesos-executor.jar
ADD ./build/docker/start-executor.sh /tmp/start-executor.sh

ENTRYPOINT ["/tmp/start-executor.sh"]
