FROM elasticsearch:1.4

VOLUME ["/maven"]
COPY maven /maven/

RUN plugin --install cloud-mesos --url file:/maven/elasticsearch-cloud-mesos-1.0-SNAPSHOT.zip
RUN echo "discovery.type: mesos" > /usr/share/elasticsearch/config/elasticsearch.yml