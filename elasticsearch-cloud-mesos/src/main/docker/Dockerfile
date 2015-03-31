FROM elasticsearch:1.4

VOLUME ["/maven"]
COPY maven /maven/

RUN plugin --install cloud-mesos --url file:/maven/elasticsearch-cloud-mesos-0.0.1-SNAPSHOT.zip
RUN echo "discovery.type: mesos" > /usr/share/elasticsearch/config/elasticsearch.yml