package org.apache.mesos.elasticsearch.common;

/**
 * Contains information about the binaries used by the framework such as filesystem and HDFS locations.
 */
public class Binaries {

    // Elasticsearch cloud plugin constants

    public static final String ES_CLOUD_MESOS_PLUGIN_NAME = "cloud-mesos";

    public static final String ES_CLOUD_MESOS_ZIP = "elasticsearch-cloud-mesos.zip";

    public static final String ES_CLOUD_MESOS_FILE_URL = "file:%s/elasticsearch-cloud-mesos.zip";

    // Elasticsearch executor constants

    public static final String ES_EXECUTOR_JAR = "elasticsearch-mesos-executor.jar";

}
