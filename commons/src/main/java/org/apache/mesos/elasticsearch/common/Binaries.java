package org.apache.mesos.elasticsearch.common;

/**
 * Contains information about the binaries used by the framework such as filesystem and HDFS locations.
 */
public class Binaries {

    // Elasticsearch HDFS path

    public static final String ES_HDFS_PATH = "/elasticsearch-mesos/";

    // Elasticsearch cloud plugin constants

    public static final String ES_CLOUD_MESOS_PLUGIN_NAME = "cloud-mesos";

    public static final String ES_CLOUD_MESOS_FILE_URL = "file:%s/elasticsearch-cloud-mesos.zip";

    public static final String ES_CLOUD_MESOS_HDFS_PATH = ES_HDFS_PATH + "elasticsearch-cloud-mesos.zip";

    // Elasticsearch executor constants

    public static final String ES_EXECUTOR_JAR = "elasticsearch-mesos-executor.jar";

    public static final String ES_EXECUTOR_HDFS_PATH = ES_HDFS_PATH + ES_EXECUTOR_JAR;

}
