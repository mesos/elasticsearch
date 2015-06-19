package org.apache.mesos.elasticsearch.common;

/**
 * Shared framework configuration.
 */
public class Configuration {

    public static final String FRAMEWORK_NAME = "elasticsearch";

    public static final String TASK_NAME = "esdemo";

    public static final String DOMAIN = "mesos";

    public static final int ELASTICSEARCH_CLIENT_PORT = 9200;

    public static final int ELASTICSEARCH_TRANSPORT_PORT = 9300;

    public static final Integer ZOOKEEPER_PORT = 2181;

    public static final int MESOS_PORT = 5050;

    public static final double CPUS = 0.2;

    public static final double MEM = 512;

    public static final double DISK = 250;

    public static final long BEGIN_PORT = 31000;

    public static final long END_PORT = 32000;
}
