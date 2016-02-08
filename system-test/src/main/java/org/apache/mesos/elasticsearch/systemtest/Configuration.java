package org.apache.mesos.elasticsearch.systemtest;

import org.apache.mesos.elasticsearch.common.util.NetworkUtils;

/**
 * SystemTest configuration object
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Configuration {
    private String schedulerImageName = "mesos/elasticsearch-scheduler";
    private String schedulerName = "elasticsearch-scheduler";
    private int schedulerGuiPort = 31100;
    private int elasticsearchNodesCount = getPortRanges().length;
    private int elasticsearchMemorySize = 200;
    private String elasticsearchJobName = "esdemo";
    private final Integer clusterTimeout = 60;

    public static String getDocker0AdaptorIpAddress() {
        return NetworkUtils.getDockerHostIpAddress(NetworkUtils.getEnvironment());
    }

    public String getSchedulerImageName() {
        return schedulerImageName;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    public int getSchedulerGuiPort() {
        return schedulerGuiPort;
    }

    public int getElasticsearchNodesCount() {
        return elasticsearchNodesCount;
    }

    public int getElasticsearchMemorySize() {
        return elasticsearchMemorySize;
    }

    public String getElasticsearchJobName() {
        return elasticsearchJobName;
    }

    public String[] getPortRanges() {
        return new String[]{
                "ports(*):[9200-9200,9300-9300]; cpus(*):1.0; mem(*):256; disk(*):200",
                "ports(*):[9201-9201,9301-9301]; cpus(*):1.0; mem(*):256; disk(*):200",
                "ports(*):[9202-9202,9302-9302]; cpus(*):1.0; mem(*):256; disk(*):200"
        };
    }

    public Integer getClusterTimeout() {
        return clusterTimeout;
    }
}
