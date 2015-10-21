package org.apache.mesos.elasticsearch.systemtest;

/**
 * SystemTest configuration object
 */
public class Configuration {
    private String executorImageName = "mesos/elasticsearch-executor";
    private String schedulerImageName = "mesos/elasticsearch-scheduler";
    private String schedulerName = "elasticsearch-scheduler";
    private int schedulerGuiPort = 31100;
    private int privateRegistryPort = 15000;
    private int elasticsearchNodesCount = 3;
    private int elasticsearchMemorySize = 256;
    private String elasticsearchJobName = "esdemo";

    public String getExecutorImageName() {
        return executorImageName;
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

    public int getPrivateRegistryPort() {
        return privateRegistryPort;
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
            "ports(*):[9200-9200,9300-9300]",
            "ports(*):[9201-9201,9301-9301]",
            "ports(*):[9202-9202,9302-9302]"
        };
    }
}
