package org.apache.mesos.elasticsearch.systemtest;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.mesos.elasticsearch.common.util.NetworkUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SystemTest configuration object
 */
@SuppressWarnings({"PMD.AvoidUsingHardCodedIP"})
public class Configuration {
    private String schedulerImageName = "mesos/elasticsearch-scheduler";
    private String schedulerName = "elasticsearch-scheduler";
    private int schedulerGuiPort = 31100;
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
        return getPortRanges().size();
    }

    public int getElasticsearchMemorySize() {
        return elasticsearchMemorySize;
    }

    public String getElasticsearchJobName() {
        return elasticsearchJobName;
    }

    private final List<ESPorts> ports = Arrays.asList(
            new ESPorts(9200, 9300),
            new ESPorts(9201, 9301),
            new ESPorts(9202, 9302)
    );

    public List<ESPorts> getPorts() {
        return ports;
    }

    public List<String> getPortRanges() {
        return getPorts().stream().map(pair -> "ports(*):" +
                "[" + pair.client() + "-" + pair.client() + "," + pair.transport() + "-" + pair.transport() + "]; " +
                "cpus(*):1.0; mem(*):256; disk(*):200")
                .collect(Collectors.toList());
    }

    public Integer getClusterTimeout() {
        return clusterTimeout;
    }

    public static class ESPorts {
        private final Pair<Integer, Integer> ports;

        public ESPorts(final Integer client, final Integer transport) {
            ports = Pair.of(client, transport);
        }

        public Integer client() {
            return ports.getLeft();
        }

        public Integer transport() {
            return ports.getRight();
        }
    }
}
