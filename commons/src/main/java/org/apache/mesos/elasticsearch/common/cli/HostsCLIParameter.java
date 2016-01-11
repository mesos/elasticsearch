package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

import java.util.ArrayList;
import java.util.List;

/**
 */
@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class HostsCLIParameter {
    public static final String ELASTICSEARCH_HOST = "--elasticsearchHost";
    public static final String DOCKER_1_9_GATEWAY = "172.17.0.1";
    public static final String DOCKER_1_7_GATEWAY = "172.17.42.1";
    @Parameter(names = {ELASTICSEARCH_HOST}, description = "Elasticsearch unicast hosts (repeatable).", validateWith = CLIValidators.NotEmptyString.class)
    private List<String> elasticsearchHosts = new ArrayList<>(0);
    public List<String> getElasticsearchHosts() {
        // This is a massive hack for minimesos. The reported IP addresses in ipAddress mode are the slaves; not the ES containers.
        // So to get around this, we add the docker gateways by default. That way, the ES container address will fail, but the
        // gateway address, with the exposed port, will work.
        // This shouldn't affect production.
        if (!elasticsearchHosts.isEmpty()) {
            // Add default docker gateway addresses, so when using minimesos, the ES also tries to cluster with the exposed hosts.
            Integer port = Integer.parseInt(elasticsearchHosts.get(0).split(":")[1]);
            addDefaultDockerGatewayToUnicastHosts(port);
        } else {
            // If the user hasn't specified hosts, then just add the default 9300.
            addDefaultDockerGatewayToUnicastHosts(9300);
        }
        return elasticsearchHosts;
    }

    public void addDefaultDockerGatewayToUnicastHosts(Integer port) {
        if (!elasticsearchHosts.contains(DOCKER_1_9_GATEWAY)) {
            elasticsearchHosts.add(DOCKER_1_9_GATEWAY + ":" + port.toString());
        }
        if (!elasticsearchHosts.contains(DOCKER_1_7_GATEWAY)) {
            elasticsearchHosts.add(DOCKER_1_7_GATEWAY + ":" + port.toString());
        }
    }
}
