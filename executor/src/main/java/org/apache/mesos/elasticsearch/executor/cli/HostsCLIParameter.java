package org.apache.mesos.elasticsearch.executor.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class HostsCLIParameter {
    @Parameter(names = {"--elasticsearchHost"}, description = "Elasticsearch unicast hosts (repeatable).", validateWith = CLIValidators.NotEmptyString.class)
    private List<String> elasticsearchHosts = new ArrayList<>(0);
    public List<String> getElasticsearchHosts() {
        return elasticsearchHosts;
    }
}
