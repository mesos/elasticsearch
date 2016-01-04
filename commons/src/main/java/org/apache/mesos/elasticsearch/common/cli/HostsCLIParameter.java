package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class HostsCLIParameter {
    public static final String ELASTICSEARCH_HOST = "--elasticsearchHost";
    @Parameter(names = {ELASTICSEARCH_HOST}, description = "Elasticsearch unicast hosts (repeatable).", validateWith = CLIValidators.NotEmptyString.class)
    private List<String> elasticsearchHosts = new ArrayList<>(0);
    public List<String> getElasticsearchHosts() {
        return elasticsearchHosts;
    }
}
