package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

/**
 * Common elasticsearch parameters
 */
public class ElasticsearchCLIParameter {
    public static final String ELASTICSEARCH_CLUSTER_NAME = "--elasticsearchClusterName";
    public static final String ELASTICSEARCH_SETTINGS_LOCATION = "--elasticsearchSettingsLocation";
    public static final String ELASTICSEARCH_NODES = "--elasticsearchNodes";
    @Parameter(names = {ELASTICSEARCH_CLUSTER_NAME}, description = "Name of the elasticsearch cluster", validateWith = CLIValidators.NotEmptyString.class)
    private String elasticsearchClusterName = "mesos-ha";
    @Parameter(names = {ELASTICSEARCH_SETTINGS_LOCATION},
            description = "URI to ES yml settings file. If file is copied to all slaves, the file must be in /tmp/config. E.g. 'file:/tmp/config/elasticsearch.yml', 'http://webserver.com/elasticsearch.yml'",
            validateWith = CLIValidators.NotEmptyString.class)
    private String elasticsearchSettingsLocation = "";
    @Parameter(names = {ELASTICSEARCH_NODES}, description = "Number of elasticsearch instances.", validateValueWith = OddNumberOfNodes.class)
    private int elasticsearchNodes = 3;

    public String getElasticsearchClusterName() {
        return elasticsearchClusterName;
    }

    public String getElasticsearchSettingsLocation() {
        if (elasticsearchSettingsLocation.equals("DEFAULT")) {
            elasticsearchSettingsLocation = "";
        }
        return elasticsearchSettingsLocation;
    }

    public int getElasticsearchNodes() {
        return elasticsearchNodes;
    }

    /**
     * Adds a warning message if an even number is encountered
     */
    public static class OddNumberOfNodes extends CLIValidators.PositiveInteger {
        @Override
        public Boolean notValid(Integer value) {
            if (value % 2 == 0) {
                System.out.println("Setting number of ES nodes to an even number. Not recommended!"); // Log4j not in commons package.
            }
            return super.notValid(value);
        }
    }
}
