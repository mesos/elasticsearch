package org.apache.mesos.elasticsearch.common.cli;

import com.beust.jcommander.Parameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;

/**
 * Common elasticsearch parameters
 */
public class ElasticsearchCLIParameter {
    public static final String ELASTICSEARCH_CLUSTER_NAME = "--elasticsearchClusterName";
    @Parameter(names = {ELASTICSEARCH_CLUSTER_NAME}, description = "Name of the elasticsearch cluster", validateWith = CLIValidators.NotEmptyString.class)
    private String elasticsearchClusterName = "mesos-ha";
    public String getElasticsearchClusterName() {
        return elasticsearchClusterName;
    }

    public static final String ELASTICSEARCH_SETTINGS_LOCATION = "--elasticsearchSettingsLocation";
    @Parameter(names = {ELASTICSEARCH_SETTINGS_LOCATION},
            description = "Path or URL to ES yml settings file. E.g. '/tmp/config/elasticsearch.yml' or 'https://gist.githubusercontent.com/mmaloney/5e1da5daa58b70a3a671/raw/elasticsearch.yml'",
            validateWith = CLIValidators.NotEmptyString.class)
    private String elasticsearchSettingsLocation = "";
    public String getElasticsearchSettingsLocation() {
        return elasticsearchSettingsLocation;
    }

    public static final String ELASTICSEARCH_NODES = "--elasticsearchNodes";
    @Parameter(names = {ELASTICSEARCH_NODES}, description = "Number of elasticsearch instances.", validateValueWith = OddNumberOfNodes.class)
    private int elasticsearchNodes = 3;
    public int getElasticsearchNodes() {
        return elasticsearchNodes;
    }
    public void setElasticsearchNodes(int numberOfNodes) throws IllegalArgumentException {
        if (numberOfNodes <= 0) {
            throw new IllegalArgumentException("Cluster size cannot be zero. This will result in data loss.");
        }
        elasticsearchNodes = numberOfNodes;
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
