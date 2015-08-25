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
    @Parameter(names = {ELASTICSEARCH_SETTINGS_LOCATION}, description = "Local path to custom elasticsearch.yml settings file", validateWith = CLIValidators.NotEmptyString.class)
    private String elasticsearchSettingsLocation = "";
    public String getElasticsearchSettingsLocation() {
        return elasticsearchSettingsLocation;
    }
}
