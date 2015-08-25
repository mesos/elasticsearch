package org.apache.mesos.elasticsearch.executor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;
import org.apache.mesos.elasticsearch.common.zookeeper.ZookeeperCLIParameter;

/**
 * Executor configuration
 */
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();

    public Configuration(String[] args) {
        final JCommander jCommander = new JCommander();
        jCommander.addObject(zookeeperCLI);
        jCommander.addObject(this);
        try {
            jCommander.parse(args); // Parse command line args into configuration class.
        } catch (com.beust.jcommander.ParameterException ex) {
            System.out.println(ex);
            jCommander.setProgramName("(Options preceded by an asterisk are required)");
            jCommander.usage();
            throw ex;
        }
    }

    // ******* ELASTICSEARCH
    public static final String ELASTICSEARCH_SETTINGS_LOCATION = "--elasticsearchSettingsLocation";
    @Parameter(names = {ELASTICSEARCH_SETTINGS_LOCATION}, description = "Local path to custom elasticsearch.yml settings file", validateWith = CLIValidators.NotEmptyString.class)
    private String elasticsearchSettingsLocation = getElasticsearchSettingsPath();
    public String getElasticsearchSettingsLocation() {
        return elasticsearchSettingsLocation;
    }
    private String getElasticsearchSettingsPath() {
        String path = "";
        try {
            path = getClass().getClassLoader().getResource("elasticsearch.yml").getPath();
        } catch (NullPointerException ex) {
            LOGGER.error("Unable to read default settings file from resources", ex);
        }
        return path;
    }
}
