package org.apache.mesos.elasticsearch.executor;

import com.beust.jcommander.JCommander;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.HostsCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.elasticsearch.common.settings.Settings;

import java.nio.file.Paths;
import java.util.List;

/**
 * Executor configuration
 */
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String ELASTICSEARCH_YML = "elasticsearch.yml";
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();

    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();
    private final HostsCLIParameter hostsCLIParameter = new HostsCLIParameter();

    public Configuration(String[] args) {
        final JCommander jCommander = new JCommander();
        jCommander.addObject(getElasticsearchCLI());
        jCommander.addObject(hostsCLIParameter);
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
    public Settings.Builder getUserESSettings() {
        String settingsLocation = getElasticsearchCLI().getElasticsearchSettingsLocation();
        if (settingsLocation.isEmpty()) {
            return Settings.builder();
        } else {
            return Settings.builder().loadFromPath(Paths.get(settingsLocation));
        }
    }

    public Settings.Builder getDefaultESSettings() {
        return Settings.builder().loadFromStream(ELASTICSEARCH_YML, this.getClass().getClassLoader().getResourceAsStream(ELASTICSEARCH_YML));
    }

    public ElasticsearchCLIParameter getElasticsearchCLI() {
        return elasticsearchCLI;
    }

    public List<String> getElasticsearchHosts() {
        return hostsCLIParameter.getElasticsearchHosts();
    }
}
