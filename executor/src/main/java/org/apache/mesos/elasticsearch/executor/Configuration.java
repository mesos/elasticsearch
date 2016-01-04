package org.apache.mesos.elasticsearch.executor;

import com.beust.jcommander.JCommander;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.HostsCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;

import java.net.URISyntaxException;
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
        jCommander.addObject(elasticsearchCLI);
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
    public String getElasticsearchSettingsLocation() {
        String result = elasticsearchCLI.getElasticsearchSettingsLocation();
        if (result.isEmpty()) {
            result = getElasticsearchSettingsPath();
        }
        return result;
    }

    public int getElasticsearchNodes() {
        return elasticsearchCLI.getElasticsearchNodes();
    }

    private String getElasticsearchSettingsPath() {
        String path = "";
        try {
            path = getClass().getClassLoader().getResource(ELASTICSEARCH_YML).toURI().toString();
        } catch (NullPointerException | URISyntaxException ex) {
            LOGGER.error("Unable to read default settings file from resources", ex);
        }
        return path;
    }

    public String getElasticsearchClusterName() {
        return elasticsearchCLI.getElasticsearchClusterName();
    }

    public List<String> getElasticsearchHosts() {
        return hostsCLIParameter.getElasticsearchHosts();
    }
}
