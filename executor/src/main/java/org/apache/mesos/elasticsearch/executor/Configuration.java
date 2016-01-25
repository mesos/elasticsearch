package org.apache.mesos.elasticsearch.executor;

import com.beust.jcommander.JCommander;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.HostsCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;

/**
 * Executor configuration
 */
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String ELASTICSEARCH_YML = "elasticsearch.yml";
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();
    UrlValidator urlValidator = new UrlValidator();

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
        } else if (urlValidator.isValid(settingsLocation)) { // If url
            final InputStream is;
            try {
                final URL url = URI.create(settingsLocation).toURL();
                LOGGER.debug("Loading settings from url: " + url.toExternalForm());
                is = url.openStream();
                return Settings.builder().loadFromStream(settingsLocation, is);
            } catch (IOException e) {
                LOGGER.error("Tried to load from URL: " + settingsLocation + ", but failed.", e);
                throw new IllegalStateException("Unable to load user specified settings file. Please check the URL");
            }
        } else { // If file
            LOGGER.debug("Loading settings from file: " + settingsLocation);
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
