package org.apache.mesos.elasticsearch.executor;

import org.apache.log4j.Logger;

import java.net.URISyntaxException;

/**
 * Executor configuration
 */
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String ELASTICSEARCH_YML = "elasticsearch.yml";

    private String elasticsearchSettingsLocation = getElasticsearchSettingsPath();

    public String getElasticsearchSettingsLocation() {
        return elasticsearchSettingsLocation;
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
}
