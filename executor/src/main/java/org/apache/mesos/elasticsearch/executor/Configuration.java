package org.apache.mesos.elasticsearch.executor;

import org.apache.log4j.Logger;

/**
 * Executor configuration
 */
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);

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
