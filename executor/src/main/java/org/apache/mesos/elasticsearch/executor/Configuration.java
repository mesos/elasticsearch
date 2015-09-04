package org.apache.mesos.elasticsearch.executor;

import com.beust.jcommander.JCommander;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ElasticsearchZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.elasticsearch.common.lang3.StringUtils;

import java.net.URISyntaxException;

/**
 * Executor configuration
 */
public class Configuration {
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String ELASTICSEARCH_YML = "elasticsearch.yml";
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();

    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();

    public Configuration(String[] args) {
        final JCommander jCommander = new JCommander();
        jCommander.addObject(zookeeperCLI);
        jCommander.addObject(elasticsearchCLI);
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

    public String getElasticsearchZKURL() {
        ZKFormatter mesosZKFormatter = new ElasticsearchZKFormatter(new ZKAddressParser());
        if (StringUtils.isBlank(zookeeperCLI.getZookeeperFrameworkUrl())) {
            LOGGER.info("Zookeeper framework option is blank, using Zookeeper for Mesos: " + zookeeperCLI.getZookeeperMesosUrl());
            return mesosZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
        } else {
            LOGGER.info("Zookeeper framework option : " + zookeeperCLI.getZookeeperFrameworkUrl());
            return mesosZKFormatter.format(zookeeperCLI.getZookeeperFrameworkUrl());
        }
    }

    public long getElasticsearchZKTimeout() {
        return zookeeperCLI.getZookeeperFrameworkTimeout();
    }

    public String getElasticsearchClusterName() {
        return elasticsearchCLI.getElasticsearchClusterName();
    }
}
