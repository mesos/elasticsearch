package org.apache.mesos.elasticsearch.scheduler;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

/**
 *
 */
@EnableAutoConfiguration
@ComponentScan
public class WebApplication {
    private final ElasticsearchScheduler elasticsearchScheduler;
    private final Configuration configuration;

    public WebApplication(ElasticsearchScheduler elasticsearchScheduler, Configuration configuration) {
        this.elasticsearchScheduler = elasticsearchScheduler;
        this.configuration = configuration;
    }

    @Bean
    public ElasticsearchScheduler getElasticsearchScheduler() {
        return elasticsearchScheduler;
    }

    @Bean
    public Configuration getConfiguration() {
        return configuration;
    }


}
