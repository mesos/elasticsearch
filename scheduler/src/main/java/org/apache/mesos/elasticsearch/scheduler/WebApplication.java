package org.apache.mesos.elasticsearch.scheduler;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.lang.ref.WeakReference;

@EnableAutoConfiguration
@ComponentScan
public class WebApplication {
    static WeakReference<ElasticsearchScheduler> elasticsearchScheduler;
    static WeakReference<Configuration> configuration;

    @Bean
    public ElasticsearchScheduler getElasticsearchScheduler() {
        return elasticsearchScheduler.get();
    }

    @Bean
    public Configuration getConfiguration() {
        return configuration.get();
    }


}
