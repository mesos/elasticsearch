package org.apache.mesos.elasticsearch.scheduler;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.*;

/**
 *
 */
@EnableAutoConfiguration
@ComponentScan
@org.springframework.context.annotation.Configuration
public class WebApplication {

    @Bean
    public HttpClient httpClient() {
        return HttpClients.createSystem();
    }
}
