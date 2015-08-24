package org.apache.mesos.elasticsearch.scheduler;

import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.context.annotation.Configuration;

/**
 */
@Configuration
public class WebConfig extends WebMvcConfigurerAdapter {
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("*^/$", "/")
            .setKeepQueryParams(true)
            .setStatusCode(HttpStatus.PERMANENT_REDIRECT);
    }
}
