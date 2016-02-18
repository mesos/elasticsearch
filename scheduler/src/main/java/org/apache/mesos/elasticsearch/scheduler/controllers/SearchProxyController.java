package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.apache.mesos.elasticsearch.scheduler.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.stream.Stream;

/**
 *
 */
@RestController
@RequestMapping("/v1/es")
public class SearchProxyController {
    @Autowired
    ElasticsearchScheduler scheduler;

    @Autowired
    HttpClient httpClient;

    @RequestMapping("/_cluster/stats")
    public ResponseEntity<InputStreamResource> stats() throws IOException {
        HttpHost httpHost = getHttpHost(scheduler);
        HttpResponse esSearchResponse = httpClient.execute(httpHost, new HttpGet("/_cluster/stats"));
        InputStreamResource inputStreamResource = new InputStreamResource(esSearchResponse.getEntity().getContent());
        return ResponseEntity.ok()
                .contentLength(esSearchResponse.getEntity().getContentLength())
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-ElasticSearch-host", httpHost.toHostString())
                .body(inputStreamResource);
    }

    @RequestMapping("/_cluster/state")
    public ResponseEntity<InputStreamResource> recovery() throws IOException {
        HttpHost httpHost = getHttpHost(scheduler);
        HttpResponse esResponse = httpClient.execute(httpHost, new HttpGet("/_cluster/state?filter_blocks=true&filter_nodes=true&filter_metadata=true"));
        InputStreamResource inputStreamResource = new InputStreamResource(esResponse.getEntity().getContent());
        return ResponseEntity.ok()
                .contentLength(esResponse.getEntity().getContentLength())
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-ElasticSearch-host", httpHost.toHostString())
                .body(inputStreamResource);
    }

    @RequestMapping("/_search")
    public ResponseEntity<InputStreamResource> search(@RequestParam("q") String query, @RequestHeader(value = "X-ElasticSearch-Host", required = false) String elasticSearchHost) throws IOException {
        HttpHost httpHost = null;
        Collection<Task> tasks = scheduler.getTasks().values();
        Stream<HttpHost> httpHostStream = tasks.stream().map(task -> new HttpHost(task.getHostname(), task.getClientAddress().getPort()));

        if (elasticSearchHost != null) {
            httpHost = httpHostStream.filter(host -> host.toHostString().equalsIgnoreCase(elasticSearchHost)).findAny().get();
        } else {
            httpHost = httpHostStream.skip(RandomUtils.nextInt(tasks.size())).findAny().get();
        }

        HttpResponse esSearchResponse = httpClient.execute(httpHost, new HttpGet("/_search?q=" + URLEncoder.encode(query, "UTF-8")));

        InputStreamResource inputStreamResource = new InputStreamResource(esSearchResponse.getEntity().getContent());

        return ResponseEntity.ok()
                .contentLength(esSearchResponse.getEntity().getContentLength())
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-ElasticSearch-host", httpHost.toHostString())
                .body(inputStreamResource);
    }

    private static HttpHost getHttpHost(ElasticsearchScheduler scheduler) {
        Collection<Task> tasks = scheduler.getTasks().values();
        Stream<HttpHost> httpHostStream = tasks.stream().map(task -> toHttpHost(task.getClientAddress()));
        return httpHostStream.skip(RandomUtils.nextInt(tasks.size())).findAny().get();
    }

    private static HttpHost toHttpHost(InetSocketAddress address) {
        return new HttpHost(address.getAddress(), address.getPort());
    }
}
