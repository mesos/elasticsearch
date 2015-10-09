package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.commons.lang.StringUtils;
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
        Collection<Task> tasks = scheduler.getTasks().values();
        Stream<HttpHost> httpHostStream = tasks.stream().map(task -> toHttpHost(task.getClientAddress()));
        HttpHost httpHost = httpHostStream.skip(RandomUtils.nextInt(tasks.size())).findAny().get();

        HttpResponse esSearchResponse = httpClient.execute(httpHost, new HttpGet("/_cluster/stats"));
        InputStreamResource inputStreamResource = new InputStreamResource(esSearchResponse.getEntity().getContent());

        return ResponseEntity.ok()
            .contentLength(esSearchResponse.getEntity().getContentLength())
            .contentType(MediaType.APPLICATION_JSON)
            .header("X-elasticsearch-host", httpHost.toHostString())
            .body(inputStreamResource);
    }

    @RequestMapping("/_search")
    public ResponseEntity<InputStreamResource> search(@RequestParam("q") String query, @RequestParam(value = "node", required = false) String elasticSearchTaskId) throws IOException {
        HttpHost httpHost;
        Collection<Task> tasks = scheduler.getTasks().values();

        if (StringUtils.isNotBlank(elasticSearchTaskId)) {
            httpHost = tasks.stream().filter(task -> task.getTaskId().equals(elasticSearchTaskId)).map(task -> toHttpHost(task.getClientAddress())).findAny().get();
        } else {
            httpHost = tasks.stream().skip(RandomUtils.nextInt(tasks.size())).map(task -> toHttpHost(task.getClientAddress())).findAny().get();
        }

        HttpResponse esSearchResponse = httpClient.execute(httpHost, new HttpGet("/_search?q=" + URLEncoder.encode(query, "UTF-8")));

        InputStreamResource inputStreamResource = new InputStreamResource(esSearchResponse.getEntity().getContent());

        return ResponseEntity.ok()
                .contentLength(esSearchResponse.getEntity().getContentLength())
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-ElasticSearch-host", httpHost.toHostString())
                .body(inputStreamResource);
    }

    private static HttpHost toHttpHost(InetSocketAddress address) {
        return new HttpHost(address.getAddress(), address.getPort());
    }
}
