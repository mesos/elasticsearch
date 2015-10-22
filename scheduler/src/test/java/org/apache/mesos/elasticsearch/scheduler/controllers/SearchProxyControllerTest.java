package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.InputStreamEntity;
import org.apache.mesos.elasticsearch.scheduler.ElasticsearchScheduler;
import org.apache.mesos.elasticsearch.scheduler.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the search proxy, Sherlock
 */
@RunWith(MockitoJUnitRunner.class)
public class SearchProxyControllerTest {

    @Mock
    HttpClient httpClient;

    @Mock
    ElasticsearchScheduler elasticsearchScheduler;

    @Mock
    HttpResponse httpResponse;

    @InjectMocks
    SearchProxyController controller;

    @Test
    public void willForwardSearchRequestToARandomNode() throws Exception {
        final HashMap<String, Task> tasks = new HashMap<>();
        tasks.put("a", new Task(null, "a", null, null, new InetSocketAddress("1.0.0.1", 1001), null));
        tasks.put("b", new Task(null, "b", null, null, new InetSocketAddress("1.0.0.1", 1002), null));
        tasks.put("c", new Task(null, "c", null, null, new InetSocketAddress("1.0.0.1", 1003), null));
        when(elasticsearchScheduler.getTasks()).thenReturn(tasks);
        when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getEntity()).thenReturn(new InputStreamEntity(new ByteArrayInputStream("Search result".getBytes())));

        final ResponseEntity<InputStreamResource> search = controller.search("test", null);
        assertEquals(200, search.getStatusCode().value());

        final ArgumentCaptor<HttpHost> httpHostArgumentCaptor = ArgumentCaptor.forClass(HttpHost.class);
        final ArgumentCaptor<HttpRequest> httpRequestArgumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).execute(httpHostArgumentCaptor.capture(), httpRequestArgumentCaptor.capture());
        assertEquals("1.0.0.1", httpHostArgumentCaptor.getValue().getHostName());
        assertEquals("/_search?q=test", httpRequestArgumentCaptor.getValue().getRequestLine().getUri());
    }

    @Test
    public void willForwardSearchRequestToAChosenNode() throws Exception {
        final String chosenNode = "1.0.0.1:1002";

        final HashMap<String, Task> tasks = new HashMap<>();
        tasks.put("a", new Task(null, "a", null, null, new InetSocketAddress("1.0.0.1", 1001), null));
        tasks.put("b", new Task(null, "b", null, null, new InetSocketAddress("1.0.0.1", 1002), null));
        tasks.put("c", new Task(null, "c", null, null, new InetSocketAddress("1.0.0.1", 1003), null));
        when(elasticsearchScheduler.getTasks()).thenReturn(tasks);
        when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getEntity()).thenReturn(new InputStreamEntity(new ByteArrayInputStream("Search result".getBytes())));

        final ResponseEntity<InputStreamResource> search = controller.search("test", chosenNode);
        assertEquals(200, search.getStatusCode().value());

        assertEquals(chosenNode, search.getHeaders().getFirst("X-ElasticSearch-host"));
        final ArgumentCaptor<HttpHost> httpHostArgumentCaptor = ArgumentCaptor.forClass(HttpHost.class);
        final ArgumentCaptor<HttpRequest> httpRequestArgumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).execute(httpHostArgumentCaptor.capture(), httpRequestArgumentCaptor.capture());
        assertEquals("1.0.0.1", httpHostArgumentCaptor.getValue().getHostName());
        assertEquals("/_search?q=test", httpRequestArgumentCaptor.getValue().getRequestLine().getUri());
    }
}