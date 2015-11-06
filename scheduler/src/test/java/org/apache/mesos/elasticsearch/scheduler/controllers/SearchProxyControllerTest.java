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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the search proxy, Sherlock
 */
@RunWith(MockitoJUnitRunner.class)
public class SearchProxyControllerTest {

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    public static final String HOSTNAME = "1.0.0.1";
    @Mock
    HttpClient httpClient;

    @Mock
    ElasticsearchScheduler elasticsearchScheduler;

    @Mock
    HttpResponse httpResponse;

    @InjectMocks
    SearchProxyController controller;

    private Map<String, Task> createTasksMap(int nodes) {
        return IntStream.rangeClosed(1, nodes)
                .mapToObj(value -> new Task(HOSTNAME, "task-" + value, null, null, new InetSocketAddress(HOSTNAME, 1000 + value), null))
                .collect(Collectors.toMap(Task::getTaskId, task -> task));
    }

    @Test
    public void willForwardSearchRequestToARandomNode() throws Exception {
        when(elasticsearchScheduler.getTasks()).thenReturn(createTasksMap(3));
        when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getEntity()).thenReturn(new InputStreamEntity(new ByteArrayInputStream("Search result".getBytes("UTF-8"))));

        final ResponseEntity<InputStreamResource> search = controller.search("test", null);
        assertEquals(200, search.getStatusCode().value());

        final ArgumentCaptor<HttpHost> httpHostArgumentCaptor = ArgumentCaptor.forClass(HttpHost.class);
        final ArgumentCaptor<HttpRequest> httpRequestArgumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).execute(httpHostArgumentCaptor.capture(), httpRequestArgumentCaptor.capture());
        assertEquals(HOSTNAME, httpHostArgumentCaptor.getValue().getHostName());
        assertEquals("/_search?q=test", httpRequestArgumentCaptor.getValue().getRequestLine().getUri());
    }

    @Test
    public void willForwardSearchRequestToAChosenNode() throws Exception {
        final String chosenNode = "1.0.0.1:1002";

        when(elasticsearchScheduler.getTasks()).thenReturn(createTasksMap(3));
        when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenReturn(httpResponse);
        when(httpResponse.getEntity()).thenReturn(new InputStreamEntity(new ByteArrayInputStream("Search result".getBytes("UTF-8"))));

        final ResponseEntity<InputStreamResource> search = controller.search("test", chosenNode);
        assertEquals(200, search.getStatusCode().value());

        assertEquals(chosenNode, search.getHeaders().getFirst("X-ElasticSearch-host"));
        final ArgumentCaptor<HttpHost> httpHostArgumentCaptor = ArgumentCaptor.forClass(HttpHost.class);
        final ArgumentCaptor<HttpRequest> httpRequestArgumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).execute(httpHostArgumentCaptor.capture(), httpRequestArgumentCaptor.capture());
        assertEquals(HOSTNAME, httpHostArgumentCaptor.getValue().getHostName());
        assertEquals("/_search?q=test", httpRequestArgumentCaptor.getValue().getRequestLine().getUri());
    }
}