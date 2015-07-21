package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;

import static org.apache.mesos.elasticsearch.systemtest.SystemTestMatchers.isValidAddress;
import static org.apache.mesos.elasticsearch.systemtest.SystemTestMatchers.isValidDateTime;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class StatusApiSystemTest extends TestBase {

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);

    @Test
    public void canGet200FromScheduler() throws Exception {
        String schedulerIp = getSlaveIp(schedulerId);
        assertThat(schedulerIp, not(isEmptyOrNullString()));
        final HttpResponse<String> tasksResponse = Unirest.get("http://" + schedulerIp + ":8080/v1/tasks").asString();
        assertEquals(200, tasksResponse.getStatus());
    }

    @Test
    public void hasThreeTasksWithValidInformation() throws Exception {
        String schedulerIp = getSlaveIp(schedulerId);

        TasksResponse tasksResponse = new TasksResponse(schedulerIp);
        await().atMost(60, TimeUnit.SECONDS).until(tasksResponse);

        assertEquals(3, tasksResponse.getJson().getBody().getArray().length());

        for (int i = 0; i < tasksResponse.getJson().getBody().getArray().length(); i++) {
            JSONObject taskObject = tasksResponse.getJson().getBody().getArray().getJSONObject(i);
            assertThat(taskObject.getString("id"), startsWith("elasticsearch_slave"));
            assertEquals("esdemo", taskObject.getString("name"));
            assertThat(taskObject.getString("started_at"), isValidDateTime());
            assertThat(taskObject.getString("http_address"), isValidAddress());
            assertThat(taskObject.getString("transport_address"), isValidAddress());
            assertThat(taskObject.getString("hostname"), startsWith("slave"));
        }
    }

    private static class TasksResponse implements Callable<Boolean> {

        private String schedulerIpAddress;

        private HttpResponse<JsonNode> response;

        public TasksResponse(String schedulerIpAddress) {
            this.schedulerIpAddress = schedulerIpAddress;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                response = Unirest.get("http://" + schedulerIpAddress + ":8080/v1/tasks").asJson();
                return response.getBody().getArray().length() == 3;
            } catch (UnirestException e) {
                LOGGER.info("Waiting until 3 tasks are a started...");
                return false;
            }
        }

        public HttpResponse<JsonNode> getJson() {
            return response;
        }
    }

}
