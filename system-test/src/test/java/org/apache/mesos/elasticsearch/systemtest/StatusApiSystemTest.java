package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.json.JSONObject;
import org.junit.Test;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class StatusApiSystemTest extends TestBase {

    @Test
    public void canGet200FromScheduler() throws Exception {
        String schedulerIp = getSlaveIp("mesoses_scheduler_1");
        HttpResponse<JsonNode> tasksResponse = Unirest.get("http://" + schedulerIp + ":8080/tasks").asJson();
        assertEquals(200, tasksResponse.getStatus());
    }

    @Test
    public void hasThreeTasksWithValidInformation() throws Exception {
        String schedulerIp = getSlaveIp("mesoses_scheduler_1");
        HttpResponse<JsonNode> tasksResponse = Unirest.get("http://" + schedulerIp + ":8080/tasks").asJson();

        assertEquals(3, tasksResponse.getBody().getArray().length());

        for (int i = 0; i < tasksResponse.getBody().getArray().length(); i++) {
            JSONObject taskObject = tasksResponse.getBody().getArray().getJSONObject(i);
            assertThat(taskObject.getString("id"), startsWith("elasticsearch_slave"));
            assertEquals("esdemo", taskObject.getString("name"));
            assertThat(taskObject.getString("started_at"), SystemTestMatchers.isValidDateTime());
            assertThat(taskObject.getString("http_address"), SystemTestMatchers.isValidAddress());
            assertThat(taskObject.getString("transport_address"), SystemTestMatchers.isValidAddress());
            assertThat(taskObject.getString("hostname"), startsWith("slave"));
        }
    }

}
