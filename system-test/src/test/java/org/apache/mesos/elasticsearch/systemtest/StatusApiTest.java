package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StatusApiTest extends TestBase {

    @Test
    public void canGet200FromScheduler() throws Exception {
        String schedulerIp = getSlaveIp("mesoses_scheduler_1");
        HttpResponse<JsonNode> tasksResponse = Unirest.get("http://" + schedulerIp + ":8080/tasks").asJson();
        assertEquals(200, tasksResponse.getStatus());
    }
}
