package org.apache.mesos.elasticsearch.performancetest;

import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesTest extends TestBase {

    public String slaveHttpAddress;
    @Test
    public void testDataRetrievable() throws Exception {

        try {
            TasksResponse tasksResponse = new TasksResponse(getScheduler().getIpAddress());
            JSONObject taskObject = tasksResponse.getJson().getBody().getArray().getJSONObject(0);
            slaveHttpAddress = taskObject.getString("http_address");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        DataPusherContainer pusher = new DataPusherContainer(CONFIG.dockerClient, slaveHttpAddress);
        pusher.start();

        TasksResponse tasksResponse = new TasksResponse(getScheduler().getIpAddress());


        for (int i = 0; i < tasksResponse.getJson().getBody().getArray().length(); i++) {
            JSONObject taskObject = tasksResponse.getJson().getBody().getArray().getJSONObject(i);
            // todo: retrieve data from all nodes
        }
    }
}
