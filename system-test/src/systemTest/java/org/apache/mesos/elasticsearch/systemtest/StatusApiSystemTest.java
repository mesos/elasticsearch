package org.apache.mesos.elasticsearch.systemtest;

import org.apache.mesos.elasticsearch.common.elasticsearch.ElasticsearchParser;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.json.JSONObject;
import org.junit.Test;

import static org.apache.mesos.elasticsearch.systemtest.util.SystemTestMatchers.isValidAddress;
import static org.apache.mesos.elasticsearch.systemtest.util.SystemTestMatchers.isValidDateTime;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests scheduler APIs
 */
public class StatusApiSystemTest extends SchedulerTestBase {

    @Test
    public void testThreeTasks() throws Exception {
        TasksResponse tasksResponse = new TasksResponse(new ESTasks(TEST_CONFIG, getScheduler().getIpAddress(), true), TEST_CONFIG.getElasticsearchNodesCount());

        assertEquals(((Integer) TEST_CONFIG.getElasticsearchNodesCount()).intValue(), tasksResponse.getJson().getBody().getArray().length());

        for (int i = 0; i < tasksResponse.getJson().getBody().getArray().length(); i++) {
            JSONObject taskObject = tasksResponse.getJson().getBody().getArray().getJSONObject(i);
            assertThat(taskObject.getString("id"), startsWith("elasticsearch_"));
            assertEquals(getTestConfig().getElasticsearchJobName(), taskObject.getString("name"));
            assertThat(taskObject.getString("started_at"), isValidDateTime());
            assertThat(ElasticsearchParser.parseHttpAddress(taskObject), isValidAddress());
            assertThat(taskObject.getString("transport_address"), isValidAddress());
        }
    }

}
