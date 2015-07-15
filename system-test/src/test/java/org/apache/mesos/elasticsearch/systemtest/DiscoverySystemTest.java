package org.apache.mesos.elasticsearch.systemtest;

import com.jayway.awaitility.core.ConditionTimeoutException;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.jayway.awaitility.Awaitility.await;
import static junit.framework.Assert.fail;
import static org.hamcrest.CoreMatchers.is;

/**
 * Tests REST node discovery
 */
public class DiscoverySystemTest extends TestBase {

    public static final Logger LOGGER = Logger.getLogger(DiscoverySystemTest.class);

    @Test
    public void testNodeDiscoveryRest() throws InterruptedException {
        String slave1Ip = getSlaveIp(cluster.getMesosContainer().getMesosContainerID());

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(slave1Ip, slave1Ip, slave1Ip);

        assertNodesDiscovered(nodesResponse);
    }

    private void assertNodesDiscovered(ElasticsearchNodesResponse nodesResponse) {
        try {
            await().atMost(5, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS).until(nodesResponse, is(true));
        } catch (ConditionTimeoutException e) {
            fail("Elasticsearch did not discover nodes within 5 minutes");
        }
        LOGGER.info("Elasticsearch nodes discovered each other successfully");
    }

    private static class ElasticsearchNodesResponse implements Callable<Boolean> {

        private String ipAddress1;
        private String ipAddress2;
        private String ipAddress3;

        public ElasticsearchNodesResponse(String ipAddress1, String ipAddress2, String ipAddress3) {
            this.ipAddress1 = ipAddress1;
            this.ipAddress2 = ipAddress2;
            this.ipAddress3 = ipAddress3;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                //todo: get ES urls from Scheduler/tasks API
                double nodesSlave1 = Unirest.get("http://" + ipAddress1 + ":9200/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length();
                double nodesSlave2 = Unirest.get("http://" + ipAddress2 + ":9201/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length();
                double nodesSlave3 = Unirest.get("http://" + ipAddress3 + ":9202/_nodes").asJson().getBody().getObject().getJSONObject("nodes").length();
                if (!(nodesSlave1 == 3 && nodesSlave2 == 3 && nodesSlave3 == 3)) {
                    return false;
                }
            } catch (UnirestException e) {
                LOGGER.info("Polling Elasticsearch on port 9200...");
                return false;
            }
            return true;
        }
    }

}
