package org.apache.mesos.elasticsearch.performancetest;

import com.jayway.awaitility.Awaitility;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.junit.Test;
import com.mashape.unirest.http.Unirest;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesPerformanceTest extends TestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesPerformanceTest.class);

    private static class FetchAllNodesData implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            JSONArray responseElements;
            for (String httpAddress: getSlavesElasticAddresses()) {
                try {
                    responseElements = Unirest.get("http://" + httpAddress + "/_count").asJson().getBody().getArray();
                } catch (Exception e) {
                    LOGGER.error("Unirest exception:" + e.getMessage());
                    return false;
                }
                LOGGER.info(responseElements);
                if (responseElements.getJSONObject(0).getInt("count") != 9) {
                    return false;
                }
            }
            return true;
        }
    }

    @Test
    public void testAllNodesContainData() throws Exception {
        LOGGER.info("Addresses:");
        LOGGER.info(getSlavesElasticAddresses());
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(2, TimeUnit.SECONDS).until(new FetchAllNodesData());
    }
}