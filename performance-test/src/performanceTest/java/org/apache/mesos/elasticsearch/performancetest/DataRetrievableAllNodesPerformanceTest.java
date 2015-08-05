package org.apache.mesos.elasticsearch.performancetest;

import com.jayway.awaitility.Awaitility;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import java.io.InputStream;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesPerformanceTest extends TestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesPerformanceTest.class);

    @Test
    public void testAllNodesContainData() throws Exception {
        LOGGER.info("Addresses:");
        LOGGER.info(getSlavesElasticAddresses());
        JSONArray responseElements;
        for (String httpAddress: getSlavesElasticAddresses()) {
            try {
                responseElements = Unirest.get("http://" + httpAddress + "/_count").asJson().getBody().getArray();
            } catch (Exception e) {
                LOGGER.error("Unirest exception:" + e.getMessage());
                throw e;
            }
            LOGGER.info("Fetched " + httpAddress + ". Response:");
            LOGGER.info(responseElements);
            TestCase.assertEquals(responseElements.getJSONObject(0).getInt("count"), 999);
        }
    }
}