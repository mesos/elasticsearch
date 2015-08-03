package org.apache.mesos.elasticsearch.performancetest;

import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesPerformanceTest extends TestBase {

//    public String slaveHttpAddress;
    @Test
    public void testDataPusherStarted() throws Exception {
        Awaitility.await().atMost(50, TimeUnit.SECONDS).until(new PusherStartedTester());
    }

    private static class PusherStartedTester implements Callable<Boolean> {
        public Boolean call() {
            return false;
        }
    }
}