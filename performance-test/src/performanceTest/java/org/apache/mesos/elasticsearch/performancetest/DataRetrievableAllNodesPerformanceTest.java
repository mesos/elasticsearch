package org.apache.mesos.elasticsearch.performancetest;

import com.jayway.awaitility.Awaitility;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.io.InputStream;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesPerformanceTest extends TestBase {

//    public String slaveHttpAddress;
    @Test
    public void testDataPusherStarted() throws Exception {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(new PusherStartedTester());
    }

    @Test
    public void testAllNodesContainData() throws Exception {
        TestCase.assertFalse(true);
    }

    private static class PusherStartedTester implements Callable<Boolean> {
        public Boolean call() throws Exception {
            InputStream exec = getPusher().getLogStreamStdOut();

            String log = IOUtils.toString(exec);
            if (log.contains("riemann.elastic - elasticized")) {
                return true;
            }

            return false;
        }
    }
}