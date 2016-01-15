package org.apache.mesos.elasticsearch.systemtest;

import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.SchedulerTestBase;
import org.apache.mesos.elasticsearch.systemtest.containers.DataPusherContainer;
import org.junit.Test;

import java.util.List;


/**
 * Tests scheduler APIs
 */
public class DataRetrievableAllNodesSystemTest extends SchedulerTestBase {

    private static final Logger LOGGER = Logger.getLogger(DataRetrievableAllNodesSystemTest.class);

    private DataPusherContainer pusher;

    @Test
    public void testDataConsistency() throws Exception {
        ESTasks esTasks = new ESTasks(TEST_CONFIG, getScheduler().getIpAddress(), true);
        esTasks.waitForGreen();

        List<String> esAddresses = esTasks.getEsHttpAddressList();
        pusher = new DataPusherContainer(CLUSTER_ARCHITECTURE.dockerClient, esAddresses.get(0));
        CLUSTER.addAndStartContainer(pusher, TEST_CONFIG.getClusterTimeout());

        LOGGER.info("Addresses:");
        LOGGER.info(esAddresses);
        esTasks.waitForCorrectDocumentCount(DataPusherContainer.CORRECT_NUM_DOCS);
    }
}