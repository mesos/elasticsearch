package org.apache.mesos.elasticsearch.systemtest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests configuration of framework roles
 */
public class FrameworkRoleSystemTest extends TestBase {
    public static final Logger LOGGER = Logger.getLogger(FrameworkRoleSystemTest.class);

    @Test
    public void miniMesosReportsFrameworkRoleStar() throws UnirestException, JsonParseException, JsonMappingException {
        testMiniMesosReportsFrameworkRole("*");
    }

    // TODO (pnw): Minimesos regression, this does not work. Enable when fixed.
    @Ignore
    @Test
    public void miniMesosReportsFrameworkRoleOther() throws UnirestException, JsonParseException, JsonMappingException {
        testMiniMesosReportsFrameworkRole("foobar");
    }

    private void testMiniMesosReportsFrameworkRole(String role) throws UnirestException, JsonParseException, JsonMappingException {
        LOGGER.info("Starting Elasticsearch scheduler with framework role: " + role);
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(
                CLUSTER.getConfig().dockerClient,
                CLUSTER.getZkContainer().getIpAddress(),
                role,
                CLUSTER);
        CLUSTER.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, CLUSTER.getConfig().getNumberOfSlaves());
        new ElasticsearchNodesResponse(esTasks, CLUSTER.getConfig().getNumberOfSlaves());

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.SECONDS).until(() -> CLUSTER.getStateInfo().getFramework("elasticsearch") != null);
        Assert.assertEquals(role, CLUSTER.getStateInfo().getFramework("elasticsearch").getRole());
    }
}
