package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.state.State;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.systemtest.base.TestBase;
import org.apache.mesos.elasticsearch.systemtest.callbacks.ElasticsearchNodesResponse;
import org.apache.mesos.elasticsearch.systemtest.containers.ElasticsearchSchedulerContainer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests configuration of framework roles
 */
@Ignore("This test has to be merged into DeploymentSystemTest. See https://github.com/mesos/elasticsearch/issues/591")
public class FrameworkRoleSystemTest extends TestBase {
    public static final Logger LOGGER = Logger.getLogger(FrameworkRoleSystemTest.class);

    @Test
    public void miniMesosReportsFrameworkRoleStar() throws UnirestException, JsonParseException, JsonMappingException {
        testMiniMesosReportsFrameworkRole("*");
    }

    // TODO (pnw): Need to rewrite the whole test to use a custom slave with specified roles.
    @Ignore
    @Test
    public void miniMesosReportsFrameworkRoleOther() throws UnirestException, JsonParseException, JsonMappingException {
        testMiniMesosReportsFrameworkRole("foobar");
    }

    private void testMiniMesosReportsFrameworkRole(String role) throws UnirestException, JsonParseException, JsonMappingException {
        LOGGER.info("Starting Elasticsearch scheduler with framework role: " + role);
        ElasticsearchSchedulerContainer scheduler = new ElasticsearchSchedulerContainer(getDockerClient(),
                CLUSTER.getZooKeeper().getIpAddress(),
                role,
                org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR);
        CLUSTER.addAndStartProcess(scheduler, TEST_CONFIG.getClusterTimeout());
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + getTestConfig().getSchedulerGuiPort());

        ESTasks esTasks = new ESTasks(TEST_CONFIG, scheduler.getIpAddress());
        new TasksResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());
        new ElasticsearchNodesResponse(esTasks, TEST_CONFIG.getElasticsearchNodesCount());

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.SECONDS).until(() -> getStateInfo(CLUSTER).getFramework("elasticsearch") != null);
        Assert.assertEquals(role, getStateInfo(CLUSTER).getFramework("elasticsearch").getRole());
    }

    public State getStateInfo(MesosCluster cluster) throws UnirestException, JsonParseException, JsonMappingException {
        return State.fromJSON(cluster.getClusterStateInfo().toString(2));
    }

}
