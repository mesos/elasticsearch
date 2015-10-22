package org.apache.mesos.elasticsearch.systemtest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.jayway.awaitility.Awaitility;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.MesosClusterConfig;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.*;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Tests configuration of framework roles
 */
public class FrameworkRoleSystemTest {
    protected static final Configuration TEST_CONFIG = new Configuration();

    protected static final MesosClusterConfig CONFIG = MesosClusterConfig.builder()
            .mesosImageTag(Main.MESOS_IMAGE_TAG)
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .extraEnvironmentVariables(new TreeMap<String, String>(){{
                this.put("MESOS_ROLES", "*,foobar");
                }})
            .build();

    public static final Logger LOGGER = Logger.getLogger(FrameworkRoleSystemTest.class);

    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(CONFIG);

    @AfterClass
    public static void killContainers() throws IOException {
        CLUSTER.stop();
        new DockerUtil(CLUSTER.getConfig().dockerClient).killAllExecutors();
    }

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
                role
        );
        CLUSTER.addAndStartContainer(scheduler);
        LOGGER.info("Started Elasticsearch scheduler on " + scheduler.getIpAddress() + ":" + TEST_CONFIG.getSchedulerGuiPort());

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(5, TimeUnit.SECONDS).until(() -> CLUSTER.getStateInfo().getFramework("elasticsearch") != null);
        Assert.assertEquals(role, CLUSTER.getStateInfo().getFramework("elasticsearch").getRole());
    }
}
