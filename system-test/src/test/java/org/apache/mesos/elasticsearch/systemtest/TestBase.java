package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.*;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * Base test class for everything shared between test cases
 */
public abstract class TestBase {
    protected static final MesosClusterConfig config = MesosClusterConfig.builder()
            .numberOfSlaves(3)
            .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
            .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
            .build();

    @ClassRule
    public static MesosCluster cluster = new MesosCluster(config);

    private static final Logger LOGGER = Logger.getLogger(TestBase.class);
    protected static String schedulerId, schedulerURL;
    static DockerClient docker;

    protected static String getSlaveIp(String slaveName) {
        InspectContainerResponse response = docker.inspectContainerCmd(slaveName).exec();
        return response.getNetworkSettings().getIpAddress();
    }

    @BeforeClass
    public static void startScheduler() throws Exception {
        docker = config.dockerClient;

        final String schedulerImage = "mesos/elasticsearch-scheduler";

        CreateContainerCmd createCommand = docker
                .createContainerCmd(schedulerImage)
                .withExtraHosts(IntStream.rangeClosed(1, config.numberOfSlaves).mapToObj(value -> "slave" + value + ":" + cluster.getMesosContainer().getMesosMasterIP()).toArray(String[]::new))
//                .withEnv("JAVA_TOOL_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005").withPortBindings(PortBinding.parse("0.0.0.0:5005:5005"))
                .withCmd("-zk", "zk://" + cluster.getMesosContainer().getMesosMasterIP() + ":2181/mesos", "-n", "3");

        final CreateContainerResponse createSchedulerResponse = createCommand.exec();
        schedulerId = createSchedulerResponse.getId();
        assertThat(schedulerId, not(isEmptyOrNullString()));
        docker.startContainerCmd(schedulerId).exec();
        schedulerURL = "http://" + getSlaveIp(schedulerId) + ":8080";

        assertThat(schedulerId, not(isEmptyOrNullString()));
        final String schedulerIp = docker.inspectContainerCmd(schedulerId).exec().getNetworkSettings().getIpAddress();
        await().atMost(20, TimeUnit.SECONDS).until(() -> Unirest.get("http://" + schedulerIp + ":8080/tasks").asString().getStatus() == 200);
    }


    @AfterClass
    public static void stopScheduler() throws Exception {
        docker.removeContainerCmd(schedulerId).withForce().exec();
    }
}
