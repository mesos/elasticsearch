package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.log4j.Logger;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.docker.DockerUtil;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.concurrent.Callable;
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
    private static final Logger LOGGER = Logger.getLogger(TestBase.class);
    @ClassRule
    public static final MesosCluster cluster = new MesosCluster(config);
    public static final String MESOS_PORT = "5050";
    static String schedulerId;
    static DockerClient docker;

    @BeforeClass
    public static void startScheduler() throws Exception {
        docker = config.dockerClient;

        cluster.injectImage("mesos/elasticsearch-executor");

        String ipAddress = cluster.getMesosContainer().getMesosMasterURL().replace(":" + MESOS_PORT, "");

        final String schedulerImage = "mesos/elasticsearch-scheduler";

        CreateContainerCmd createCommand = docker
                .createContainerCmd(schedulerImage)
                .withExtraHosts(IntStream.rangeClosed(1, config.numberOfSlaves).mapToObj(value -> "slave" + value + ":" + ipAddress).toArray(String[]::new))
                .withCmd("-zk", "zk://" + ipAddress + ":2181/mesos", "-n", "3");

        DockerUtil dockerUtil = new DockerUtil(config.dockerClient);
        schedulerId = dockerUtil.createAndStart(createCommand);


        assertThat(schedulerId, not(isEmptyOrNullString()));
        final String schedulerIp = docker.inspectContainerCmd(schedulerId).exec().getNetworkSettings().getIpAddress();
        await().atMost(60, TimeUnit.SECONDS).until(new SchedulerResponse(schedulerIp));
    }

    private static class SchedulerResponse implements Callable<Boolean> {

      private String schedulerIpAddress;

      public SchedulerResponse(String schedulerIpAddress) {
        this.schedulerIpAddress = schedulerIpAddress;
      }

      @Override
      public Boolean call() throws Exception {
        try {
          return Unirest.get("http://" + schedulerIpAddress + ":8080/tasks").asString().getStatus() == 200;
        } catch (UnirestException e) {
          LOGGER.info("Polling Elasticsearch Scheduler UI on 8080...");
          return false;
        }
      }
    }

    @AfterClass
    public static void stopScheduler() throws Exception {
        docker.removeContainerCmd(schedulerId).withForce().exec();
    }

    protected String getSlaveIp(String slaveName) {
        InspectContainerResponse response = docker.inspectContainerCmd(slaveName).exec();
        return response.getNetworkSettings().getIpAddress();
    }
}
