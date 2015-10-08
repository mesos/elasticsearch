package org.apache.mesos.elasticsearch.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.scheduler.Configuration;
import org.apache.mesos.mini.MesosCluster;
import org.apache.mesos.mini.mesos.MesosClusterConfig;
import org.json.JSONObject;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

/**
 * A system test to ensure that the framework can run as a JAR, not using docker.
 */
public class RunAsJarSystemTest {
    @ClassRule
    public static final MesosCluster CLUSTER = new MesosCluster(
            MesosClusterConfig.builder()
                    .numberOfSlaves(3)
                    .privateRegistryPort(15000) // Currently you have to choose an available port by yourself
                    .slaveResources(new String[]{"ports(*):[9200-9200,9300-9300]", "ports(*):[9201-9201,9301-9301]", "ports(*):[9202-9202,9302-9302]"})
                    .build()
    );
    private static final ContainerLifecycleManagement CONTAINER_MANAGER = new ContainerLifecycleManagement();

    @After
    public void stopContainer() {
        CONTAINER_MANAGER.stopAll();
    }

    @Test
    public void shouldStartScheduler() {
        JarScheduler scheduler = new JarScheduler(CLUSTER.getConfig().dockerClient, CLUSTER.getMesosContainer().getIpAddress());
        CONTAINER_MANAGER.addAndStart(scheduler);
        TasksResponse tasksResponse = new TasksResponse(scheduler.getIpAddress(), CLUSTER.getConfig().getNumberOfSlaves());

        List<JSONObject> tasks = tasksResponse.getTasks();

        ElasticsearchNodesResponse nodesResponse = new ElasticsearchNodesResponse(tasks, CLUSTER.getConfig().getNumberOfSlaves());
        assertTrue("Elasticsearch nodes did not discover each other within 5 minutes", nodesResponse.isDiscoverySuccessful());
    }

    private class JarScheduler extends ElasticsearchSchedulerContainer {
        protected JarScheduler(DockerClient dockerClient, String mesosIp) {
            super(dockerClient, mesosIp);
        }

        @Override
        protected CreateContainerCmd dockerCommand() {
            return dockerClient
                    .createContainerCmd(SCHEDULER_IMAGE)
                    .withName(SCHEDULER_NAME + "_" + new SecureRandom().nextInt())
                    .withEnv("JAVA_OPTS=-Xms128m -Xmx256m")
                    .withExtraHosts(IntStream.rangeClosed(1, 3).mapToObj(value -> "slave" + value + ":" + mesosIp).toArray(String[]::new))
                    .withCmd(
                            ZookeeperCLIParameter.ZOOKEEPER_MESOS_URL, getZookeeperMesosUrl(),
                            ElasticsearchCLIParameter.ELASTICSEARCH_NODES, "3",
                            Configuration.ELASTICSEARCH_RAM, "256",
                            Configuration.FRAMEWORK_USE_DOCKER, "false"
                    );
        }
    }
}
