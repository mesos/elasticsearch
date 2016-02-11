package org.apache.mesos.elasticsearch.systemtest.base;

import com.containersol.minimesos.MesosCluster;
import com.containersol.minimesos.mesos.*;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.jayway.awaitility.Awaitility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.apache.mesos.elasticsearch.systemtest.containers.AlpineContainer;
import org.apache.mesos.elasticsearch.systemtest.util.DockerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base test class which launches Mesos CLUSTER
 */
@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", "MS_PKGPROTECT", "MS_CANNOT_BE_FINAL"})
public abstract class TestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);
    protected static final Configuration TEST_CONFIG = new Configuration();
    public static final String MESOS_IMAGE_TAG = "0.25.0-0.2.70.ubuntu1404";

    protected static ClusterArchitecture CLUSTER_ARCHITECTURE;

    protected static MesosCluster CLUSTER;

    @ClassRule
    public static final TestWatcher WATCHER = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            CLUSTER.stop();
        }
    };

    @BeforeClass
    public static void reinitialiseCluster() {
        CLUSTER_ARCHITECTURE = new ClusterArchitecture.Builder()
                .withZooKeeper()
                .withMaster(MesosMasterTagged::new)
                .withSlave(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(0)))
                .withSlave(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(1)))
                .withSlave(zooKeeper -> new MesosSlaveTagged(zooKeeper, TEST_CONFIG.getPortRanges().get(2)))
                .build();
        CLUSTER = new MesosCluster(CLUSTER_ARCHITECTURE);
        CLUSTER.start(60);

        // Install IP tables and reroute traffic from slaves to ports exposed on host.
        for (MesosSlave slave : CLUSTER.getSlaves()) {
            LOGGER.debug("Applying iptable redirect to " + slave.getIpAddress());
            Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS).until(new ApplyIptables(slave.getContainerId()));
        }
    }

    private static class ApplyIptables implements Callable<Boolean> {
        public static final String IPTABLES_FINISHED_FLAG = "iptables_finished_flag";
        private final String containerId;

        private ApplyIptables(String containerId) {
            this.containerId = containerId;
        }

        @Override
        public Boolean call() throws Exception {
            String iptablesRoute = TEST_CONFIG.getPorts().stream().map(ports -> "" +
                            "sudo iptables -t nat -A PREROUTING -p tcp --dport " + ports.client() + " -j DNAT --to-destination 172.17.0.1:" + ports.client() + " ; " +
                            "sudo iptables -t nat -A PREROUTING -p tcp --dport " + ports.transport() + " -j DNAT --to-destination 172.17.0.1:" + ports.transport() + " ; "
            ).collect(Collectors.joining(" "));
            ExecCreateCmdResponse execResponse = CLUSTER_ARCHITECTURE.dockerClient.execCreateCmd(containerId)
                    .withAttachStdout()
                    .withAttachStderr()
                    .withTty(true)
                    .withCmd("sh", "-c", "" +
                                    "echo 1 > /proc/sys/net/ipv4/ip_forward ; " +
                                    iptablesRoute +
                                    "sudo iptables -t nat -A POSTROUTING -j MASQUERADE  ; " +
                                    "echo " + IPTABLES_FINISHED_FLAG
                    ).exec();
            try (InputStream inputstream = CLUSTER_ARCHITECTURE.dockerClient.execStartCmd(containerId).withTty().withExecId(execResponse.getId()).exec()) {
                String log = IOUtils.toString(inputstream, "UTF-8");
                LOGGER.info("Install iptables log: " + log);
                return log.contains(IPTABLES_FINISHED_FLAG);
            } catch (IOException e) {
                LOGGER.error("Could not read log. Retrying.");
                return false;
            }
        }
    }

    @BeforeClass
    public static void prepareCleanDockerEnvironment() {
        new DockerUtil(CLUSTER_ARCHITECTURE.dockerClient).killAllSchedulers();
        new DockerUtil(CLUSTER_ARCHITECTURE.dockerClient).killAllExecutors();

        // Completely wipe out old data dir, just in case there is any old data in there.
        String dataDir = org.apache.mesos.elasticsearch.scheduler.Configuration.DEFAULT_HOST_DATA_DIR;
        AlpineContainer alpineContainer = new AlpineContainer(CLUSTER_ARCHITECTURE.dockerClient, dataDir, dataDir, new String[]{"sh", "-c", "rm -rf " + dataDir + "/* ; sleep 9999 ;"});
        alpineContainer.start(TEST_CONFIG.getClusterTimeout());
        alpineContainer.remove();
    }

    @AfterClass
    public static void killAllContainers() {
        CLUSTER.stop();
        new DockerUtil(CLUSTER_ARCHITECTURE.dockerClient).killAllExecutors();
    }

    public static Configuration getTestConfig() {
        return TEST_CONFIG;
    }

    private static class MesosMasterTagged extends MesosMasterExtended {
        public MesosMasterTagged(ZooKeeper zooKeeperContainer) {
            super(DockerClientFactory.build(), zooKeeperContainer, MESOS_MASTER_IMAGE, TestBase.MESOS_IMAGE_TAG, Collections.emptyMap(), true);
        }
    }

    private static class MesosSlaveTagged extends MesosSlaveExtended {
        public MesosSlaveTagged(ZooKeeper zooKeeperContainer, String resources) {
            super(DockerClientFactory.build(), resources, Integer.toString(MESOS_SLAVE_PORT), zooKeeperContainer, MESOS_SLAVE_IMAGE, TestBase.MESOS_IMAGE_TAG);
        }
    }
}
