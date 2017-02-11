package org.apache.mesos.elasticsearch.systemtest.util;

import com.containersol.minimesos.cluster.MesosAgent;
import com.containersol.minimesos.cluster.MesosCluster;
import com.github.dockerjava.api.DockerClient;
import com.jayway.awaitility.Awaitility;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Adds ip table rules to forward all ES traffic to the docker host.
 */
public class IpTables implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IpTables.class);
    public static final String IPTABLES_FINISHED_FLAG = "iptables_finished_flag";
    private final DockerClient client;
    private final Configuration config;
    private final String containerId;

    public static void apply(DockerClient client, MesosCluster cluster, Configuration config) {
        // Install IP tables and reroute traffic from slaves to ports exposed on host.
        for (MesosAgent slave : cluster.getAgents()) {
            LOGGER.debug("Applying iptable redirect to " + slave.getIpAddress());
            Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(30, TimeUnit.SECONDS).until(new IpTables(client, config, slave.getContainerId()));
        }
    }

    private IpTables(DockerClient client, Configuration config, String containerId) {
        this.client = client;
        this.config = config;
        this.containerId = containerId;
    }

    @Override
    public Boolean call() throws Exception {
        String iptablesRoute = config.getPorts().stream().map(ports -> "" +
                        "sudo iptables -t nat -A PREROUTING -p tcp --dport " + ports.client() + " -j DNAT --to-destination " + Configuration.getDocker0AdaptorIpAddress() + ":" + ports.client() + " && " +
                        "sudo iptables -t nat -A PREROUTING -p tcp --dport " + ports.transport() + " -j DNAT --to-destination " + Configuration.getDocker0AdaptorIpAddress() + ":" + ports.transport() + " && "
        ).collect(Collectors.joining(" "));
        client.execCreateCmd(containerId)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withTty(true)
                .withCmd("sh", "-c", "" +
                                "echo 1 > /proc/sys/net/ipv4/ip_forward && " +
                                iptablesRoute +
                                "sudo iptables -t nat -A POSTROUTING -j MASQUERADE  && " +
                                "echo " + IPTABLES_FINISHED_FLAG)
                .exec();

//        final List<String> logs = new ArrayList<>();
//        try {
//            client.execStartCmd(containerId).withTty(true).withExecId(execResponse.getId()).exec(new LogContainerResultCallback() {
//                @Override
//                public void onNext(Frame item) {
//                    LOGGER.info("Install iptables log: " + item.toString());
//                    logs.add(item.toString());
//                }
//            });
//        } catch (Exception e) {
//            LOGGER.error("Could not read log. Retrying.");
//            return false;
//        }

        return true;
    }
}
