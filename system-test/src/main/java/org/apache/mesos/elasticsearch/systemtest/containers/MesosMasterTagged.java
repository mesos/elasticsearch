package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.config.MesosMasterConfig;
import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosMaster;
import com.containersol.minimesos.mesos.ZooKeeper;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A tagged version of MesosMaster
 */
public class MesosMasterTagged extends MesosMaster {

    private static final Logger LOG = LoggerFactory.getLogger(MesosMasterTagged.class);

    private final Map<String, String> extraEnvironmentVariables = new HashMap<>();

    public MesosMasterTagged(ZooKeeper zooKeeperContainer) {
        super(DockerClientFactory.build(), zooKeeperContainer, masterConfig(MesosMasterConfig.MESOS_MASTER_IMAGE, Configuration.MESOS_IMAGE_TAG));
        LOG.debug("Using image: " + MesosMasterConfig.MESOS_MASTER_IMAGE + ":" + Configuration.MESOS_IMAGE_TAG);
    }

    public MesosMasterTagged(ZooKeeper zooKeeperContainer, Map<String, String> extraEnvVars) {
        super(DockerClientFactory.build(), zooKeeperContainer, masterConfig(MesosMasterConfig.MESOS_MASTER_IMAGE, Configuration.MESOS_IMAGE_TAG));
        extraEnvironmentVariables.putAll(extraEnvVars);
        LOG.debug("Using image: " + MesosMasterConfig.MESOS_MASTER_IMAGE + ":" + Configuration.MESOS_IMAGE_TAG);
    }

    private static MesosMasterConfig masterConfig(String imageName, String imageTag) {
        MesosMasterConfig masterConfig = new MesosMasterConfig();
        masterConfig.setImageName(imageName);
        masterConfig.setImageTag(imageTag);
        return masterConfig;
    }

    @Override
    protected String[] createMesosLocalEnvironment() {
        TreeMap<String, String> envs = getDefaultEnvVars();

        envs.putAll(this.extraEnvironmentVariables);
        envs.putAll(getSharedEnvVars());
        return envs.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toArray(String[]::new);
    }

}
