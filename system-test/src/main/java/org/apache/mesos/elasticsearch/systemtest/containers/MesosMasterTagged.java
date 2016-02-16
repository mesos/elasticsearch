package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosMasterExtended;
import com.containersol.minimesos.mesos.ZooKeeper;
import org.apache.mesos.elasticsearch.systemtest.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A tagged version of MesosMaster
 */
public class MesosMasterTagged extends MesosMasterExtended {
    private static final Logger LOG = LoggerFactory.getLogger(MesosMasterTagged.class);
    public MesosMasterTagged(ZooKeeper zooKeeperContainer) {
        super(DockerClientFactory.build(), zooKeeperContainer, MESOS_MASTER_IMAGE, Configuration.MESOS_IMAGE_TAG, Collections.emptyMap(), true);
        LOG.debug("Using image: " + MESOS_MASTER_IMAGE + ":" + Configuration.MESOS_IMAGE_TAG);
    }

    public MesosMasterTagged(ZooKeeper zooKeeperContainer, Map<String, String> extraEnvVars) {
        super(DockerClientFactory.build(), zooKeeperContainer, MESOS_MASTER_IMAGE, Configuration.MESOS_IMAGE_TAG, extraEnvVars, true);
        LOG.debug("Using image: " + MESOS_MASTER_IMAGE + ":" + Configuration.MESOS_IMAGE_TAG);
    }
}
