package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosMasterExtended;
import com.containersol.minimesos.mesos.ZooKeeper;
import org.apache.mesos.elasticsearch.systemtest.Configuration;

import java.util.Collections;
import java.util.Map;

/**
 * A tagged version of MesosMaster
 */
public class MesosMasterTagged extends MesosMasterExtended {
    public MesosMasterTagged(ZooKeeper zooKeeperContainer) {
        super(DockerClientFactory.build(), zooKeeperContainer, MESOS_MASTER_IMAGE, Configuration.MESOS_IMAGE_TAG, Collections.emptyMap(), true);
    }

    public MesosMasterTagged(ZooKeeper zooKeeperContainer, Map<String, String> extraEnvVars) {
        super(DockerClientFactory.build(), zooKeeperContainer, MESOS_MASTER_IMAGE, Configuration.MESOS_IMAGE_TAG, extraEnvVars, true);
    }
}
