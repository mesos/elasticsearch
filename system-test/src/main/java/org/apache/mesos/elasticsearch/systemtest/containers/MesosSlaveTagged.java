package org.apache.mesos.elasticsearch.systemtest.containers;

import com.containersol.minimesos.mesos.DockerClientFactory;
import com.containersol.minimesos.mesos.MesosSlaveExtended;
import com.containersol.minimesos.mesos.ZooKeeper;
import org.apache.mesos.elasticsearch.systemtest.Configuration;

/**
 * A tagged version of MesosSlave
 */
public class MesosSlaveTagged extends MesosSlaveExtended {
    public MesosSlaveTagged(ZooKeeper zooKeeperContainer, String resources) {
        super(DockerClientFactory.build(), resources, Integer.toString(MESOS_SLAVE_PORT), zooKeeperContainer, MESOS_SLAVE_IMAGE, Configuration.MESOS_IMAGE_TAG);
    }
}
