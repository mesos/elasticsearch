package org.apache.mesos.elasticsearch.systemtest.util;

import com.containersol.minimesos.container.AbstractContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple class to monitor lifecycle of scheduler container.
 */
public class ContainerLifecycleManagement {
    private List<AbstractContainer> containers = new ArrayList<>();
    public void addAndStart(AbstractContainer container) {
        container.start();
        containers.add(container);
    }

    public void stopContainer(AbstractContainer container) {
        container.remove();
        containers.remove(container);
    }

    public void stopAll() {
        containers.forEach(AbstractContainer::remove);
        containers.clear();
    }
}
