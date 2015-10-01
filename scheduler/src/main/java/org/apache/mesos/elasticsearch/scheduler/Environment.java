package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;

/**
 * Delegates system environment to enable testing of Main.
 */
public class Environment {
    public String getJavaHeap() {
        return System.getenv().get(ExecutorEnvironmentalVariables.JAVA_OPTS);
    }
}
