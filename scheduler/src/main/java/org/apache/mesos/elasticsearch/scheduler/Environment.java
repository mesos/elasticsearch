package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;

/**
 * Delegates system environment to enable testing of Main.
 */
public class Environment {
    public String getJavaHeap() {
        String javaOpts = System.getenv().get(ExecutorEnvironmentalVariables.JAVA_OPTS);
        if (javaOpts == null || javaOpts.isEmpty()) {
            javaOpts = System.getenv().get(ExecutorEnvironmentalVariables.ES_JAVA_OPTS);
        }
        return javaOpts;
    }
}
