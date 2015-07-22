package org.apache.mesos.elasticsearch.scheduler.configuration;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Environmental variables for the executor
 */
public class ExecutorEnvironmentalVariables {
    private static final String native_mesos_library_key = "MESOS_NATIVE_JAVA_LIBRARY";
    private static final String native_mesos_library_path = "/usr/lib/libmesos.so"; // libmesos.so is usually symlinked to the version.
    public static final String JAVA_OPTS = "JAVA_OPTS";
    private final List<Protos.Environment.Variable> envList = new ArrayList<>();

    /**
     * @param configuration The mesos cluster configuration
     */
    public ExecutorEnvironmentalVariables(Configuration configuration) {
        populateEnvMap(configuration);
    }

    /**
     * Get a list of environmental variables
     * @return
     */
    public List<Protos.Environment.Variable> getList() {
        return envList;
    }

    /**
     * Adds environmental variables to the list. Please add new environmental variables here.
     * @param configuration
     */
    private void populateEnvMap(Configuration configuration) {
        addToList(native_mesos_library_key, native_mesos_library_path);
        addToList(JAVA_OPTS, getHeapSpaceString(configuration));
    }

    private void addToList(String key, String value) {
        envList.add(getEnvProto(key, value));
    }

    private Protos.Environment.Variable getEnvProto(String key, String value) {
        return Protos.Environment.Variable.newBuilder()
                .setName(key)
                .setValue(value).build();
    }

    /**
     * Gets the heap space settings. Will set heap space to (available - 256MB) or available/4, whichever is smaller.
     * @param configuration The mesos cluster configuration
     * @return A string representing the java heap space.
     */
    private String getHeapSpaceString(Configuration configuration) {
        int osRam = (int) Math.min(256.0, configuration.getMem() / 4.0);
        return "-Xms" + (int) (configuration.getMem() / 4.0) + "m -Xmx" + ((int) configuration.getMem() - osRam) + "m";
    }
}