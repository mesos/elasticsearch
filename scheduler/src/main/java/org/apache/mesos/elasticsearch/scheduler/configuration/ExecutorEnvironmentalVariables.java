package org.apache.mesos.elasticsearch.scheduler.configuration;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Environmental variables for the executor
 */
public class ExecutorEnvironmentalVariables {
    private static final Logger LOGGER = Logger.getLogger(ExecutorEnvironmentalVariables.class.toString());

    private static final String native_mesos_library_key = "MESOS_NATIVE_JAVA_LIBRARY";
    private static final String native_mesos_library_path = "/usr/lib/libmesos.so"; // libmesos.so is usually symlinked to the version.
    private static final String CONTAINER_PATH_SETTINGS = "/tmp/config";
    
    public static final String JAVA_OPTS = "JAVA_OPTS";
    public static final String ES_HEAP = "ES_HEAP_SIZE";
    public static final int EXTERNAL_VOLUME_NOT_CONFIGURED = -1;
    public static final String ELASTICSEARCH_NODE_ID = "ELASTICSEARCH_NODE_ID";

    public static final String DVDI_VOLUME_NAME = "DVDI_VOLUME_NAME";
    public static final String DVDI_VOLUME_DRIVER = "DVDI_VOLUME_DRIVER";
    public static final String DVDI_VOLUME_OPTS = "DVDI_VOLUME_OPTS";
    public static final String DVDI_VOLUME_CONTAINERPATH = "DVDI_VOLUME_CONTAINERPATH";
    
    private final List<Protos.Environment.Variable> envList = new ArrayList<>();

    /**
     * @param configuration The mesos cluster configuration
     */
    public ExecutorEnvironmentalVariables(Configuration configuration) {
        populateEnvMap(configuration);
    }

    public ExecutorEnvironmentalVariables(Configuration configuration, long lNodeId) {
        populateEnvMap(configuration);

        if (lNodeId == EXTERNAL_VOLUME_NOT_CONFIGURED) {
            return; //invalid node id
        }

        addToList(ELASTICSEARCH_NODE_ID, Long.toString(lNodeId));
        LOGGER.debug("Elastic Node ID: " + lNodeId);

        //uses the mesos isolator to create/attach external volumes by setting env variables
        populateEnvMapForMesos(configuration, lNodeId);
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
        addToList(ES_HEAP, getHeapSpaceString(configuration));
        if (configuration.isFrameworkUseDocker()) {
            addToList(native_mesos_library_key, native_mesos_library_path);
        }
        addToList(JAVA_OPTS, getHeapSpaceString(configuration));
    }

    private void populateEnvMapForMesos(Configuration configuration, long lNodeId) {
        if (configuration.isFrameworkUseDocker() ||
                configuration.getExternalVolumeDriver() == null ||
                configuration.getExternalVolumeDriver().length() == 0) {
            LOGGER.debug("Not using Mesos Isolator driver");
            return; //volume driver not set
        }

        LOGGER.debug("Docker Driver: " + configuration.getExternalVolumeDriver());

        //note: this makes a unique configuration volume name per elastic search node
        StringBuffer sbConfig = new StringBuffer(configuration.getFrameworkName());
        sbConfig.append(Long.toString(lNodeId));
        sbConfig.append("config");
        String sHostPathOrExternalVolumeForConfig = sbConfig.toString();
        LOGGER.debug("Config Volume Name: " + sHostPathOrExternalVolumeForConfig);

        //note: this makes a unique data volume name per elastic search node
        StringBuffer sbData = new StringBuffer(configuration.getFrameworkName());
        sbData.append(Long.toString(lNodeId));
        sbData.append("data");
        String sHostPathOrExternalVolumeForData = sbData.toString();
        LOGGER.debug("Config Volume Name: " + sHostPathOrExternalVolumeForConfig);

        //sets the environment variables for to create and/or attach the configuration volume
        //to the mesos containerizer
        addToList(DVDI_VOLUME_DRIVER, configuration.getExternalVolumeDriver());
        addToList(DVDI_VOLUME_NAME, sHostPathOrExternalVolumeForConfig);
        if (configuration.getExternalVolumeOption() != null && configuration.getExternalVolumeOption().length() > 0) {
            addToList(DVDI_VOLUME_OPTS, configuration.getExternalVolumeOption());
        }
        addToList(DVDI_VOLUME_CONTAINERPATH, CONTAINER_PATH_SETTINGS);

        //sets the environment variables for to create and/or attach the data volume
        //to the mesos containerizer
        addToList(DVDI_VOLUME_DRIVER + "1", configuration.getExternalVolumeDriver());
        addToList(DVDI_VOLUME_NAME + "1", sHostPathOrExternalVolumeForData);
        if (configuration.getExternalVolumeOption() != null && configuration.getExternalVolumeOption().length() > 0) {
            addToList(DVDI_VOLUME_OPTS + "1", configuration.getExternalVolumeOption());
        }
        addToList(DVDI_VOLUME_CONTAINERPATH, configuration.getDataDir());
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
        return "" + ((int) configuration.getMem() - osRam) + "m";
    }
}