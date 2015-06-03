package org.elasticsearch.cloud.mesos;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lang3.tuple.Pair;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service that manages the lifecycle of Mesos Tasks.
 */
public class MesosStateServiceRest extends AbstractLifecycleComponent<MesosStateServiceRest> implements MesosStateService {

    // An elasticsearch task has 2 ports, client port and transport port.
    public static final int TRANSPORT_PORT_INDEX = 1;

    private final String master;

    private MesosStateRepository repository;

    @Inject
    public MesosStateServiceRest(Settings settings) {
        super(settings);
        master = settings.get("cloud.mesos.master");
        repository = new MesosStateRepository(master);
    }

    @Override
    protected void doStart() throws ElasticsearchException {

    }

    @Override
    protected void doStop() throws ElasticsearchException {

    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }

    @SuppressWarnings("PMD.CyclomaticComplexity")
    @Override
    public List<Pair<String, Integer>> getNodeIpsAndPorts(MesosUnicastHostsProvider mesosUnicastHostsProvider) {
        logger.debug("Using mesos master {}", master);

        List<Pair<String, Integer>> nodeIps = Lists.newArrayList();
        JSONObject state = repository.retrieveState();
        try {
            String clusterName = state.getString("cluster");
            logger.info("received state for cluster name={}", clusterName);
        } catch (JSONException e) {
            logger.warn("Mesos 'cluster' property not set");
        }

        JSONArray slaves = state.getJSONArray("slaves");
        Map<String, JSONObject> slaveIdMap = Maps.newHashMap();
        for (int i = 0; i < slaves.length(); i++) {
            JSONObject slave = slaves.getJSONObject(i);
            slaveIdMap.put(slave.getString("id"), slave);
        }
        logger.info("Found slaves " + slaveIdMap);

        JSONArray frameworks = state.getJSONArray("frameworks");
        for (int i = 0; i < frameworks.length(); i++) {
            JSONObject framework = frameworks.getJSONObject(i);

            if (framework.getBoolean("active") && framework.getString("name").equalsIgnoreCase("elasticsearch")) {
                JSONArray tasks = framework.getJSONArray("tasks");

                for (int j = 0; j < tasks.length(); j++) {
                    JSONObject task = tasks.getJSONObject(j);
                    String hostname = slaveIdMap.get(task.getString("slave_id")).getString("hostname");
                    logger.info("Found slave hostname " + hostname);
                    JSONObject resources = task.getJSONObject("resources");

                    String portRanges = resources.getString("ports");
                    logger.info("Ports " + portRanges);

                    Pattern pattern = Pattern.compile("\\[(\\d+)\\-(\\d+)(, (\\d+)\\-(\\d+))*\\]");
                    Matcher matcher = pattern.matcher(portRanges);
                    if (!matcher.matches()) {
                        throw new RuntimeException("Could not parse port ranges: " + portRanges);
                    }
                    logger.info("Found " + matcher.groupCount() + " groups");

                    List<Integer> portNumbers = new ArrayList<>();
                    int numberOfRanges = (matcher.groupCount() - 1) / 2;
                    for (int k = 0; k < numberOfRanges; k += 1) {
                        int beginPort = Integer.parseInt(matcher.group(3 * k + 1));
                        int endPort = Integer.parseInt(matcher.group(3 * k + 2));
                        for (int l = beginPort; l <= endPort; l++) {
                             portNumbers.add(l);
                        }
                    }

                    Pair<String, Integer> ipAndPort = Pair.of(hostname, portNumbers.get(TRANSPORT_PORT_INDEX));
                    nodeIps.add(ipAndPort);
                }
            }
        }
        return nodeIps;
    }

    public void setRepository(MesosStateRepository repository) {
        this.repository = repository;
    }
}
