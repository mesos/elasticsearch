package org.elasticsearch.cloud.mesos;

import org.apache.mesos.elasticsearch.common.Discovery;
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

/**
 * Service that manages the lifecycle of Mesos Tasks.
 */
public class MesosStateServiceRest extends AbstractLifecycleComponent<MesosStateServiceRest> implements MesosStateService {

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
                    try {
                        JSONObject task = tasks.getJSONObject(j);
                        String hostname = slaveIdMap.get(task.getString("slave_id")).getString("hostname");
                        logger.info("Found slave hostname " + hostname);

                        JSONObject discovery = task.optJSONObject("discovery");
                        logger.info("Found discovery: " + discovery.toString());
                        JSONObject portsOuter = discovery.optJSONObject("ports");
                        JSONArray portsInner = portsOuter.optJSONArray("ports");
                        logger.info("Found ports: " + portsInner.toString());
                        List<Integer> portNumbers = new ArrayList<>();
                        for (int k = 0; k < portsInner.length(); k++) {
                            JSONObject port = portsInner.optJSONObject(k);
                            Integer portNumber = port.getInt("number");
                            logger.info("Found port [" + Integer.toString(k) + "]: " + Integer.toString(portNumber));
                            portNumbers.add(portNumber);
                        }

                        Pair<String, Integer> ipAndPort = Pair.of(hostname, portNumbers.get(Discovery.TRANSPORT_PORT_INDEX));
                        nodeIps.add(ipAndPort);
                    } catch (Exception ex) {
                        logger.warn("There was an issue parsing port numbers from state.json." + ex);
                    }
                }
            }
        }
        return nodeIps;
    }

    public void setRepository(MesosStateRepository repository) {
        this.repository = repository;
    }
}
