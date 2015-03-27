package org.elasticsearch.discovery.mesos;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Provides a list of discovery nodes from the state.json data at the Mesos master.
 */
public class MesosUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    private final TransportService transportService;

    private final Version version;

    private final String master;

    @Inject
    public MesosUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
        super(settings);
        this.transportService = transportService;
        this.version = version;

      // todo: (kensipe) no hardcoded IP
        master = settings.get("cloud.mesos.master", "http://10.186.201.243:5050");
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        logger.debug("Using mesos master {}", master);
        ArrayList<DiscoveryNode> discoveryNodes = Lists.newArrayList();

        List<String> nodeIps = Lists.newArrayList();
        try {
            JSONObject state = Unirest.get(master + "/master/state.json").asJson().getBody().getObject();

            String clusterName = state.getString("cluster");
            logger.debug("received state for cluster name={}", clusterName);

            JSONArray slaves = state.getJSONArray("slaves");
            Map<String, JSONObject> slaveIdMap = Maps.newHashMap();
            for (int i = 0; i < slaves.length(); i++) {
                JSONObject slave = slaves.getJSONObject(i);
                slaveIdMap.put(slave.getString("id"), slave);
            }

            JSONArray frameworks = state.getJSONArray("frameworks");
            for (int i = 0; i < frameworks.length(); i++) {
                JSONObject framework = frameworks.getJSONObject(i);

                if (framework.getBoolean("active") && framework.getString("name").equalsIgnoreCase("elasticsearch")) {
                    JSONArray tasks = framework.getJSONArray("tasks");

                    for (int j = 0; j < tasks.length(); j++) {
                        JSONObject task = tasks.getJSONObject(j);
                        nodeIps.add(slaveIdMap.get(task.getString("slave_id")).getString("hostname"));
                    }
                }
            }

        } catch (UnirestException e) {
            logger.warn("Failed to fetch cluster state", e);
        }


        for (String nodeIp : nodeIps) {
            logger.debug("slave ip={}", nodeIps);
            try {
                discoveryNodes.add(new DiscoveryNode("node-" + nodeIp, transportService.addressesFromString(nodeIp)[0], version.minimumCompatibilityVersion()));
            } catch (Exception e) {
                throw new RuntimeException("Could not create discoverynode", e);
            }
        }

        return discoveryNodes;
    }
}
