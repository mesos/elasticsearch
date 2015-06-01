package org.elasticsearch.cloud.mesos;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.mesos.Protos;
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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Service that manages the lifecycle of Mesos Tasks.
 */
public class MesosStateServiceRest extends AbstractLifecycleComponent<MesosStateServiceRest> implements MesosStateService {

    private final String master;

    @Inject
    public MesosStateServiceRest(Settings settings) {
        super(settings);
        master = settings.get("cloud.mesos.master");
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

    @Override
    public List<Pair<String, Integer>> getNodeIpsAndPorts(MesosUnicastHostsProvider mesosUnicastHostsProvider) {
        logger.debug("Using mesos master {}", master);

        List<Pair<String, Integer>> nodeIps = Lists.newArrayList();
        try {
            JSONObject state = Unirest.get(master + "/master/state.json").asJson().getBody().getObject();

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
                        long transportPort;
                        try {
                            String ports = resources.getString("ports");
                            Protos.Resource resource = Protos.Resource.PARSER.parseFrom(ports.getBytes(StandardCharsets.UTF_8));
                            Protos.Value.Range range = resource.getRanges().getRange(0);
                            transportPort = range.getEnd();
                        } catch (InvalidProtocolBufferException e) {
                            logger.error("Could not parse port range from task " + task.getString("id"));
                            return nodeIps;
                        }

                        Pair<String, Integer> ipAndPort = Pair.of(hostname, (int) transportPort);
                        nodeIps.add(ipAndPort);
                    }
                }
            }

        } catch (UnirestException e) {
            logger.warn("Failed to fetch cluster state", e);
        }
        return nodeIps;
    }
}
