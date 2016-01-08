package org.apache.mesos.elasticsearch.executor.model;

import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.executor.mesos.ElasticsearchExecutor;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.List;
import java.util.stream.Collectors;

/**
 */
public class HostsModel implements RunTimeSettings {
    private static final Logger LOG = Logger.getLogger(ElasticsearchExecutor.class.getCanonicalName());
    private final List<String> hosts;

    public HostsModel(List<String> hosts) {
        this.hosts = hosts;
    }


    @Override
    public ImmutableSettings.Builder getRuntimeSettings() {
        return ImmutableSettings.settingsBuilder()
                .put("discovery.zen.ping.unicast.hosts", getHosts());
    }

    public String getHosts() {
        String res = hosts.stream().collect(Collectors.joining(","));
        LOG.debug("Unicast hosts = " + res);
        return res;
    }
}
