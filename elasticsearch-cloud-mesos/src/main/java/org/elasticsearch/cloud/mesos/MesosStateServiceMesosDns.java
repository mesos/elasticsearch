package org.elasticsearch.cloud.mesos;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MesosStateServiceMesosDns extends AbstractLifecycleComponent<MesosStateService> implements MesosStateService {
    private final DirContext ctx;

    @Inject
    public MesosStateServiceMesosDns(Settings settings, DirContext ctx) {
        super(settings);
        this.ctx = ctx;
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
    public List<String> getNodeIpsAndPorts(MesosUnicastHostsProvider mesosUnicastHostsProvider) {
        //TODO: (MWL) remove hardcoded framework name
        String taskHostName = "_elasticsearch-mesos._tcp.marathon.mesos";
        final ArrayList<String> nodes = Lists.newArrayList();

        Attributes attrs;
        NamingEnumeration<?> enumeration;
        try {
            attrs = ctx.getAttributes(taskHostName, new String[]{"SRV"});

            final Attribute srvRecords = attrs.get("SRV");
            if (srvRecords != null && srvRecords.size() > 0) {
                enumeration = srvRecords.getAll();

                while (enumeration.hasMore()) {
                    SrvRecord record = new SrvRecord((String) enumeration.next());

                    nodes.add(record.getHostAndPort());
                }
            }
        } catch (NamingException e) {
            throw new RuntimeException("Failed to resolve hostname", e);
        }

        return nodes;
    }

    private static class SrvRecord {
        private final String[] fields;

        public SrvRecord(String record) {
            fields = record.split(" ");
        }

        public String getPort() {
            return fields[2];
        }

        public String getHost() {
            return fields[3];
        }

        public String getHostAndPort() {
            return getHost() + ":" + getPort();
        }
    }

}
