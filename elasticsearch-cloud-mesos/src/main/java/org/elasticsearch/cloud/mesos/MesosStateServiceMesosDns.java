package org.elasticsearch.cloud.mesos;

import org.apache.mesos.elasticsearch.common.Configuration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lang3.tuple.Pair;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
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
        logger.info("Starting Mesos DNS state service");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping Mesos DNS state service");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("Closing Mesos DNS state service");
    }

    @Override
    public List<Pair<String, Integer>> getNodeIpsAndPorts(MesosUnicastHostsProvider mesosUnicastHostsProvider) {
        String taskHostName = "_" + Configuration.TASK_NAME + "._tcp." + Configuration.FRAMEWORK_NAME + "." + Configuration.DOMAIN;

        final List<Pair<String, Integer>> nodes = Lists.newArrayList();

        Attributes attrs;
        NamingEnumeration<?> enumeration;
        try {
            attrs = ctx.getAttributes(taskHostName, new String[]{"SRV"});

            final Attribute srvRecords = attrs.get("SRV");
            if (srvRecords != null && srvRecords.size() > 0) {
                enumeration = srvRecords.getAll();

                while (enumeration.hasMore()) {
                    SrvRecord record = new SrvRecord((String) enumeration.next());
                    nodes.add(Pair.of(record.getHost(), Integer.valueOf(record.getPort())));
                }
            }
        } catch (NamingException e) {
            logger.debug("Failed to resolve hostname", e);
        }

        logger.debug("getNodeIpsAndPorts - " + taskHostName + " -> " + nodes);

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
