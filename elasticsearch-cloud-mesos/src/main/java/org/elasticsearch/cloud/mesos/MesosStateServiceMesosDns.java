package org.elasticsearch.cloud.mesos;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.mesos.MesosUnicastHostsProvider;
import org.xbill.DNS.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class MesosStateServiceMesosDns extends AbstractLifecycleComponent<MesosStateService> implements MesosStateService {
    private final String dnsServer;

    @Inject
    public MesosStateServiceMesosDns(Settings settings) {
        super(settings);
        dnsServer = settings.get("cloud.mesos.resolver", "localhost");
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
        logger.debug("Using resolver: {}", dnsServer);
        final ArrayList<String> nodes = Lists.newArrayList();
        try {
            //TODO: (MWL) remove hardcoded framework name
            final Lookup lookup = new Lookup("_elasticsearch-mesos._tcp.marathon.mesos", Type.SRV);
            lookup.setResolver(new SimpleResolver(dnsServer));
            final Record[] records = lookup.run();

            if (lookup.getResult() != Lookup.SUCCESSFUL) {
                throw new RuntimeException("Failed to resolver task: " + lookup.getErrorString());
            }

            for (Record record : records) {
                if (record instanceof SRVRecord) {
                    final SRVRecord srvRecord = (SRVRecord) record;
                    //TODO: (MWL) first entry of what?
                    final String node = srvRecord.getTarget().getLabelString(0) + ":" + srvRecord.getPort();
                    logger.debug("Adding node {}", node);
                    nodes.add(node);
                }
                else {
                    logger.warn("Ignoring record: {}", record);
                }
            }

        } catch (TextParseException e) {
            throw new RuntimeException("Could not perform dns lookup", e);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not find resolver " + dnsServer, e);
        }

        return nodes;
    }
}
