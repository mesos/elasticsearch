package org.elasticsearch.discovery.mesos;

import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;

public class MesosDiscoveryModule extends ZenDiscoveryModule {

    @Override
    protected void bindDiscovery() {
        bind(Discovery.class).to(MesosDiscovery.class).asEagerSingleton();
    }
}
