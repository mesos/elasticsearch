package org.elasticsearch.cloud.mesos;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

/**
 * The elastic search module to be used with a Mesos Cluster.
 */
public class MesosModule extends AbstractModule {
    private Settings settings;

    @Inject
    public MesosModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        String discoverySetting = settings.get("cloud.mesos.discovery", "mesos-dns");

        if (discoverySetting.equals("rest")) {
            bind(MesosStateService.class).to(MesosStateServiceRest.class).asEagerSingleton();
        } else {
            String resolver = settings.get("cloud.mesos.resolver", "");

            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
            env.put(Context.PROVIDER_URL, "dns://" + resolver);
            try {
                bind(DirContext.class).toInstance(new InitialDirContext(env));
            } catch (NamingException e) {
                throw new RuntimeException("Failed to create resolver context", e);
            }

            bind(MesosStateService.class).to(MesosStateServiceMesosDns.class).asEagerSingleton();
        }


    }
}
