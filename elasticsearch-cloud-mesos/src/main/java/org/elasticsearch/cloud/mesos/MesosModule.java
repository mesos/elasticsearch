package org.elasticsearch.cloud.mesos;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

/**
 * The elastic search module to be used with a Mesos Cluster.
 */
public class MesosModule extends AbstractModule {
    private Settings settings;
    private final ESLogger logger = Loggers.getLogger(getClass());

    @Inject
    public MesosModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        String discoverySetting = settings.get("cloud.mesos.discovery", "auto");

        final InitialDirContext dirContext = createDirContext(settings.get("cloud.mesos.resolver", ""));
        logger.info("Discovery setting: {}", discoverySetting);

        if (discoverySetting.equals("rest")) {
            setupRestDiscovery();
        } else if (discoverySetting.equals("mesos-dns")) {
            setupMesosDnsDiscovery(dirContext);
        } else {
            try {
                final Attribute aRecords = dirContext.getAttributes("leader.mesos", new String[]{"A"}).get("A");
                if (aRecords != null && aRecords.size() > 0) {
                    logger.info("Found 'leader.mesos' A record");
                    setupMesosDnsDiscovery(dirContext);
                } else {
                    logger.info("No 'leader.mesos' A record. Falling back to Rest discovery");
                    setupRestDiscovery();
                }
            } catch (NamingException e) {
                logger.info("Naming exception asking for 'leader.mesos'. Choosing Rest discovery strategy", e);
                setupRestDiscovery();
            }
        }
    }

    private void setupRestDiscovery() {
        bind(MesosStateService.class).to(MesosStateServiceRest.class).asEagerSingleton();
        logger.info("Choosen discovery strategy: Rest");
    }

    private void setupMesosDnsDiscovery(DirContext dirContext) {
        bind(DirContext.class).toInstance(dirContext);
        bind(MesosStateService.class).to(MesosStateServiceMesosDns.class).asEagerSingleton();
        logger.info("Choosen discovery strategy: Mesos-dns");
    }

    private InitialDirContext createDirContext(String resolver) {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        env.put(Context.PROVIDER_URL, "dns://" + resolver);
        final InitialDirContext dirContext;
        try {
            dirContext = new InitialDirContext(env);
        } catch (NamingException e) {
            throw new RuntimeException("Failed to create resolver context", e);
        }
        return dirContext;
    }
}
