package org.elasticsearch.plugin.cloud.mesos;

import org.elasticsearch.cloud.mesos.MesosModule;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.ArrayList;
import java.util.Collection;

public class CloudMesosPlugin extends AbstractPlugin {
    private final Settings settings;

    public CloudMesosPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "cloud-mesos";
    }

    @Override
    public String description() {
        return "Cloud Mesos Plugin";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        ArrayList<Class<? extends Module>> modules = Lists.newArrayList();
        if (settings.getAsBoolean("cloud.enabled", true)) {
            modules.add(MesosModule.class);
        }
        return modules;
    }
}
