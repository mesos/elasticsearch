package org.elasticsearch.cloud.mesos;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class MesosModule extends AbstractModule {
    private Settings settings;

    @Inject
    public MesosModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(MesosTaskService.class).to(MesosTaskServiceImpl.class).asEagerSingleton();
    }
}