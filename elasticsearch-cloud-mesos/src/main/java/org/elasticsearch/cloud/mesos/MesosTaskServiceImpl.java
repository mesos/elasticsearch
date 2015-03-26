package org.elasticsearch.cloud.mesos;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 *  Service that manages the lifecycle of Mesos Tasks.
 */
public class MesosTaskServiceImpl extends AbstractLifecycleComponent<MesosTaskServiceImpl> implements MesosTaskService {

    @Inject
    public MesosTaskServiceImpl(Settings settings) {
        super(settings);
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
}
