package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.mesos.elasticsearch.scheduler.cluster.ESTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A task list that is persisted.
 */
public class ESTaskList {
    private static final Logger LOG = LoggerFactory.getLogger(ESTaskList.class);
    private final ESState<List<ESTask>> state;

    public ESTaskList(ESState<List<ESTask>> state) {
        this.state = state;
    }

    public void add(ESTask task) {
        final List<ESTask> esTasks = get();
        if (esTasks.contains(task)) {
            LOG.warn("Task already exists. Not adding to list.");
        } else {
            esTasks.add(task);
            state.set(esTasks);
        }
    }

    public void remove(ESTask task) {
        task.destroy();
        final List<ESTask> esTasks = get();
        if (esTasks.remove(task)) {
            state.set(esTasks);
        } else {
            LOG.warn("Removed task that was not in task list. " + task.toString());
        }
    }

    public List<ESTask> get() {
        final List<ESTask> taskList = state.get();
        return taskList == null ? new ArrayList<>(0) : taskList;
    }

    public void destroy() {
        get().stream().forEach(this::remove);
        state.destroy();
    }
}
