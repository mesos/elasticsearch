package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.elasticsearch.scheduler.cluster.ESTask;
import org.apache.mesos.elasticsearch.scheduler.configuration.ExecutorEnvironmentalVariables;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Model of cluster state. User is able to add, remove and monitor task status.
 */
public class ClusterState {
    public static final Logger LOGGER = Logger.getLogger(ClusterState.class);
    public static final String STATE_LIST = "stateList";
    private final ESTaskList taskList;

    public ClusterState(ESState<List<ESTask>> state) {
        taskList = new ESTaskList(state);
    }

    public List<ESTask> get() {
        return taskList.get();
    }

    public void add(ESTask task) {
        taskList.add(task);
    }

    public void update(ESTask task) {
        taskList.get().stream()
                .filter(task1 -> task1.getTask().getTaskId().getValue().equals(task.getTask().getTaskId().getValue()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Task is not in task list. Cannot update. " + task.toString()))
                .updateStatus(task.getStatus());

    }

    public void remove(ESTask task) {
        taskList.remove(task);
    }

    public void destroy() {
        taskList.destroy();
    }

    /**
     * When using external volumes, retrieve the next available elasticsearch node id
     *
     * @return Integer (node id)
     */
    public Integer getElasticNodeId() {
        final List<Integer> idList = getElasticNodeIdList();
        return IntStream.range(0, idList.size() + 1).filter(value -> !idList.contains(value)).findFirst().getAsInt();
    }

    private List<Integer> getElasticNodeIdList() {
        return get().stream()
                .filter(this::containsElasticNodeId)
                .map(this::getElasticNodeId)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    public static String getKey(FrameworkState frameworkState) {
        return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
    }

    private String getElasticNodeId(ESTask task) {
        return task.getTask().getCommand().getEnvironment().getVariablesList().stream().filter(variable1 -> variable1.getName().equals(ExecutorEnvironmentalVariables.ELASTICSEARCH_NODE_ID)).findFirst().get().getValue();
    }

    private boolean containsElasticNodeId(ESTask task) {
        return task.getTask().getCommand().getEnvironment().getVariablesList().stream().anyMatch(variable -> variable.getName().equals(ExecutorEnvironmentalVariables.ELASTICSEARCH_NODE_ID));
    }
}
