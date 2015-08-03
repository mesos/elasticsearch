package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.TaskInfo;

import java.io.NotSerializableException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.mesos.Protos.TaskID;

/**
 * Model of cluster state
 */
public class ClusterState {
    public static final Logger LOGGER = Logger.getLogger(ClusterState.class);
    public static final String STATE_LIST = "stateList";
    private final SerializableState state;
    private final FrameworkState frameworkState;
    private final StatePath statePath;

    public ClusterState(SerializableState state, FrameworkState frameworkState) {
        this.state = state;
        this.frameworkState = frameworkState;
        statePath = new StatePath(state);
    }

    /**
     * Get a list of all Executors with state
     * @return a list of Executor states
     */
    public List<TaskInfo> getStateList() {
        return getTaskInfoList();
    }

    /**
     * Get the state of a specific executor
     */
    public ESTaskStatus getStatus(TaskID taskID) {
        TaskInfo taskInfo = getTask(taskID);
        return new ESTaskStatus(state, frameworkState.getFrameworkID(), taskInfo);
    }

    public TaskInfo getTask(TaskID taskID) {
        LOGGER.debug("Getting taskInfo from cluster for task: " + taskID.getValue());
        List<TaskInfo> taskInfoList = getTaskInfoList();
        TaskInfo taskInfo = null;
        for (TaskInfo info : taskInfoList) {
            if (info.getTaskId().equals(taskID)) {
                taskInfo = info;
                break;
            }
        }
        if (taskInfo == null) {
            throw new InvalidParameterException("Could not find executor with that task ID: " + taskID.getValue());
        }
        return taskInfo;
    }

    public void addTask(TaskInfo taskInfo) {
        LOGGER.debug("Adding TaskInfo to cluster for task: " + taskInfo.getTaskId().getValue());
        List<TaskInfo> taskList = getTaskInfoList();
        taskList.add(taskInfo);
        setTaskInfoList(taskList);
    }

    public void removeTask(TaskInfo taskInfo) {
        List<TaskInfo> slaveList = getTaskInfoList();
        slaveList.remove(taskInfo);
        setTaskInfoList(slaveList);
    }

    private List<TaskInfo> getTaskInfoList() {
        List<TaskInfo> taskinfolist = new ArrayList<>();
        try {
            taskinfolist.addAll(state.get(getKey()));
        } catch (Exception ex) {
            LOGGER.info("Unable to get key for cluster state due to invalid frameworkID.");
        }
        return taskinfolist;
    }

    private String logTaskList(List<TaskInfo> taskInfoList) {
        List<String> res = new ArrayList<>();
        for (TaskInfo t : taskInfoList) {
            res.add(t.getTaskId().getValue());
        }
        return Arrays.toString(res.toArray());
    }

    private void setTaskInfoList(List<TaskInfo> taskInfoList) {
        LOGGER.debug("Writing executor state list: " + logTaskList(taskInfoList));
        try {
            statePath.setAndCreateParents(getKey(), taskInfoList);
        } catch (Exception ex) {
            LOGGER.error("Could not write list of executor states to zookeeper: ", ex);
        }
    }

    private String getKey() throws NotSerializableException {
        return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
    }

    public Boolean exists(TaskID taskId) {
        try {
            getStatus(taskId);
        } catch (InvalidParameterException e) {
            return false;
        }
        return true;
    }
}
