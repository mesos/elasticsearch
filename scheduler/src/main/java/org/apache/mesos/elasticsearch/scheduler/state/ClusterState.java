package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.elasticsearch.scheduler.Task;

import java.io.IOException;
import java.io.NotSerializableException;
import java.security.InvalidParameterException;
import java.util.*;

import static org.apache.mesos.Protos.TaskID;

/**
 * Model of cluster state. User is able to add, remove and monitor task status.
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
     * Get a list of all tasks with state
     * @return a list of TaskInfo
     */
    public List<TaskInfo> getTaskList() {
        List<TaskInfo> taskInfoList = null;
        try {
            taskInfoList = state.get(getKey());
        } catch (IOException e) {
            LOGGER.info("Unable to get key for cluster state due to invalid frameworkID.", e);
        }
        return taskInfoList == null ? new ArrayList<>(0) : taskInfoList;
    }

    /**
     * Get a list of all tasks in a format specific to the web GUI.
     * @return
     */
    public Map<String, Task> getGuiTaskList() {
        Map<String, Task> tasks = new HashMap<>();
        getTaskList().forEach(taskInfo -> tasks.put(taskInfo.getTaskId().getValue(), Task.from(taskInfo, getStatus(taskInfo.getTaskId()).getStatus())));
        return tasks;
    }

    /**
     * Get the status of a specific task
     * @param taskID the taskID to retreive the task status for
     * @return a POJO representing TaskInfo, TaskStatus and FrameworkID packets
     * @throws InvalidParameterException when the taskId does not exist in the Task list.
     */
    public ESTaskStatus getStatus(TaskID taskID) throws IllegalArgumentException {
        return getStatus(getTask(taskID));
    }

    private ESTaskStatus getStatus(TaskInfo taskInfo) {
        return new ESTaskStatus(state, frameworkState.getFrameworkID(), taskInfo);
    }

    public void addTask(TaskInfo taskInfo) {
        LOGGER.debug("Adding TaskInfo to cluster for task: " + taskInfo.getTaskId().getValue());
        if (exists(taskInfo.getTaskId())) {
            removeTask(taskInfo);
        }
        List<TaskInfo> taskList = getTaskList();
        taskList.add(taskInfo);
        setTaskInfoList(taskList);
    }

    public void removeTask(TaskInfo taskInfo) throws InvalidParameterException {
        List<TaskInfo> taskList = getTaskList();
        LOGGER.debug("Removing TaskInfo from cluster for task: " + taskInfo.getTaskId().getValue());
        if (!taskList.remove(taskInfo)) {
            throw new InvalidParameterException("TaskInfo does not exist in list: " + taskInfo.getTaskId().getValue());
        }
        getStatus(taskInfo).destroy(); // Destroy task status in ZK.
        setTaskInfoList(taskList); // Remove from cluster state list
    }

    public Boolean exists(TaskID taskId) {
        try {
            getStatus(taskId);
            getTask(taskId);
        } catch (InvalidParameterException e) {
            return false;
        }
        return true;
    }

    /**
     * Get the TaskInfo packet for a specific task.
     * @param taskID the taskID to retreive the TaskInfo for
     * @return a TaskInfo packet
     * @throws IllegalArgumentException when the taskId does not exist in the Task list.
     */
    public TaskInfo getTask(TaskID taskID) throws IllegalArgumentException {
        LOGGER.debug("Getting taskInfo from cluster for task: " + taskID.getValue());
        List<TaskInfo> taskInfoList = getTaskList();
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

    public void update(Protos.TaskStatus status)  throws IllegalArgumentException {
        if (!exists(status.getTaskId())) {
            throw new IllegalArgumentException("Task does not exist in zk.");
        }
        getStatus(status.getTaskId()).setStatus(status);
    }

    public boolean taskInError(Protos.TaskStatus status) {
        return getStatus(status.getTaskId()).taskInError();
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
            statePath.mkdir(getKey());
            state.set(getKey(), taskInfoList);
        } catch (IOException ex) {
            LOGGER.error("Could not write list of executor states to zookeeper: ", ex);
        }
    }

    private String getKey() throws NotSerializableException {
        return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
    }
}
