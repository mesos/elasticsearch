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
    private SerializableState zooKeeperStateDriver;
    private FrameworkState frameworkState;

    public ClusterState(SerializableState zooKeeperStateDriver, FrameworkState frameworkState) {
        this.zooKeeperStateDriver = zooKeeperStateDriver;
        this.frameworkState = frameworkState;
        frameworkState.onStatusUpdate(this::updateTask);
    }

    /**
     * Get a list of all tasks with state
     * @return a list of TaskInfo
     */
    public List<TaskInfo> getTaskList() {
        List<TaskInfo> taskInfoList = null;
        try {
            taskInfoList = zooKeeperStateDriver.get(getKey());
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
        return new ESTaskStatus(zooKeeperStateDriver, frameworkState.getFrameworkID(), taskInfo, new StatePath(zooKeeperStateDriver));
    }

    public void addTask(ESTaskStatus esTask) {
        addTask(esTask.getTaskInfo());
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
            getTask(taskId);
        } catch (IllegalArgumentException e) {
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
        List<TaskInfo> taskInfoList = getTaskList();
        LOGGER.debug("Getting task " + taskID.getValue() + ", from " + logTaskList(taskInfoList));
        TaskInfo taskInfo = null;
        for (TaskInfo info : taskInfoList) {
            LOGGER.debug("Testing: " + info.getTaskId().getValue() + " .equals " + taskID.getValue() + " = " + info.getTaskId().getValue().equals(taskID.getValue()));
            if (info.getTaskId().getValue().equals(taskID.getValue())) {
                taskInfo = info;
                break;
            }
        }
        if (taskInfo == null) {
            throw new IllegalArgumentException("Could not find executor with that task ID: " + taskID.getValue());
        }
        return taskInfo;
    }

    public TaskInfo getTask(Protos.ExecutorID executorID) throws IllegalArgumentException {
        if (executorID.getValue().isEmpty()) {
            throw new IllegalArgumentException("ExecutorID.value() is blank. Cannot be blank.");
        }
        List<TaskInfo> taskInfoList = getTaskList();
        LOGGER.debug("Getting task " + executorID.getValue());
        TaskInfo taskInfo = null;
        for (TaskInfo info : taskInfoList) {
            LOGGER.debug("Testing: " + info.getExecutor().getExecutorId().getValue() + " .equals " + executorID.getValue() + " = " + info.getExecutor().getExecutorId().getValue().equals(executorID.getValue()));
            if (info.getExecutor().getExecutorId().getValue().equals(executorID.getValue())) {
                taskInfo = info;
                break;
            }
        }
        if (taskInfo == null) {
            throw new IllegalArgumentException("Could not find executor with that executor ID: " + executorID.getValue());
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

    /**
     * Updates a task with the given status. Status is written to zookeeper.
     * If the task is in error, then the healthchecks are stopped and state is removed from ZK
     * @param status A received task status
     */
    private void updateTask(Protos.TaskStatus status) {
        if (!exists(status.getTaskId())) {
            LOGGER.warn("Could not find task in cluster state.");
            return;
        }

        try {
            Protos.TaskInfo taskInfo = getTask(status.getTaskId());
            LOGGER.debug("Updating task status for executor: " + status.getExecutorId().getValue() + " [" + status.getTaskId().getValue() + ", " + status.getTimestamp() + ", " + status.getState() + "]");
            update(status); // Update state of Executor

            if (taskInError(status)) {
                LOGGER.error("Task in error state. Removing state for executor: " + status.getExecutorId().getValue() + ", due to: " + status.getState());
                removeTask(taskInfo); // Remove task from cluster state.
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }
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
            new StatePath(zooKeeperStateDriver).mkdir(getKey());
            zooKeeperStateDriver.set(getKey(), taskInfoList);
        } catch (IOException ex) {
            LOGGER.error("Could not write list of executor states to zookeeper: ", ex);
        }
    }

    private String getKey() throws NotSerializableException {
        return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
    }
}
