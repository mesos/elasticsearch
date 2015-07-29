package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.State;

import java.util.ArrayList;
import java.util.List;

import static org.apache.mesos.Protos.SlaveID;

/**
 * Model of cluster state
 */
public class ClusterState {
    public static final Logger LOGGER = Logger.getLogger(ClusterState.class);
    public static final String SLAVE_LIST = "slaveList";
    private final State state;

    public ClusterState(State state) {
        this.state = state;
    }

    /**
     * Get a list of all Executors with state
     * @return a list of Executor states
     */
    public List<ExecutorState> getStateList() {
        List<SlaveID> slaveList = getSlaveList();
        List<ExecutorState> executorStateList = new ArrayList<>(slaveList.size());
        for (SlaveID id : slaveList) {
            ExecutorState exState = new ExecutorState(state, state.getFrameworkID(), id);
            executorStateList.add(exState);
        }
        return executorStateList;
    }

    /**
     * Get the state of a specific executor
     * @param executorID
     * @return
     */
    public ExecutorState getState(Protos.SlaveID executorID) {
        return new ExecutorState(state, state.getFrameworkID(), executorID);
    }

    public void addSlave(SlaveID slaveID) {
        List<SlaveID> slaveList = getSlaveList();
        slaveList.add(slaveID);
        setSlaveList(slaveList);
    }

    public void removeSlave(SlaveID slaveID) {
        List<SlaveID> slaveList = getSlaveList();
        slaveList.remove(slaveID);
        setSlaveList(slaveList);
    }

    private List<SlaveID> getSlaveList() {
        List<SlaveID> slaveIDList = new ArrayList<>();
        try {
            slaveIDList.addAll(state.get(getSlaveListKey()));
        } catch (Exception ex) {
            LOGGER.info(getSlaveListKey() + " doesn't exist.");
        }
        return slaveIDList;
    }

    private void setSlaveList(List<SlaveID> slaveIDList) {
        try {
            state.setAndCreateParents(getSlaveListKey(), slaveIDList);
        } catch (Exception ex) {
            LOGGER.error("Could not set slave list: ", ex);
        }
    }

    private String getSlaveListKey() {
        return state.getFrameworkID().getValue() + "/" + SLAVE_LIST;
    }


//    /**
//     */
//     * @return a list of executor states
//     * Get's the current cluster state
//    public List<ExecutorState> getState() {
//        update();
//        return executorStateList;
//    }
}
