package org.apache.mesos.elasticsearch.scheduler.matcher;

import org.apache.mesos.Protos;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.ArrayList;
import java.util.List;

/**
 * Matcher for {@link org.apache.mesos.Protos.TaskInfo}s
 */
public class TaskInfoMatcher extends BaseMatcher<Protos.TaskInfo> {

    private String id;
    private Protos.SlaveID slaveId;
    private Double cpus;
    private Double mem;
    private Double disk;

    public TaskInfoMatcher(String id) {
        this.id = id;
    }

    @Override
    public boolean matches(Object o) {
        Protos.TaskInfo taskInfo = (Protos.TaskInfo) o;

        List<Protos.Resource> resources = new ArrayList<>();
        if (cpus != null) {
            resources.add(newResource("cpus", cpus));
        }

        if (mem != null) {
            resources.add(newResource("mem", mem));
        }

        if (disk != null) {
            resources.add(newResource("disk", disk));
        }

        return taskInfo.getResourcesList().containsAll(resources) &&
                taskInfo.getSlaveId().equals(slaveId) &&
                taskInfo.getTaskId().getValue().equals(id) &&
                taskInfo.getName().equals(id);
    }

    private Protos.Resource newResource(String name, Double value) {
        return Protos.Resource.newBuilder()
                .setName(name)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value).build())
                .build();
    }

    @Override
    public void describeTo(Description description) {
        if (cpus != null) {
            description.appendText("(cpu:" + cpus + ")");
        }
        if (mem != null) {
            description.appendText("(mem: " + mem + ")");
        }
        if (disk != null) {
            description.appendText("(disk: " + disk + ")");
        }
    }

    public TaskInfoMatcher slaveId(String slaveId) {
        this.slaveId = Protos.SlaveID.newBuilder().setValue(slaveId).build();
        return this;
    }

    public TaskInfoMatcher cpus(double cpus) {
        this.cpus = cpus;
        return this;
    }

    public TaskInfoMatcher mem(double mem) {
        this.mem = mem;
        return this;
    }

    public TaskInfoMatcher disk(double disk) {
        this.disk = disk;
        return this;
    }

}
