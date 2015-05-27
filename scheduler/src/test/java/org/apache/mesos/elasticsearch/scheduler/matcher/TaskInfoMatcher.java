package org.apache.mesos.elasticsearch.scheduler.matcher;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Configuration;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.ArrayList;
import java.util.List;

import static org.apache.mesos.elasticsearch.common.Resources.singlePortRange;

/**
 * Matcher for {@link org.apache.mesos.Protos.TaskInfo}s
 */
public class TaskInfoMatcher extends BaseMatcher<Protos.TaskInfo> {

    private String id;
    private Protos.SlaveID slaveId;
    private Double cpus;
    private Double mem;
    private Double disk;
    private int beginPort;
    private int endPort;

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

        if (beginPort != 0 && endPort != 0) {
            resources.add(singlePortRange(beginPort));
            resources.add(singlePortRange(endPort));
        }

        return taskInfo.getResourcesList().containsAll(resources) &&
                taskInfo.getSlaveId().equals(slaveId) &&
                taskInfo.getTaskId().getValue().equals(id) &&
                taskInfo.getName().equals(Configuration.TASK_NAME);
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

    public TaskInfoMatcher beginPort(int beginPort) {
        this.beginPort = beginPort;
        return this;
    }

    public TaskInfoMatcher endPort(int endPort) {
        this.endPort = endPort;
        return this;
    }
}
