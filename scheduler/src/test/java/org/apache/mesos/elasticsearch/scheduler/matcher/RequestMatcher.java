package org.apache.mesos.elasticsearch.scheduler.matcher;

import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.scheduler.Resources;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Collection;

/**
 * Matcher for {@link org.apache.mesos.Protos.Request}s
 */
public class RequestMatcher extends BaseMatcher<Collection<Protos.Request>> {

    private double cpus;
    private double mem;
    private double disk;
    private String frameworkRole;

    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(Object o) {
        Collection<Protos.Request> requests = (Collection<Protos.Request>) o;

        Protos.Resource cpuResource = Protos.Resource.newBuilder()
                .setName(Resources.RESOURCE_CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .setRole(frameworkRole)
                .build();

        Protos.Resource memResource = Protos.Resource.newBuilder()
                .setName(Resources.RESOURCE_MEM)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .setRole(frameworkRole)
                .build();

        Protos.Resource diskResource = Protos.Resource.newBuilder()
                .setName(Resources.RESOURCE_DISK)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk).build())
                .setRole(frameworkRole)
                .build();

        Protos.Request request = Protos.Request.newBuilder()
                .addResources(cpuResource)
                .addResources(memResource)
                .addResources(diskResource)
                .build();

        return requests.contains(request);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(cpus + " cpu(s)");
    }

    public RequestMatcher(double cpus, double mem, double disk, String frameworkRole) {
        this.cpus = cpus;
        this.mem = mem;
        this.disk = disk;
        this.frameworkRole = frameworkRole;
    }
}
