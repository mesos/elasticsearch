package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for ResourceCheck
 */
public class ResourceCheckTest {
    @Test
    public void givenExactResourceShouldAccept() {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_CPUS);
        List<Protos.Resource> resourceList = new ArrayList<>();
        resourceList.add(Resources.cpus(1, "*"));
        assertTrue(resourceCheck.isEnough(resourceList, 1));
    }

    @Test
    public void givenMoreResourceShouldAccept() {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_CPUS);
        List<Protos.Resource> resourceList = new ArrayList<>();
        resourceList.add(Resources.cpus(10, "*"));
        assertTrue(resourceCheck.isEnough(resourceList, 1));
    }

    @Test
    public void givenMultipleResourceShouldAccept() {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_CPUS);
        List<Protos.Resource> resourceList = new ArrayList<>();
        resourceList.add(Resources.mem(10, "*"));
        resourceList.add(Resources.disk(10, "*"));
        resourceList.add(Resources.cpus(10, "*"));
        assertTrue(resourceCheck.isEnough(resourceList, 1));
    }

    @Test
    public void givenTooLittleResourceShouldDeny() {
        ResourceCheck resourceCheck = new ResourceCheck(Resources.RESOURCE_CPUS);
        List<Protos.Resource> resourceList = new ArrayList<>();
        resourceList.add(Resources.cpus(0.1, "*"));
        assertFalse(resourceCheck.isEnough(resourceList, 1));
    }
}