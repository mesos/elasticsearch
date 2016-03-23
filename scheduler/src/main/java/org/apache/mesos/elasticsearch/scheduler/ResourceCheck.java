package org.apache.mesos.elasticsearch.scheduler;

import org.apache.mesos.Protos;

import java.util.List;

/**
 * Checks a list of resources to ensure that it contains more than the required value
 */
public class ResourceCheck {
    private final String resourceName;

    /**
     * @param resourceName the name of the resource you want to check
     */
    public ResourceCheck(String resourceName) {
        this.resourceName = resourceName;
    }

    /**
     * Check to see if the required amount of resources is contained within the resources list.
     * @param resourcesList a list of resources
     * @param requiredValue the required minimum amount of resource
     * @return true if there are enough resources
     */
    public Boolean isEnough(List<Protos.Resource> resourcesList, double requiredValue) {
        Protos.Resource resource = getResource(resourcesList);
        return resource != null && resource.getScalar() != null && resource.getScalar().getValue() >= requiredValue;
    }

    private Protos.Resource getResource(List<Protos.Resource> resourcesList) {
        for (Protos.Resource resource : resourcesList) {
            if (resource.getName().equals(resourceName)) {
                return resource;
            }
        }
        return null;
    }
}