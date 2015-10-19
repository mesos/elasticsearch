package org.apache.mesos.elasticsearch.scheduler.controllers;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertFalse;

/**
 * Test Cluster controller.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TestConfiguration.class)
public class ClusterControllerTest {

    @Autowired
    private ClusterController clusterController;

    @Test
    public void shouldNotExceptionWhenGeneratingConfiguration() {
        clusterController.clusterInfo();
    }

    @Test
    public void shouldNotHaveErrorInText() {
        clusterController.clusterInfo().configuration.values().forEach(v -> {
            assertFalse(v.toString().contains("ERROR"));
        });
    }


}