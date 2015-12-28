package org.apache.mesos.elasticsearch.scheduler.controllers;

import org.apache.mesos.elasticsearch.scheduler.state.FrameworkState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TestConfiguration.class)
public class TasksControllerTest {

    @Autowired
    TasksController controller;

    @Autowired
    FrameworkState frameworkState;

    @Test
    public void shouldNotCrashWhenFrameworkNotRegistered() {
        Mockito.when(frameworkState.isRegistered()).thenReturn(false);
        List<TasksController.GetTasksResponse> tasks = controller.getTasks();
        assertTrue(tasks.isEmpty());
    }
}