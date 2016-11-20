package org.apache.mesos.elasticsearch.systemtest;

import com.containersol.minimesos.cluster.MesosCluster;
import com.containersol.minimesos.junit.MesosClusterTestRule;
import com.jayway.awaitility.Awaitility;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Performs end-to-end tests when the framework has been deployed on minimesos.
 */
public class DeploymentSystemTest {

    public static final Logger LOGGER = Logger.getLogger(DeploymentSystemTest.class);

    @ClassRule
    public static final MesosClusterTestRule RULE = MesosClusterTestRule.fromFile("src/systemTest/resources/testMinimesosFile");

    public static final MesosCluster CLUSTER = RULE.getMesosCluster();

    @Test
    public void testDeployment() {
        Awaitility.await().pollInterval(1L, TimeUnit.SECONDS).atMost(2L, TimeUnit.MINUTES).until(this::frameworkIsDeployed);
    }

    private boolean frameworkIsDeployed() {
        JSONArray frameworks = CLUSTER.getClusterStateInfo().getJSONArray("frameworks");

        for (Object framework : frameworks) {
            JSONObject fw = (JSONObject) framework;
            LOGGER.info("Found framework: " + fw.getString("name"));
            if (fw.getString("name").equals("elasticsearch")) {
                return true;
            }
        }

        return false;
    }

}
