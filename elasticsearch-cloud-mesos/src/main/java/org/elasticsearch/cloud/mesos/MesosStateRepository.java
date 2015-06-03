package org.elasticsearch.cloud.mesos;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;

/**
 * Repository for retrieving the master state file.
 */
public class MesosStateRepository {

    private String master;

    public MesosStateRepository(String master) {
        this.master = master;
    }

    public JSONObject retrieveState() {
        try {
            return Unirest.get(master + "/master/state.json").asJson().getBody().getObject();
        } catch (UnirestException e) {
            throw new RuntimeException("Could not retrieve state file", e);
        }
    }
}
