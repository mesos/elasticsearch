package org.apache.mesos.elasticsearch.systemtest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONObject;
import org.junit.Test;

import java.time.DateTimeException;
import java.time.ZonedDateTime;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StatusApiTest extends TestBase {

    @Test
    public void canGet200FromScheduler() throws Exception {
        String schedulerIp = getSlaveIp("mesoses_scheduler_1");
        HttpResponse<JsonNode> tasksResponse = Unirest.get("http://" + schedulerIp + ":8080/tasks").asJson();
        assertEquals(200, tasksResponse.getStatus());
    }

    @Test
    public void hasThreeTasksWithValidInformation() throws Exception {
        String schedulerIp = getSlaveIp("mesoses_scheduler_1");
        HttpResponse<JsonNode> tasksResponse = Unirest.get("http://" + schedulerIp + ":8080/tasks").asJson();

        assertEquals(3, tasksResponse.getBody().getArray().length());

        for (int i = 0; i < tasksResponse.getBody().getArray().length(); i++) {
            JSONObject taskObject = tasksResponse.getBody().getArray().getJSONObject(i);
            assertThat(taskObject.getString("id"), startsWith("elasticsearch_slave"));
            assertEquals("esdemo", taskObject.getString("name"));
            assertThat(taskObject.getString("started_at"), isValidDateTime());
            assertThat(taskObject.getString("http_address"), isValidAddress());
            assertThat(taskObject.getString("transport_address"), isValidAddress());
            assertThat(taskObject.getString("hostname"), startsWith("slave"));
        }
    }

    private Matcher<? super String> isValidAddress() {
        return new TypeSafeMatcher<String>() {
            @Override
            protected boolean matchesSafely(String item) {
                return item.matches("^[a-zA-Z0-9\\.\\-]+(:[0-9]+)?$");
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("host[:port]");
            }
        };
    }

    private Matcher<? super String> isValidDateTime() {
        return new TypeSafeMatcher<String>() {
            @Override
            protected boolean matchesSafely(String item) {
                try {
                    ZonedDateTime.parse(item);
                    return true;
                } catch (DateTimeException e) {
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Valid ISO zoned date time");
            }
        };
    }
}
