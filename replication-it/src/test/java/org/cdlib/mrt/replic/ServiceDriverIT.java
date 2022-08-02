package org.cdlib.mrt.replic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class ServiceDriverIT {
        private int port = 8080;
        private int primaryNode = 7777;
        private int replNode = 8888;
        private String cp = "mrtreplic";

        public ServiceDriverIT() throws IOException, JSONException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set, defaulting to " + port);
                }
                initService();
        }

        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
                System.out.println(json.toString());
                assertTrue(json.has("repsvc:replicationServiceState"));
                assertEquals("running", json.getJSONObject("repsvc:replicationServiceState").get("repsvc:status"));       
        }

        public void initService() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/service/start?t=json", port, cp);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertFalse(s.isEmpty());

                        JSONObject json =  new JSONObject(s);
                        assertNotNull(json);
                }
        }
        public String getContent(String url, int status) throws HttpResponseException, IOException {
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);
                    if (status > 0) {
                        assertEquals(status, response.getStatusLine().getStatusCode());
                    }

                    if (status > 300) {
                        return "";
                    }
                    String s = new BasicResponseHandler().handleResponse(response).trim();
                    assertFalse(s.isEmpty());
                    return s;
                }
        }

        public JSONObject getJsonContent(String url, int status) throws HttpResponseException, IOException, JSONException {
                String s = getContent(url, status);
                JSONObject json = s.isEmpty() ? new JSONObject() : new JSONObject(s);
                assertNotNull(json);
                return json;
        }

}
