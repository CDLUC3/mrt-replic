package org.cdlib.mrt.replic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class ServiceDriverIT {
        private int port = 8080;
        private int dbport = 9999;
        private int primaryNode = 7777;
        private int replNode = 8888;
        private String cp = "mrtreplic";

        private String connstr;
        private String user = "user";
        private String password = "password";

        private String audit_count_sql = "select count(*) from inv_audits where inv_node_id = " + 
          "(select id from inv_nodes where number=?)";

        public ServiceDriverIT() throws IOException, JSONException, SQLException {
                try {
                        port = Integer.parseInt(System.getenv("it-server.port"));
                        dbport = Integer.parseInt(System.getenv("mrt-it-database.port"));
                } catch (NumberFormatException e) {
                        System.err.println("it-server.port not set, defaulting to " + port);
                }
                connstr = String.format("jdbc:mysql://localhost:%d/inv?characterEncoding=UTF-8&characterSetResults=UTF-8&useSSL=false&serverTimezone=UTC", dbport);
                initService();
        }

        @Test
        public void SimpleTest() throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/state?t=json", port, cp);
                JSONObject json = getJsonContent(url, 200);
                System.out.println(json.toString(2));
                assertTrue(json.has("repsvc:replicationServiceState"));
                assertEquals("running", json.getJSONObject("repsvc:replicationServiceState").get("repsvc:status"));       
        }


        public void deleteReplications(int status) throws IOException, JSONException {
                String[] arks = {"ark:/1111/2222", "ark:/1111/3333", "ark:/1111/4444" };
                for(String ark: arks) {
                        String url = String.format("http://localhost:%d/%s/deletesecondary/%s?t=json",
                                port,
                                cp,
                                URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                        );
                        try (CloseableHttpClient client = HttpClients.createDefault()) {
                                HttpDelete del = new HttpDelete(url);
                                HttpResponse response = client.execute(del);
                                if (status > 0) {
                                        assertEquals(status, response.getStatusLine().getStatusCode());
                                        if (status == 200) {
                                                String s = new BasicResponseHandler().handleResponse(response).trim();
                                                assertFalse(s.isEmpty());
                                                JSONObject json = new JSONObject(s);
                                                assertTrue(json.has("repdel:replicationDeleteState"));                
                                        }
                                }
                        }
                }
        }

        @Test
        public void TestReplication() throws IOException, JSONException, InterruptedException, SQLException {
                int repCount = getDatabaseVal(audit_count_sql, 8888, -1);
                if (repCount > 0) {
                        deleteReplications(0);
                        runUpdate("update inv_nodes_inv_objects set replicated = null");
                }

                checkInvDatabase(audit_count_sql, "Confirm replication cleared for 8888", 8888, 0);
                int orig = getDatabaseVal(audit_count_sql, 7777, -1);
                //allow time for the replication to complete
                Thread.sleep(10000);
                checkInvDatabase(audit_count_sql, "Confirm a complete replication from 7777 to 8888", 8888, orig);
                deleteReplications(200);
                checkInvDatabase(audit_count_sql, "Confirm replication cleared for 8888", 8888, 0);
        }


        public void initService() throws IOException, JSONException, SQLException {
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

        public void checkInvDatabase(String sql, String message, int n, int value) throws SQLException {
                try(Connection con = DriverManager.getConnection(connstr, user, password)){
                        try (PreparedStatement stmt = con.prepareStatement(sql)){
                                stmt.setInt(1, n);
                                ResultSet rs=stmt.executeQuery();
                                while(rs.next()) {
                                        assertEquals(message, value, rs.getInt(1));  
                                }  
                        }
                }
        }

        public int getDatabaseVal(String sql, int n, int value) throws SQLException {
                try(Connection con = DriverManager.getConnection(connstr, user, password)){
                        try (PreparedStatement stmt = con.prepareStatement(sql)){
                                stmt.setInt(1, n);
                                ResultSet rs=stmt.executeQuery();
                                while(rs.next()) {
                                        return rs.getInt(1);  
                                }  
                        }
                }
                return value;
        }

        public boolean runUpdate(String sql) throws SQLException {
                try(Connection con = DriverManager.getConnection(connstr, user, password)){
                        try (PreparedStatement stmt = con.prepareStatement(sql)){
                                return stmt.execute();
                        }
                }
        }

        /* 
         * For testing
        - content
        - manifest - get primary 

        Do test
        - delete/node
        - invdelete/node
        - delete secondary
        - add - adds if replic is needed
        - adding - adds if database update is needed
        - match - check if matching (not critical to test)
        */
}
