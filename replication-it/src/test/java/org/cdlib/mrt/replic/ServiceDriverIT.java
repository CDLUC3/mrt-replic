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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class ServiceDriverIT {
        private int port = 8080;
        private int dbport = 9999;
        private int primaryNode = 7777;
        private int replNode = 8888;
        private String cp = "mrtreplic";
        String[] ARKS = {"ark:/1111/2222", "ark:/1111/3333", "ark:/1111/4444" };

        private String connstr;
        private String user = "user";
        private String password = "password";

        private String audit_count_sql = "select count(*) from inv_audits where inv_node_id = " + 
          "(select id from inv_nodes where number=?)";
        private String storage_maints_sql = "select count(*) from inv_storage_maints where inv_storage_scan_id=? and maint_type=?";
        private String storage_maints_del_sql = "delete from inv_storage_maints";

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
                assertTrue(json.has("repsvc:replicationServiceState"));
                assertEquals("running", json.getJSONObject("repsvc:replicationServiceState").get("repsvc:status"));       
        }

       @Test
        public void testJsonState() throws HttpResponseException, IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/jsonstate", port, cp);
                testNodeStatus(getJsonContent(url, 200), "NodesState");
        }

        @Test
        public void testJsonStatus() throws HttpResponseException, IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/jsonstatus", port, cp);
                testNodeStatus(getJsonContent(url, 200), "NodesStatus");
        }

        public void testNodeStatus(JSONObject json, String key) throws HttpResponseException, IOException, JSONException {
                JSONArray jarr = json.getJSONArray(key);
                assertTrue(jarr.length() > 0);
                for(int i=0; i < jarr.length(); i++){
                        assertTrue(jarr.getJSONObject(i).getBoolean("running"));
                }
        }

        public void deleteReplication(String ark, int status) throws IOException, JSONException {
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

        public void deleteReplications(int status) throws IOException, JSONException {
                String[] arks = {"ark:/1111/2222", "ark:/1111/3333", "ark:/1111/4444" };
                for(String ark: arks) {
                        deleteReplication(ark, status);
                }
        }

        public void deleteReplicationsFromNode(int number, int status) throws IOException, JSONException {
                for(String ark: ARKS) {
                        String url = String.format("http://localhost:%d/%s/delete/%d/%s?t=json",
                                port,
                                cp,
                                number,
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
                        url = String.format("http://localhost:%d/%s/invdelete/%d/%s?t=json",
                                port,
                                cp,
                                number,
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

        public void addReplications() throws IOException, JSONException {
                for(String ark: ARKS) {
                        String url = String.format("http://localhost:%d/%s/add/%s?t=json",
                                port,
                                cp,
                                URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                        );
                        try (CloseableHttpClient client = HttpClients.createDefault()) {
                                HttpPost post = new HttpPost(url);
                                HttpResponse response = client.execute(post);
                                assertEquals(200, response.getStatusLine().getStatusCode());
                                String s = new BasicResponseHandler().handleResponse(response).trim();
                                assertFalse(s.isEmpty());
                                JSONObject json = new JSONObject(s);
                                assertTrue(json.has("repadd:replicationAddState"));                
                        }
                        url = String.format("http://localhost:%d/%s/addinv/%s?t=json",
                                port,
                                cp,
                                URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                        );
                        try (CloseableHttpClient client = HttpClients.createDefault()) {
                                HttpPost post = new HttpPost(url);
                                HttpResponse response = client.execute(post);
                                assertEquals(200, response.getStatusLine().getStatusCode());
                                String s = new BasicResponseHandler().handleResponse(response).trim();
                                assertFalse(s.isEmpty());
                                JSONObject json = new JSONObject(s);
                                assertTrue(json.has("repadd:replicationAddState"));                
                        }
                }
        }

        public void resetReplication() throws IOException, JSONException, InterruptedException, SQLException {
                int repCount = getDatabaseVal(audit_count_sql, replNode, -1);
                if (repCount > 0) {
                        deleteReplications(0);
                }
                runUpdate("update inv_nodes_inv_objects set replicated = null");
                checkInvDatabase(audit_count_sql, "Confirm replication cleared for Repl Node", replNode, 0);
        }

        public void letReplicationRun()  throws IOException, JSONException, InterruptedException, SQLException {
                int orig = getDatabaseVal(audit_count_sql, primaryNode, -1);
                //allow time for the replication to complete
                int count = getDatabaseVal(audit_count_sql, replNode, -1);
                for(int i=0; i < 10 && count != orig; i++) {
                        Thread.sleep(10000);
                        count = getDatabaseVal(audit_count_sql, replNode, -1);
                }
                assertEquals(orig, count);
        }

        public void matchObject(String ark) throws IOException, JSONException, InterruptedException, SQLException {
                String url = String.format("http://localhost:%d/%s/match/%d/%d/%s?t=json",
                        port,
                        cp,
                        primaryNode,
                        replNode,
                        URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                );
                JSONObject json = getJsonContent(url, 200);
                assertTrue(json.has("obj:matchObjectState"));
                assertEquals(true, json.getJSONObject("obj:matchObjectState").getBoolean("obj:matchManifestInv"));
                assertEquals(true, json.getJSONObject("obj:matchObjectState").getBoolean("obj:matchManifestStore"));
        }

        @Test
        public void TestReplication() throws IOException, JSONException, InterruptedException, SQLException {
                resetReplication();
                letReplicationRun();

                for(String ark: ARKS) {
                        matchObject(ark);
                }

                deleteReplications(200);
                checkInvDatabase(audit_count_sql, "Confirm replication cleared for Replication Node", replNode, 0);
        }

        @Test
        public void TestAccessHelperMethods() throws IOException, JSONException, InterruptedException, SQLException {
                for(String ark: ARKS) {
                        String url = String.format("http://localhost:%d/%s/manifest/%s",
                                port,
                                cp,
                                URLEncoder.encode(ark, StandardCharsets.UTF_8.name())
                        );
                        getContent(url, 200);

                        url = String.format("http://localhost:%d/%s/content/%s/%d/%s",
                                port,
                                cp,
                                URLEncoder.encode(ark, StandardCharsets.UTF_8.name()),
                                1,
                                URLEncoder.encode("producer/mrt-dc.xml", StandardCharsets.UTF_8.name())
                        );
                        getContent(url, 200);
                }

        }

        @Test
        public void TestReplicationExplictAddDelete() throws IOException, JSONException, InterruptedException, SQLException {
                resetReplication();
                int orig = getDatabaseVal(audit_count_sql, primaryNode, -1);
                addReplications();
                checkInvDatabase(audit_count_sql, "Confirm a complete replication from Primary Node to Repl Node", replNode, orig);
                deleteReplicationsFromNode(replNode, 200);
                checkInvDatabase(audit_count_sql, "Confirm replication cleared for Repl Node", replNode, 0);
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

        public void checkInvDatabase(String sql, String message, int n, String s, int value) throws SQLException {
                try(Connection con = DriverManager.getConnection(connstr, user, password)){
                        try (PreparedStatement stmt = con.prepareStatement(sql)){
                                stmt.setInt(1, n);
                                stmt.setString(2, s);
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

        public JSONObject startScan(int nodenum) throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/scan/start/%d?t=json", port, cp, nodenum);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertFalse(s.isEmpty());

                        JSONObject json =  new JSONObject(s);
                        assertNotNull(json);
                        assertTrue(json.has("repscan:invStorageScan"));
                        return json;
                }
        }

        public JSONObject scanStatus(int scanid) throws IOException, JSONException {
                String url = String.format("http://localhost:%d/%s/scan/status/%d?t=json", port, cp, scanid);
                try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost(url);
                        HttpResponse response = client.execute(post);
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        String s = new BasicResponseHandler().handleResponse(response).trim();
                        assertFalse(s.isEmpty());

                        JSONObject json =  new JSONObject(s);
                        assertNotNull(json);
                        assertTrue(json.has("repscan:invStorageScan"));
                        return json;
                }
        }

        public boolean testCompleted(JSONObject json) throws JSONException {
                return json.getJSONObject("repscan:invStorageScan").get("repscan:scanStatus").equals("completed");
        }

        public int runScan(int nodenum, int missing_file, int missing_ark, int non_ark) throws IOException, JSONException, SQLException, InterruptedException {
                int orig = getDatabaseVal(audit_count_sql, primaryNode, -1);

                JSONObject json = startScan(nodenum);
                int scanid = json.getJSONObject("repscan:invStorageScan").getInt("repscan:id");
                json = scanStatus(scanid);
                for(int i=0; i<10 && !testCompleted(json); i++) {
                        Thread.sleep(5000);
                        json = scanStatus(scanid);
                }
                assertTrue(testCompleted(json));
                assertTrue(json.getJSONObject("repscan:invStorageScan").getInt("repscan:keysProcessed") >= orig);

                checkInvDatabase(storage_maints_sql, "No scan results found", scanid, "missing-file", missing_file);
                checkInvDatabase(storage_maints_sql, "No scan results found", scanid, "missing-ark", missing_ark);
                checkInvDatabase(storage_maints_sql, "No scan results found", scanid, "non-ark", non_ark);
                return scanid;
        }

        @Test
        public void testScanPrimaryNode() throws IOException, JSONException, SQLException, InterruptedException {
                //Scan process only hits on files more than 1 day old.
                //This test will fail if the docker image has been recreated in the past day

                runUpdate(storage_maints_del_sql);

                // ark|1|producer/hello2.txt will not be found for all 3 objects
                // primary node has 2 additional files that will not be found
                runScan(primaryNode, 3, 1, 1);

                runUpdate(storage_maints_del_sql);
        }

        @Test
        public void testScanReplicationNode() throws IOException, JSONException, SQLException, InterruptedException {
                runUpdate(storage_maints_del_sql);

                resetReplication();
                letReplicationRun();

                runScan(replNode, 0, 0, 0);

                runUpdate(storage_maints_del_sql);
        }

        //@Test
        //TODO
        public void testScanDelete() {                
        }

        //@Test
        //TODO
        public void testScanAllow() {                
        }
}
