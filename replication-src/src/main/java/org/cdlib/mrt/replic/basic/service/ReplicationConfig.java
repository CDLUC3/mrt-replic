/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/

package org.cdlib.mrt.replic.basic.service;
import java.util.List;
import java.util.Properties;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

import java.io.File;
import java.io.InputStream;
import java.util.LinkedHashMap;
import org.cdlib.mrt.core.DateState;


import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.tools.SSMConfigResolver;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.tools.YamlParser;
import org.cdlib.mrt.utility.LoggerAbs;
import org.json.JSONObject;

/**
 * Base properties for Inv
 * @author  dloy
 */

public class ReplicationConfig
{
    private static final String NAME = "ReplicationConfig";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
    protected JSONObject stateJSON = null;
    protected JSONObject serviceJSON = null;
    protected JSONObject cleanupJSON = null;
    protected JSONObject jdb = null;
//    protected DPRFileDB db = null;
    //protected FileManager fileManager = null;
    protected LoggerInf logger = null;
    protected boolean shutdown = true;
    protected static NodeIO nodeIO = null;
    protected Properties cleanupEmailProp = null;
    private static class Test{ };
    
    public static ReplicationConfig useYaml()
        throws TException
    {
        try {

            JSONObject replicInfoJSON = getYamlJson();
            ReplicationConfig replicationConfig = new ReplicationConfig(replicInfoJSON);
            
            return replicationConfig;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    protected ReplicationConfig(JSONObject replicInfoJSON) 
        throws TException
    {
        try {
            System.out.println("***getYamlJson:\n" + replicInfoJSON.toString(3));
            
            stateJSON = replicInfoJSON.getJSONObject("state");
            serviceJSON = replicInfoJSON.getJSONObject("service");
            cleanupJSON = replicInfoJSON.getJSONObject("cleanup");
            jdb = replicInfoJSON.getJSONObject("db");
            
            JSONObject jInvLogger = replicInfoJSON.getJSONObject("fileLogger");
            logger = setLogger(jInvLogger);
            setNodeIO();
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected static JSONObject getYamlJson()
       throws TException
    {
        try {
            String propName = "resources/replicConfig.yml";
            System.out.println("propName:" + propName);
            Test test=new Test();
            InputStream propStream =  test.getClass().getClassLoader().
                    getResourceAsStream(propName);
            String replicationYaml = StringUtil.streamToString(propStream, "utf8");
            System.out.println("replicationYaml:\n" + replicationYaml);
            String invInfoConfig = getYamlInfo();
            System.out.println("\n\n***table:\n" + invInfoConfig);
            String rootPath = System.getenv("SSM_ROOT_PATH");
            System.out.append("\n\n***root:\n" + rootPath + "\n");
            SSMConfigResolver ssmResolver = new SSMConfigResolver();
            YamlParser yamlParser = new YamlParser(ssmResolver);
            System.out.println("\n\n***InventoryYaml:\n" + replicationYaml);
            LinkedHashMap<String, Object> map = yamlParser.parseString(replicationYaml);
            LinkedHashMap<String, Object> lmap = (LinkedHashMap<String, Object>)map.get(invInfoConfig);
            if (lmap == null) {
                throw new TException.INVALID_CONFIGURATION(MESSAGE + "Unable to locate configuration");
            }
            //System.out.println("lmap not null");
            yamlParser.loadConfigMap(lmap);

            yamlParser.resolveValues();
            return yamlParser.getJson();
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected static String getYamlInfo()
       throws TException
    { 
        String invInfoConfig = System.getenv("which-replic-info");
        if (invInfoConfig == null) {
            invInfoConfig = System.getenv("MERRITT_REPLIC_INFO");
        }
        if (invInfoConfig == null) {
            invInfoConfig = "replic-info";
        }
        return invInfoConfig;
    }
    
    public void setNodeIO()
        throws TException
    {
        try {
            String nodeIOPath = serviceJSON.getString("nodePath");
            nodeIO = NodeIO.getNodeIOConfig(nodeIOPath, logger);
            setNodeIO(nodeIO);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public DPRFileDB startDB()
       throws TException
    {
        return startDB(logger);
    }
    
    protected ReplicationServiceState getServiceState()
        throws TException
    {
        try {
            ReplicationServiceState serviceState = new ReplicationServiceState(this);
            return serviceState;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public DPRFileDB startDB(LoggerInf logger)
       throws TException
    {
        try {
            /*
              "db": {
                "user": "invrw",
                "password": "ixxx",
                "url": "uc3db-inv-stg.cdlib.org",
                "name": "inv",
                "encoding": "OPTIONAL",
                "maxConnections": "OPTIONAL"
                }
            jdbc:mysql://uc3db-inv-stg.cdlib.org:3306/inv?characterEncoding=UTF-8&characterSetResults=UTF-8
            
    public DPRFileDB(LoggerInf logger,
            String dburl,
            String dbuser,
            String dbpass)
        throws TException
    {
        this.logger = logger;
        this.dburl = dburl;
        this.dbuser = dbuser;
        this.dbpass = dbpass;
        setPool();
    }
            */
            
            String  password = jdb.getString("password");
            String  user = jdb.getString("user");
            
            String server = jdb.getString("host");
            String encoding = jdb.getString("encoding");
            if (encoding.equals("OPTIONAL")) {
                encoding = "";
            } else {
                encoding = "?" + encoding;
            }
            String name = jdb.getString("name");
            String url = "jdbc:mysql://" + server + ":3306/" + name + encoding;
            DPRFileDB db = new DPRFileDB(logger, url, user, password);
            return db;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    /**
     * set local logger to node/log/...
     * @param path String path to node
     * @return Node logger
     * @throws Exception process exception
     */
    protected LoggerInf setLogger(JSONObject fileLogger)
        throws Exception
    {
        String qualifier = fileLogger.getString("qualifier");
        String path = fileLogger.getString("path");
        String name = fileLogger.getString("name");
        Properties logprop = new Properties();
        logprop.setProperty("fileLogger.message.maximumLevel", "" + fileLogger.getInt("messageMaximumLevel"));
        logprop.setProperty("fileLogger.error.maximumLevel", "" + fileLogger.getInt("messageMaximumError"));
        logprop.setProperty("fileLogger.name", fileLogger.getString("name"));
        logprop.setProperty("fileLogger.trace", "" + fileLogger.getInt("trace"));
        logprop.setProperty("fileLogger.qualifier", fileLogger.getString("qualifier"));
        if (StringUtil.isEmpty(path)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "setCANLog: path not supplied");
        }

        File canFile = new File(path);
        File log = new File(canFile, "logs");
        if (!log.exists()) log.mkdir();
        String logPath = log.getCanonicalPath() + '/';
        
        if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("LOG", logprop)
            + "\npath:" + path
            + "\nlogpath:" + logPath
        );
        LoggerInf logger = LoggerAbs.getTFileLogger(name, log.getCanonicalPath() + '/', logprop);
        return logger;
    }
    
    
    public Properties getCleanupEmailProp()
       throws TException
    {
        Properties cleanupEmailProp = new Properties();
        try {
            if (!setCleanupProp("subject", "emailSubject", cleanupEmailProp)) {
                return null;
            }
            if (!setCleanupProp("from", "emailFrom", cleanupEmailProp)) {
                return null;
            }
            if (!setCleanupProp("to", "emailTo", cleanupEmailProp)) {
                return null;
            }
            String msg = cleanupJSON.getString("msg");
            String msgArr[] = msg.split("\\n");
            for (int i=0; i<msgArr.length; i++) {
                int propVal = i+1;
                cleanupEmailProp.setProperty("ReplicCleanup.emailMsg." + propVal, msgArr[i]);
            }
            if (msgArr.length == 0) {
                return null;
            }
            return cleanupEmailProp;
  
        } catch (Exception ex) {
            return null;
        }
    }
    
    protected boolean setCleanupProp(String jsonKey, String propKey, Properties prop)
       throws TException
    {
        
        String base = "ReplicCleanup";
        try {
            String jValue = cleanupJSON.getString(jsonKey);
            if (jValue.equals("NONE")) return false;
            prop.setProperty(base + "." + propKey, jValue);
            return true;
            
        } catch (Exception ex) {
            return false;
        }
    }
    

    public LoggerInf getLogger() {
        return logger;
    }

    public JSONObject getStateJSON() {
        return stateJSON;
    }

    public JSONObject getServiceJSON() {
        return serviceJSON;
    }
    
    public void setLogger(LoggerInf logger) {
        this.logger = logger;
    }

    public JSONObject getJdb() {
        return jdb;
    }

    public void setJdb(JSONObject jdb) {
        this.jdb = jdb;
    }

    public static NodeIO getNodeIO() {
        return nodeIO;
    }

    public void setNodeIO(NodeIO nodeIO) {
        this.nodeIO = nodeIO;
    }
    
    public static void main(String[] argv) {
    	
    	try {
            
            LoggerInf logger = new TFileLogger("test", 50, 50);
            ReplicationConfig replicConfig = ReplicationConfig.useYaml();
            Properties setupProperties = replicConfig.getCleanupEmailProp();
            System.out.println(PropertiesUtil.dumpProperties("***Cleanup", setupProperties));
            //FileManager.printNodes("MAIN NODEIO");
            ServiceStatus serviceStatus = null;
            DPRFileDB db = replicConfig.startDB();
            if (db == null) serviceStatus = ServiceStatus.shutdown;
            else  serviceStatus = ServiceStatus.running;
            
            System.out.println("setDB dbStatus:" + serviceStatus);
            if (serviceStatus == ServiceStatus.running) {
                db.shutDown();
            }
            NodeIO nodeIO = replicConfig.getNodeIO();
            nodeIO.printNodes("test");
            validateNodeIO(nodeIO);
            
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    public static void validateNodeIO(NodeIO nodeIO) 
    {        
        try {
            
            nodeIO.printNodes("test");
            List<NodeIO.AccessNode> accessNodes = nodeIO.getAccessNodesList();
            System.out.println("\n###ValidateNodeIO - Number nodes:" + accessNodes.size());
            for (NodeIO.AccessNode accessNode : accessNodes) {
                CloudStoreInf service = accessNode.service;
                long node = accessNode.nodeNumber;
                String bucket = accessNode.container;
                System.out.println(">>>Start:"
                        + " - node=" + node
                        + " - bucket=" + bucket
                );
                try {
                    service.getState(bucket);
                    System.out.println(">>>End:"
                        + " - node=" + node
                        + " - bucket=" + bucket
                    );
                    
                } catch (Exception ex) {
                    System.out.println("Exception:" + ex);
                }
            }
            
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
}
