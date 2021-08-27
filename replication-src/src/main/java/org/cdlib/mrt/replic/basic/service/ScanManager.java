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


import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvStorageMaint;
import org.cdlib.mrt.inv.service.VersionsState;
import org.cdlib.mrt.inv.service.Version;
import org.cdlib.mrt.inv.service.VFile;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.DoScan;
import org.cdlib.mrt.replic.basic.action.ScanWrapper;
import org.cdlib.mrt.replic.basic.action.FileInput;
import org.cdlib.mrt.replic.basic.action.ReplaceWrapper;
import org.cdlib.mrt.replic.basic.app.ReplicationServiceInit;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;

/**
 * Inv Service
 * @author  dloy
 */

public class ScanManager
{
    private static final String NAME = "ScanManager";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    private static final boolean THREADDEBUG = false;
    protected ArrayList<Long> scanNodes = null;
    
    protected HashMap<Long,NodeScan> nodeScanMap = new HashMap<>();
    protected LoggerInf logger = null;
    protected Exception exception = null;
    protected DPRFileDB db = null;
    protected Integer maxkeys = null;
    protected Integer iterations = null;
    protected Long threadSleep = null;
    protected ReplicationRunInfo replicationInfo = null;
    
    public static void main(String args[])
    {

        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            ScanManager scanManager = getScanManager(config, logger);
            
            
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            try {
                db.shutDown();
            } catch (Exception ex) {
                System.out.println("db Exception:" + ex);
            }
        }
    }
    public static ScanManager getScanManager(
            ReplicationConfig config,
            LoggerInf logger)
        throws TException
    {
        try { 
            JSONObject scanJSON = config.getScanJSON();
            DPRFileDB db = config.getDB();
            if (db == null) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "Database unavailable at this time");
            }
            ReplicationRunInfo replicationInfo = new ReplicationRunInfo(config);
            
            if (scanJSON == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "scanJSON missing");
            }
            System.out.println("scanJSON=" + scanJSON.toString(2));
            ArrayList<Long> scanNodes = getScanNodes(scanJSON);
            Integer maxkeys = scanJSON.getInt("maxkeys");
            Integer iterations = scanJSON.getInt("iterations");
            Long threadSleep = scanJSON.getLong("threadSleep");
            ScanManager scanManager = new ScanManager(scanNodes, db, replicationInfo, maxkeys, iterations, threadSleep, logger);
            return scanManager;
                    
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }

    protected ScanManager(
            ArrayList<Long> scanNodes,
            DPRFileDB db,
            ReplicationRunInfo replicationInfo,
            Integer maxkeys, 
            Integer iterations, 
            Long threadSleep,
            LoggerInf logger)
        throws TException
    {
        this.scanNodes = scanNodes;
        this.db = db;
        this.maxkeys = maxkeys;
        this.iterations = iterations;
        this.threadSleep = threadSleep;
        this.logger = logger;
        this.db = db;
        setup();
    }
    
    protected static ArrayList<Long> getScanNodes(JSONObject scanJSON)
        throws TException
    {
        ArrayList<Long> scanNodes = new ArrayList<>();
        try { 
            if (scanJSON == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "scanJSON missing");
            }
            
            String nodes = scanJSON.getString("nodes");
            String parts[] = nodes.split("\\s*\\;\\s*");
            for (String part : parts) {
                try {
                    Long node = Long.parseLong(part);
                    System.out.println("Add node:" + node);
                    scanNodes.add(node);
                    
                } catch (Exception ex) {
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "node invalid:" + part);
                }
            }
            if (scanNodes.size() == 0) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "scanNodes not found");
            }
            return scanNodes;       
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public void setup()
        throws TException
    {
        Connection connection = null;
        try { 
            connection = db.getConnection(true);
            System.out.println("valid:" + connection.isValid(10));
            for (long scanNode: scanNodes) {
                nodeSetup(scanNode, connection);
            }
            
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) { }
            }
        }
    }
    
    protected void nodeSetup(long nodeNum, Connection connection)
        throws TException
    {
        try { 
            NodeScan nodeScan = new NodeScan(nodeNum);
            nodeScanMap.put(nodeNum, nodeScan);
            nodeScan.adminStoreMaint = getStoreMaint(nodeNum, connection);
            setScanWrapper(nodeNum, nodeScan);
            
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public InvStorageMaint getStoreMaint(long nodeNum, Connection connection)
        throws TException
    {
        try { 
            System.out.println("getNodeAdmin valid:" + connection.isValid(10));
            InvStorageMaint storeMaint = InvDBUtil.getStorageMaintAdmin(nodeNum, connection, logger);
            Long nodeid = InvDBUtil.getNodeSeq(nodeNum,connection, logger);
            if (storeMaint == null) {
                storeMaint = new InvStorageMaint(logger);
                storeMaint.setAdmin(nodeid);
                connection.setAutoCommit(true);
                DBAdd dbAdd = new DBAdd(connection, logger);
                connection.setAutoCommit(true);
                long ismseq = dbAdd.insert(storeMaint);
                //connection.commit();
                System.out.println("added " + nodeNum + " as " + ismseq);
            } else {
                System.out.println("getNodeAdmin found:" + storeMaint.getId());
            }
            return storeMaint;
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    protected void setScanWrapper(Long nodeNum, NodeScan nodeScan)
        throws TException
    {
        InvStorageMaint storageMaint = nodeScan.adminStoreMaint;
        try { 
            nodeScan.wrapper = ScanWrapper.getScanWrapper(replicationInfo, db, storageMaint, nodeNum,
                    maxkeys, iterations, threadSleep, logger);
            nodeScan.wrapperThread = new Thread(nodeScan.wrapper);
            
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public ScanWrapper.RunStatus startDoScan(Long nodeNum)
        throws TException
    {
        Connection connection = null;
        ScanWrapper.RunStatus scanStatus = null;
        try { 
            NodeScan nodeScan = nodeScanMap.get(nodeNum);
            if (nodeScan == null) {
                throw new TException.INVALID_OR_MISSING_PARM("startDoScan not supported for "+ nodeNum);
            }
            ScanWrapper scanWrapper = nodeScan.wrapper;
            if (scanWrapper != null) {
                DoScan doScan = scanWrapper.getDoScan();
                if (doScan != null) {
                    nodeScan.ex = doScan.getException();
                }
            }
            if (!replicationInfo.isRunReplication()) {
                return ScanWrapper.RunStatus.runReplicationOff;
            }
            Thread wrapperThread = nodeScan.wrapperThread;
            if (wrapperThread != null) {
                if (wrapperThread.isAlive()) {
                    return ScanWrapper.RunStatus.running;
                }
            }
            setScanWrapper(nodeNum, nodeScan);
            wrapperThread = nodeScan.wrapperThread;
            wrapperThread.start();
            Thread.currentThread().sleep(100);
            scanWrapper = nodeScan.wrapper;
            return scanWrapper.getRunStatus();
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }

    public HashMap<Long, NodeScan> getNodeScanMap() {
        return nodeScanMap;
    }

    public NodeScan getNodeScan(Long nodeNum) {
        if (nodeNum == null) return null;
        return nodeScanMap.get(nodeNum);
    }
    
    public static class NodeScan
    {
        public long nodeNum = 0;
        public Thread wrapperThread = null;
        public ScanWrapper wrapper = null;
        public InvStorageMaint adminStoreMaint = null;
        public Exception ex = null;
        public NodeScan(long nodeNum)
        {
            this.nodeNum = nodeNum;
        }
        
    }
}

