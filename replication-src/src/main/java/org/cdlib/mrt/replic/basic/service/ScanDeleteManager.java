
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


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.inv.service.VersionsState;
import org.cdlib.mrt.inv.service.Version;
import org.cdlib.mrt.inv.service.VFile;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.DoScan;
import org.cdlib.mrt.replic.basic.action.FileInput;
import org.cdlib.mrt.replic.basic.action.ReplaceWrapper;
import org.cdlib.mrt.replic.basic.action.ScanDeleteNode;
import org.cdlib.mrt.replic.basic.app.ReplicationServiceInit;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import static org.cdlib.mrt.replic.basic.service.ScanManager.getStorageScan;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.replic.utility.ScanUtil;
import static org.cdlib.mrt.replic.utility.ScanUtil.getLastScan;
import static org.cdlib.mrt.replic.utility.ScanUtil.writeStorageScan;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;

/**
 * Inv Service
 * @author  dloy
 */

public class ScanDeleteManager
{
    private static final String NAME = "ScanManager";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    private static final boolean THREADDEBUG = false;
    protected File keyListFile = null;
    //protected static int failMs = 2*60*60*1000;
    protected static int failMs = 10*60*1000;
    
    protected LoggerInf logger = null;
    protected Exception exception = null;
    protected DPRFileDB db = null;
    protected Integer maxkeys = null;
    protected Long threadSleep = null;
    protected ReplicationRunInfo replicationInfo = null;
    protected InvStorageScan activeScan = null;
    protected ScanDeleteNode scanDeleteNode = null;
    
    
    public static void main(String args[])
    {
        //main_format(args);
    }
    
    /*
    public static void main_format(String args[])
    {

        int scanNum = 11;
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            config.startDB();
            db = config.getDB();
            LoggerInf logger = new TFileLogger("DoScan", 9, 10);
            Connection connection = db.getConnection(true);
            InvStorageScan activeScan = ScanDeleteManager.getStorageScan(scanNum, connection, logger);
            String response = ScanDeleteManager.formatIt("json", logger, activeScan);
            JSONObject dobj = new JSONObject(response);
            System.out.println(dobj.toString(2));
            System.out.println("Test format:" + response);
            
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
                //System.out.println("db Exception:" + ex);
            }
        }
    }
    
    public static void main_list(String args[])
    {

        long nodeNumber = 9502;
        String keyList = "7001:scanlist/9502-210913.log";
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            config.startDB();
            ReplicationRunInfo info = new ReplicationRunInfo(config);
            ScanDeleteManager scanManager = config.getScanManager(info);
            info.setRunReplication(true);
            scanManager.process(nodeNumber, keyList);
            
            
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
                //System.out.println("db Exception:" + ex);
            }
        }
    }
    
    public static void main_list_restart(String args[])
    {

        int scanNum = 11;
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            config.startDB();
            ReplicationRunInfo info = new ReplicationRunInfo(config);
            ScanDeleteManager scanManager = config.getScanManager(info);
            info.setRunReplication(true);
            scanManager.restart(scanNum);
            
            
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
                //System.out.println("db Exception:" + ex);
            }
        }
    }
    
    public static void main_start(String args[])
    {

        long nodeNumber = 5001;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            config.startDB();
            ReplicationRunInfo info = new ReplicationRunInfo(config);
            ScanDeleteManager scanManager = config.getScanManager(info);
            info.setRunReplication(true);
            scanManager.process(nodeNumber, null);
            
            
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
                //System.out.println("db Exception:" + ex);
            }
        }
    }
    
    public static void main_restart(String args[])
    {

        int scanNum = 9;
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            config.startDB();
            ReplicationRunInfo info = new ReplicationRunInfo(config);
            ScanDeleteManager scanManager = config.getScanManager(info);
            info.setRunReplication(true);
            scanManager.restart(scanNum);
            
            
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
                //System.out.println("db Exception:" + ex);
            }
        }
    }
    */
    public static ScanDeleteManager getScanDeleteManager( //!!! for testing only - replicationInfo not set
            ReplicationConfig config,
            LoggerInf logger)
        throws TException
    {
        try { 
            ReplicationRunInfo replicationInfo = new ReplicationRunInfo(config);
            ScanDeleteManager scanManager = config.getScanDeleteManager(replicationInfo);
            return scanManager;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public ScanDeleteManager(
            DPRFileDB db,
            ReplicationRunInfo replicationInfo,
            Integer maxkeys, 
            Long threadSleep,
            LoggerInf logger)
        throws TException
    {
        this.db = db;
        this.replicationInfo = replicationInfo;
        this.maxkeys = maxkeys;
        this.threadSleep = threadSleep;
        this.logger = logger;
        this.db = db;
        System.out.println("ScanDeleteManager:" 
                + " - maxkeys=" + maxkeys
                + " - threadSleep=" + threadSleep
        );
    }
    
    public InvStorageScan process(Long nodeNum)
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            activeScan = getActiveDeleteScan(nodeNum,connection);
            if (activeScan.getScanStatus() == InvStorageScan.ScanStatus.started) {
                System.out.println("return running scan");
                return activeScan;
            }
            if (activeScan.getScanStatus() == InvStorageScan.ScanStatus.pending) {
                
                System.out.println("start new scan:" + nodeNum);
                startDeleteScan(nodeNum);
                return activeScan;
            }
            throw new TException.INVALID_ARCHITECTURE(MESSAGE + "ScanStatus not started or pending:" + activeScan.getScanStatus());
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        } finally {
            ReplicDB.closeConnect(connection);
        }
        
    }
    
    public InvStorageScan restartDelete(int scanNum)
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            activeScan = ScanUtil.getStorageScan(scanNum, connection, logger);
            if (activeScan == null) {
                throw new TException.INVALID_OR_MISSING_PARM("ScanID not found:" + scanNum);
            }
            if (isActiveScan(activeScan)) {
                return activeScan;
            }
            doResetScan(activeScan, connection);
            Long nodeNum = InvDBUtil.getNodeNumber(activeScan.getNodeid(), connection, logger);
            System.out.println("Recovered nodeNum:" + nodeNum);
            startDeleteScan(nodeNum);
            return activeScan;
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
            
        } finally {
            ReplicDB.closeConnect(connection);
        }
    }
    
    public InvStorageScan getActiveDeleteScan(Long nodeNum, Connection connection)
        throws TException
    {
        InvStorageScan activeScan = null;
        try {
            List<InvStorageScan> scanList = ScanUtil.getStorageScanStatus(nodeNum, "started", connection, logger);
            if (scanList == null) {
                activeScan = getNewDeleteScan(nodeNum,connection);
                return activeScan;
            }
            activeScan = manageActiveDeleteScan(scanList, connection, logger);
            if (activeScan == null) {
                activeScan = getNewDeleteScan(nodeNum, connection);
                return activeScan;
            }
            return activeScan;
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
    
    public InvStorageScan isActive(Long nodeNum, String keyList)
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            activeScan = getActiveDeleteScan(nodeNum, connection);
            if (activeScan.getScanStatus() == InvStorageScan.ScanStatus.started) {
                System.out.println("return running scan");
                return activeScan;
            }
            throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "ScanStatus not started for:" + nodeNum);
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        } finally {
            ReplicDB.closeConnect(connection);
        }
        
    }

    public void startDeleteScan(Long nodeNum)
        throws TException
    {
        try {
            System.out.println(PropertiesUtil.dumpProperties("startScan", activeScan.retrieveProp()));
            scanDeleteNode = ScanDeleteNode.getScanDeleteNode(replicationInfo, db, activeScan, nodeNum, maxkeys, threadSleep, logger);
            if (!replicationInfo.isAllowScan()) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE 
                        + "Scan may not currently allowed: replicationInfo.isAllowScan=" + replicationInfo.isAllowScan());
            }
            if (false) {
                //scanWrapper.run();
                
                return;
            }
            Thread wrapperThread = new Thread(scanDeleteNode);
            wrapperThread.start();
            Thread.currentThread().sleep(100);
            
            return;
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
    
    public InvStorageScan getNewDeleteScan(Long nodeNum, Connection connection)
        throws TException
    {
        InvStorageScan activeScan = null;
        try {
            String scanType = "delete";
            activeScan = buildInitDeleteStorageScan(nodeNum, connection, logger);
            return activeScan;
            
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
    
    public void doResetScan(InvStorageScan activeScan, Connection connection)
        throws TException
    {
        try {
            String scanType = "delete";
            String activeType = activeScan.getScanType().toString();
            if (!scanType.equals(activeType)) {
               throw new TException.INVALID_ARCHITECTURE(MESSAGE + "getResetScan-discrpency resetScan and scanType:"
                       + "dbType=" + activeType
                       + "expected Type=" + scanType
               );
            }
            ScanUtil.resetStorageScanStatus("pending", activeScan, connection, logger);
            
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
    
    public InvStorageScan manageActiveDeleteScan(List<InvStorageScan> scanList, Connection connection, LoggerInf logger)
        throws TException
    {
        InvStorageScan activeScan = null;
        ArrayList<InvStorageScan> activeScans = new ArrayList<>();
        try {
            int numScans = scanList.size();
            for (InvStorageScan scan : scanList) {
                if (scan.getScanStatus() == InvStorageScan.ScanStatus.started) {
                    if (!isActiveScan(scan)) {
                        System.out.println(PropertiesUtil.dumpProperties("CANCELLED", scan.retrieveProp()));
                        scan.setScanStatusDB("cancelled");
                        ScanUtil.writeStorageScan(scan, connection, logger);
                    } else {
                        if (scan.getScanType() == InvStorageScan.ScanType.delete) {
                            activeScans.add(scan);
                        } else {
                            throw new TException.INVALID_CONFIGURATION(MESSAGE 
                                    + "Delete may not run during active scan:" + scan.getId());
                        }
                    }
                }
            }
            if (activeScans.size() == 0) return null;
            if (activeScans.size() == 1) {
                activeScan = activeScans.get(0);
                return activeScan;
            }
            throw new TException.INVALID_CONFIGURATION(MESSAGE + "Only one active scan per node:" + activeScans.size());
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
    
    public boolean isActiveScan(InvStorageScan scan)
        throws TException
    {
        try {
            
            if (scan.getScanStatus() != InvStorageScan.ScanStatus.started) return false;
            DateState current = new DateState();
            long currentMs = current.getTimeLong();
            DateState updated = scan.getUpdated();
            long updatedMs = updated.getTimeLong();
            long durationMs = currentMs - updatedMs;
            log(3, "isActiveScan:"
                    + " - failMs=" + failMs
                    + " - durationMs=" + durationMs);
            if ((durationMs) > failMs) {
                return false;
            }
            return true;
                
        }  catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
        }
        
    }
    
    public void setKeyListFile(String keyList)
        throws TException
    {
        Long nodeNum = null;
        if (keyList == null) return;
        try {
            String [] parts = keyList.split("\\s*\\:\\s*",2);
            if (parts.length != 2) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Invalid keyList - form <node>:<storage key>:" + keyList);
            }
            try {
                nodeNum = Long.parseLong(parts[0]);
            } catch (Exception nex) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Invalid node in keylist:" + parts[0]);
            }
            NodeIO nodeIO = ReplicationConfig.getNodeIO();
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeNum);
            
            if (accessNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Node not supported:" + nodeNum);
            }
            String key = parts[1];
            keyListFile = FileUtil.getTempFile("scanfile" + nodeNum, ".txt");
            NodeService nodeService = new NodeService(accessNode, nodeNum, logger);
            CloudResponse cloudResponse = CloudResponse.get(nodeService.getBucket(), key);
            nodeService.getObject(key, keyListFile, cloudResponse);
            Exception ex = cloudResponse.getException();
            if (ex != null) {
                if (ex instanceof TException) {
                    throw (TException) ex;
                } else {
                    throw ex;
                }
            }
            
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }

    public ReplicationRunInfo getReplicationInfo() {
        return replicationInfo;
    }
    
     

    public static String formatIt(
            String formatTypeS,
            LoggerInf logger,
            StateInf responseState)
    {
        try {
            if (responseState == null) {
               return "NULL RESPONSE";
            }
            FormatterInf.Format formatType = null;
            formatType = FormatterInf.Format.valueOf(formatTypeS);
            FormatterInf formatter = FormatterAbs.getFormatter(formatType, logger);
            ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
            PrintStream  stream = new PrintStream(outStream, true, "utf-8");
            formatter.format(responseState, stream);
            stream.close();
            byte [] bytes = outStream.toByteArray();
            String retString = new String(bytes, "UTF-8");
            return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        }
    }
    
    public static InvStorageScan buildInitDeleteStorageScan(
            long nodeNum, 
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        try {
            
            InvStorageScan scan = getLastScan(nodeNum, "started", connection, logger);
            if (scan != null) {
                throw new TException.INVALID_OR_MISSING_PARM("started scan found:" + scan.getId());
            }
            Long nodeid = InvDBUtil.getNodeSeq(nodeNum,connection, logger);
            if (nodeid == null) {
                throw new TException.INVALID_OR_MISSING_PARM("Scan node invalid:" + nodeNum);
            }
            
            InvStorageScan storageScan = new InvStorageScan(logger);
            storageScan.setNodeid(nodeid);
            storageScan.setUpdatedDB();
            storageScan.setScanStatusDB("pending");
            storageScan.setScanTypeDB("delete");
            storageScan.setLastS3Key(" ");
            storageScan.setKeyListName(null);
            writeStorageScan(storageScan, connection, logger);
            
            
            scan = getLastScan(nodeNum, "pending", connection, logger);
            if (scan == null) {
                throw new TException.INVALID_OR_MISSING_PARM("created  scan not found" );
            }
            return scan;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex ;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
            
        }
    }
    
    private void log(int lvl, String msg) 
    {
        logger.logMessage(msg, lvl, true);
        if (lvl <= 3) System.out.println(msg);
    }
}

