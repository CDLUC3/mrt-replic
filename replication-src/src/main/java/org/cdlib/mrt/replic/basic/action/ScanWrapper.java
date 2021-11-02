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
package org.cdlib.mrt.replic.basic.action;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import org.cdlib.mrt.core.DateState;

import org.cdlib.mrt.inv.content.InvStorageMaint;
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Run fixity
 * @author dloy
 */
public class ScanWrapper
        implements Runnable
{

    public enum RunStatus {running, stopped, eof, failed, started, runReplicationOff, initial};
    
    protected static final String NAME = "ScanWrapper";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
    
    protected DPRFileDB db = null;
    protected LoggerInf logger = null;
    protected ReplicationRunInfo replicationInfo = null;
    protected Exception exception = null;
    protected InvStorageScan invStorageScan = null;
    
    protected DoScanBase doScan = null;
    protected DoScanBase.ScanInfo doScanInfo = null;
    protected File keyFile = null;
    protected String msg = null;
    protected String afterKey=null;
    protected int iteratesCnt = 0;
    protected int scanCnt = 0;
    
    protected final Long nodeNum;
    protected final Integer maxKeys;
    protected final Long threadSleep;
    protected volatile Boolean threadStop = false;
    protected RunStatus runStatus = RunStatus.initial;
    protected long keysProcessed = 0;
    
    public static void main(String args[])
    {

        long nodeNumber = 5001;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            testConnect("main", connection);
            ReplicationRunInfo replicationInfo = new ReplicationRunInfo(config);
            replicationInfo.setRunReplication(true);
            InvStorageScan invStorageScan = ScanWrapper.getLastScan(nodeNumber, "started", connection, logger);
            if (invStorageScan == null) {
                invStorageScan = ScanWrapper.buildInitStorageScan(nodeNumber, "next", null, connection, logger);
            }
            
            ScanWrapper scanWrapper = ScanWrapper.getScanWrapper(replicationInfo, db, invStorageScan, nodeNumber, 2000, 500L, logger, null);
            
            scanWrapper.run();
            System.out.println("end");
            
            
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            try {
                //db.shutDown();
            } catch (Exception ex) {
                System.out.println("db Exception:" + ex);
            }
        }
    }
    public static ScanWrapper getScanWrapper(           
            ReplicationRunInfo replicationInfo,
            DPRFileDB db,
            InvStorageScan invStorageScan,
            long nodeNum,
            Integer maxKeys,
            Long threadSleep,
            LoggerInf logger,
            File keyFile)
        throws TException
    {
        ScanWrapper addWrapper = new ScanWrapper(replicationInfo,db, invStorageScan, nodeNum, maxKeys,threadSleep, logger, keyFile);
        return addWrapper;
    }

    protected ScanWrapper(ReplicationRunInfo replicationInfo,
            DPRFileDB db,
            InvStorageScan invStorageScan,
            long nodeNum,
            Integer maxKeys,
            Long threadSleep,
            LoggerInf logger,
            File keyFile)
        throws TException
    {
        this.replicationInfo = replicationInfo;
                if (replicationInfo == null) System.out.println("runReplication null zero");
        this.invStorageScan = invStorageScan;
        this.nodeNum = nodeNum;
        this.db = db;
        this.maxKeys = maxKeys;
        this.afterKey = invStorageScan.getLastS3Key();
        this.keyFile = keyFile;
        if (invStorageScan.getKeysProcessed() == null) invStorageScan.setKeysProcessed(0L);
        this.keysProcessed = invStorageScan.getKeysProcessed();
        System.out.print("KEYSPROCESSED:" + this.keysProcessed);
        this.threadSleep = threadSleep;
        this.threadStop = false;
        this.logger = logger;
        if (invStorageScan.getScanType() == InvStorageScan.ScanType.list) {
            if (keyFile == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "keyFile required");
            }
            setScanList();
            
        } else if (invStorageScan.getScanType() == InvStorageScan.ScanType.next) {
            setScanNext();
        }
    }
    
    protected void setScanList()
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            DoScanList doScanList = DoScanList.getScanList(nodeNum,
                    invStorageScan.getId(),
                    keyFile,
                    invStorageScan.getLastS3Key(),
                    invStorageScan.getKeysProcessed(),
                    connection,
                    logger);
            doScan = doScanList;
            
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
    
    protected void setScanNext()
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            DoScanNext doScanNext = DoScanNext.getScanNext(nodeNum,
                    invStorageScan.getId(),
                    invStorageScan.getLastS3Key(),
                    keysProcessed,
                    connection,
                    logger);
            doScan = doScanNext;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex ;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
            
        }
        
    }

    @Override
    public void run()
    {
        
        Connection runConnect = null;
        Connection connection = null;
        try {
            runStatus = RunStatus.running;
            runConnect = db.getConnection(true);
            
            scanLog(2, "start");
            invStorageScan.setScanStatusDB("started");
            writeStorageScan(invStorageScan, runConnect, logger);
            scanCnt = 0;
            while(true) {
                connection = db.getConnection(true);
                
                if (DEBUG) testConnect("whilestart", connection);
                doScanInfo = doScan.process(maxKeys, connection);
                scanCnt++;
                if (DEBUG) System.out.println(doScanInfo.dump("***Count Dump:" + scanCnt));
                scanLog(8, doScanInfo.dump("Count Dump:" + scanCnt));
                afterKey = doScanInfo.getLastProcessKey();
                if (DEBUG) System.out.println("(" + scanCnt + "):replicationInfo.isAllowScan()=" + replicationInfo.isAllowScan());
                if (doScanInfo.isEof()) {
                    scanLog(2, "eof stop");
                    invStorageScan.setScanStatusDB("completed");
                    invStorageScan.setLastS3Key(afterKey);
                    invStorageScan.setKeysProcessed(doScanInfo.getLastScanCnt());
                    rewriteStorageScan(runConnect);
                    runStatus = RunStatus.eof;
                    System.out.println("ScanWrapper eof break");
                    log(2, "ScanWrapper Exit(" + invStorageScan.getId() + "," + scanCnt + "): eof");
                    break;
                }
                
                if (replicationInfo == null) System.out.println("runReplication null 3");
                if (!replicationInfo.isAllowScan() || threadStop) {
                    scanLog(2, "replication stop");
                    invStorageScan.setScanStatusDB("cancelled");
                    invStorageScan.setLastS3Key(afterKey);
                    invStorageScan.setKeysProcessed(doScanInfo.getLastScanCnt());
                    rewriteStorageScan(runConnect);
                    System.out.println("ScanWrapper stop break");
                    log(2, "ScanWrapper Exit(" + invStorageScan.getId() + "," + scanCnt + "): stop");
                    break;

                }
                
                InvStorageScan testStorageScan = InvDBUtil.getStorageScan(invStorageScan.getId(), connection, logger);
                if (testStorageScan.getScanStatus() == InvStorageScan.ScanStatus.cancelled) {
                    scanLog(2, "replication db cancelled");
                    invStorageScan.setScanStatusDB("cancelled");
                    invStorageScan.setLastS3Key(afterKey);
                    invStorageScan.setKeysProcessed(doScanInfo.getLastScanCnt());
                    rewriteStorageScan(runConnect);
                    System.out.println("ScanWrapper stop cancelled");
                    log(2, "ScanWrapper Exit(" + invStorageScan.getId() + "," + scanCnt + "): db cancel");
                    break;
                    
                }
                invStorageScan.setLastS3Key(afterKey);
                invStorageScan.setKeysProcessed(doScanInfo.getLastScanCnt());
                rewriteStorageScan(runConnect);
                scanLog(8, "end iteration");

                if ((threadSleep != null) && (threadSleep > 0)) {
                    scanLog(3, "Sleep=" + threadSleep);
                    Thread.sleep(threadSleep);
                }
                if (connection != null) {
                    try {
                        connection.close();
                        if (DEBUG) System.out.println(">>>Connection closed:" + scanCnt);
                    } catch (Exception ex) { }
                }
                
               
                //if (scanCnt > 3) break;
            }
            runStatus = RunStatus.stopped;

        } catch(Exception ex)  {
            exception = ex;
            runStatus = RunStatus.failed;
            System.out.println("ScanWrapper Exception:" + ex);
            ex.printStackTrace();
    
        } finally {
            try {
                connection.close();
            } catch (Exception xx) { }
            try {
                runConnect.close();
            } catch (Exception xx) { }
        }
    }
    
    
    public static void testConnect(String header, Connection connection)
        throws Exception
    {
        if (connection == null) {
            System.out.println("CONNECT - " + header + ": connection null");
            return;
        }
        if (connection.isClosed()) {
            System.out.println(header + ": connection closed");
            return;
        }
        
        System.out.println(header + ": connection open");
    }
    
    public Connection resetConnect(Connection connection)
        throws TException
    {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) { }
        }
        return db.getConnection(true);
    }
    
    public void rewriteStorageScan(Connection connection)
        throws TException
    {
        try {
            DBAdd dbAdd = new DBAdd(connection, logger);
            if (!connection.isValid(10)) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "rewriteAdminMaint connection not valid");
            }
            connection.setAutoCommit(true);
            invStorageScan.setUpdatedDB();
            long ismseq = dbAdd.update(invStorageScan);
            //connection.commit();
            scanLog(8, "rewriteAdminMaint ismseq:" + ismseq + " - " + invStorageScan.dump("rewriteAdminMaint"));
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public static InvStorageScan buildInitStorageScan(
            long nodeNum, 
            String scanType, 
            String keyListName, 
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        try {
            if ((scanType != null) && scanType.equals("list") && StringUtil.isAllBlank(keyListName)) {
                throw new TException.INVALID_OR_MISSING_PARM("scanType list requires a keyListName");
            }
            
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
            storageScan.setScanTypeDB(scanType);
            storageScan.setLastS3Key(" ");
            storageScan.setKeyListName(keyListName);
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

    public static InvStorageScan getLastScan(
            long nodeNum,
            String scanStatus,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        List<InvStorageScan> scanList = getStorageScanStatus(nodeNum, scanStatus, connection, logger);
        if (scanList == null) {
            System.out.println("scanList null");
            return null;
        }
        int last = (scanList.size() - 1);
        System.out.println("**getLastScan:" + last);
        InvStorageScan scan = scanList.get(last);
        return scan;
    }

    public static List<InvStorageScan> getStorageScanStatus(
            long nodeNum,
            String scanStatus,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return InvDBUtil.getStorageScanStatus(nodeNum, scanStatus, connection, logger);
    }

    public static void resetStorageScanStatus(
            String scanStatus,
            InvStorageScan storageScan,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        storageScan.setScanStatusDB(scanStatus);
        writeStorageScan(storageScan, connection, logger);
    }
    
    public static void writeStorageScan(InvStorageScan storageScan, Connection connection, LoggerInf logger)
        throws TException
    {
        try {
            DBAdd dbAdd = new DBAdd(connection, logger);
            if (!connection.isValid(10)) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "rewriteAdminMaint connection not valid");
            }
            connection.setAutoCommit(true);
            long ismseq = dbAdd.insert(storageScan);
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }

    public RunStatus getRunStatus() {
        return runStatus;
    }

    public int getIteratesCnt() {
        return iteratesCnt;
    }

    public int getScanCnt() {
        return scanCnt;
    }

    public Long getNodeNum() {
        return nodeNum;
    }

    public Integer getMaxKeys() {
        return maxKeys;
    }

    public void setRunStatus(RunStatus runStatus) {
        this.runStatus = runStatus;
    }

    public Boolean getThreadStop() {
        return threadStop;
    }

    public DoScanBase getDoScan() {
        return doScan;
    }

    public void setThreadStop(Boolean threadStop) {
        this.threadStop = threadStop;
    }
    
    private void log(int lvl, String msg)
    {
        try {
            //System.out.println("LOG:" + msg);
            logger.logMessage(msg, lvl, true);
        } catch (Exception ex) { System.out.println("log exception"); }
    }
    
    private void scanLog(int lvl, String msg)
    {
        String prtException = "";
        if (exception != null) {
            prtException = " - exception=" + exception.toString();
        }
        log(lvl,MESSAGE + msg
                + " - node=" + nodeNum
                + " - scanCnt=" + scanCnt
                + " - iterateCnt=" + iteratesCnt
                + " - afterKey=" + afterKey
                + " - exception=" + prtException
        );
    }
}

