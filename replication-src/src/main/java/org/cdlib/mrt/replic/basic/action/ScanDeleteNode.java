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
import org.cdlib.mrt.replic.utility.ReplicDBUtil;
import org.cdlib.mrt.replic.utility.ScanUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Run fixity
 * @author dloy
 */
public class ScanDeleteNode
        implements Runnable
{

    public enum RunStatus {running, stopped, eof, failed, started, runReplicationOff, initial};
    
    protected static final String NAME = "ScanDeleteNode";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
    
    protected DPRFileDB db = null;
    protected LoggerInf logger = null;
    protected ReplicationRunInfo replicationInfo = null;
    protected Exception exception = null;
    protected InvStorageScan invStorageScan = null;
    protected String msg = null;
    protected int maxKeys = 0;
    
    protected final Long nodeNum;
    protected final Long threadSleep;
    protected boolean scanEof = false;
    protected volatile Boolean threadStop = false;
    protected RunStatus runStatus = RunStatus.initial;
    protected ScanDeleteS3Gen scanDeleteS3Gen = null;
    
    public static void main(String args[])
    {

        long nodeNumber = 5001;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            ScanUtil.testConnect("main", connection);
            ReplicationRunInfo replicationInfo = new ReplicationRunInfo(config);
            replicationInfo.setRunReplication(true);
            InvStorageScan invStorageScan = ScanUtil.getLastScan(nodeNumber, "started", connection, logger);
            if (invStorageScan == null) {
                invStorageScan = ScanWrapper.buildInitStorageScan(nodeNumber, "next", null, connection, logger);
            }
            
            ScanDeleteNode scanWrapper = ScanDeleteNode.getScanDeleteNode(replicationInfo, db, invStorageScan, nodeNumber, 2000, 500L, logger);
            
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
    public static ScanDeleteNode getScanDeleteNode(           
            ReplicationRunInfo replicationInfo,
            DPRFileDB db,
            InvStorageScan invStorageScan,
            long nodeNum,
            Integer maxKeys,
            Long threadSleep,
            LoggerInf logger)
        throws TException
    {
        ScanDeleteNode addWrapper = new ScanDeleteNode(replicationInfo,db, invStorageScan, nodeNum, maxKeys, threadSleep, logger);
        return addWrapper;
    }

    protected ScanDeleteNode(         
            ReplicationRunInfo replicationInfo,
            DPRFileDB db,
            InvStorageScan invStorageScan,
            long nodeNum,
            Integer maxKeys,
            Long threadSleep,
            LoggerInf logger)
        throws TException
    {
        this.replicationInfo = replicationInfo;
                if (replicationInfo == null) System.out.println("runReplication null zero");
        this.invStorageScan = invStorageScan;
        this.nodeNum = nodeNum;
        this.db = db;
        if (invStorageScan.getKeysProcessed() == null) invStorageScan.setKeysProcessed(0L);
        if (maxKeys == null) this.maxKeys = 500;
        else this.maxKeys = maxKeys;
        this.threadSleep = threadSleep;
        this.threadStop = false;
        this.logger = logger;
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
            rewriteStorageScan(runConnect);
            while(true) {
                connection = db.getConnection(true);
                scanDeleteS3Gen = ScanDeleteS3Gen.getScanDeleteS3(
                    nodeNum,
                    connection,
                    logger);
                processBlock(connection);
                
                if (scanEof) {
                    scanLog(2, "eof stop");
                    invStorageScan.setScanStatusDB("completed");
                    rewriteStorageScan(runConnect);
                    runStatus = RunStatus.eof;
                    System.out.println("ScanDeleteNode eof break");
                    log(2, "ScanDeleteNode Exit(" + invStorageScan.getId() + "," + invStorageScan.getKeysProcessed() + "): eof");
                    break;
                }
                
                if (replicationInfo == null) System.out.println("runReplication null 3");
                if (!replicationInfo.isAllowScan() || threadStop) {
                    scanLog(2, "replication stop");
                    invStorageScan.setScanStatusDB("cancelled");
                    rewriteStorageScan(runConnect);
                    System.out.println("ScanDeleteNode stop break");
                    log(2, "ScanDeleteNode Exit(" + invStorageScan.getId() + "," + invStorageScan.getKeysProcessed() + "): stop");
                    break;

                }
                
                InvStorageScan testStorageScan = InvDBUtil.getStorageScan(invStorageScan.getId(), connection, logger);
                if (testStorageScan.getScanStatus() == InvStorageScan.ScanStatus.cancelled) {
                    scanLog(2, "replication db cancelled");
                    invStorageScan.setScanStatusDB("cancelled");
                    rewriteStorageScan(runConnect);
                    System.out.println("ScanDeleteNode stop cancelled");
                    log(2, "ScanDeleteNode Exit(" + invStorageScan.getId() + "," + invStorageScan.getKeysProcessed() + "): db cancel");
                    break;
                    
                }
                rewriteStorageScan(runConnect);
                scanLog(8, "end iteration");

                if ((threadSleep != null) && (threadSleep > 0)) {
                    scanLog(3, "Sleep=" + threadSleep);
                    Thread.sleep(threadSleep);
                }
                if (connection != null) {
                    try {
                        connection.close();
                        if (DEBUG) System.out.println(">>>Connection closed:" + invStorageScan.getKeysProcessed());
                    } catch (Exception ex) { }
                }
                
               
                //if (scanCnt > 3) break;
            }
            runStatus = RunStatus.stopped;

        } catch(Exception ex)  {
            exception = ex;
            runStatus = RunStatus.failed;
            System.out.println("ScanDeleteNode Exception:" + ex);
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
    
    

    
    public void processBlock(
            Connection connection)
        throws TException
    {
        try {
            if (connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            List<InvStorageMaint> maints = ReplicDBUtil.getMaintsStatus(InvStorageMaint.MaintStatus.delete, nodeNum, maxKeys, connection, logger);
            
            if (maints == null) {
                System.out.println("EOF set");
                scanEof = true;
                return;
            }
            log(15, "entryList=" + maints.size());
            for (InvStorageMaint maint: maints) {
                deleteEntry(maint, connection);
            }
            if (maints.size() < maxKeys) {
                System.out.println("EOF set");
                scanEof = true;
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void deleteEntry(
            InvStorageMaint invStorageMaint,
            Connection connection
    )
        throws TException
    {
        try {
            if (connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            scanDeleteS3Gen.delete(invStorageMaint);
            if (invStorageMaint.getMaintStatus() == InvStorageMaint.MaintStatus.removed) {
                bumpKeyCnt(1, invStorageScan);
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
        
    public void rewriteStorageScan(Connection connection)
        throws TException
    {
        try {
            ScanUtil.writeStorageScan(invStorageScan, connection, logger);
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public void bumpKeyCnt(
            int val,
            InvStorageScan invStorageScan
    )
    {
        Long processCnt = invStorageScan.getKeysProcessed();
        if (processCnt == null) processCnt = 0L;
        processCnt += val;
        invStorageScan.setKeysProcessed(processCnt);
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

    public RunStatus getRunStatus() {
        return runStatus;
    }
    
    public void setRunStatus(RunStatus runStatus) {
        this.runStatus = runStatus;
    }

    public Boolean getThreadStop() {
        return threadStop;
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
                + " - deleteCnt=" + invStorageScan.getKeysProcessed()
                + " - exception=" + prtException
        );
    }
}

