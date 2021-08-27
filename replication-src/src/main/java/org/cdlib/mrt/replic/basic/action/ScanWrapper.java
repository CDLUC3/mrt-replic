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

import java.sql.Connection;
import org.cdlib.mrt.core.DateState;

import org.cdlib.mrt.inv.content.InvStorageMaint;
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
    protected InvStorageMaint invMaint = null;
    
    protected DoScan doScan = null;
    protected DoScan.ScanInfo doScanInfo = null;
    protected String msg = null;
    protected String afterKey=null;
    protected int iteratesCnt = 0;
    protected int scanCnt = 0;
    
    protected final Long nodeNum;
    protected final Integer maxKeys;
    protected final Integer iterate;
    protected final Long threadSleep;
    protected volatile Boolean threadStop = false;
    protected RunStatus runStatus = RunStatus.initial;
    
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
            InvStorageMaint invMaint = InvDBUtil.getStorageMaintAdmin(nodeNumber, connection, logger);
            ScanWrapper scanWrapper = ScanWrapper.getScanWrapper(replicationInfo, db, invMaint, nodeNumber, 2000, 5, 500L, logger);
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
        
    public static void main_thread(String args[])
    {

        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            ReplicationRunInfo replicationInfo = new ReplicationRunInfo(config);
            InvStorageMaint invMaint = InvDBUtil.getStorageMaintAdmin(nodeNumber, connection, logger);
            ScanWrapper scanWrapper = ScanWrapper.getScanWrapper(replicationInfo, db, invMaint, nodeNumber, 5000, 5, null, logger);
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
            InvStorageMaint invMaint,
            Long nodeNum,
            int maxKeys,
            int iterate,
            Long threadSleep,
            LoggerInf logger)
        throws TException
    {
        ScanWrapper addWrapper = new ScanWrapper(replicationInfo,db, invMaint, nodeNum, maxKeys, iterate, threadSleep, logger);
        return addWrapper;
    }

    protected ScanWrapper(
            ReplicationRunInfo replicationInfo,
            DPRFileDB db,
            InvStorageMaint invMaint,
            Long nodeNum,
            int maxKeys,
            int iterate,
            Long threadSleep,
            LoggerInf logger)
        throws TException
    {
        this.replicationInfo = replicationInfo;
        this.invMaint = invMaint;
        this.nodeNum = nodeNum;
        this.db = db;
        this.afterKey = invMaint.getKey();
        this.maxKeys = maxKeys;
        this.iterate = iterate;
        this.threadSleep = threadSleep;
        this.threadStop = false;
        this.logger = logger;
    }
    

    @Override
    public void run()
    {
        try {
            runStatus = RunStatus.running;
            // connection here used for setup of DoScan
            Connection connection = db.getConnection(true);
            testConnect("db", connection);
            doScan = DoScan.getScan(nodeNum, connection, logger);
            Connection tconnect = doScan.getConnection();
            testConnect("initial", tconnect);
           
            try {
                connection.close();
            } catch (Exception ex) { }
            
            scanLog(2, "start");
            while(true) {
                boolean runContinue = iterates(doScan);
                iteratesCnt++;

                if ((threadSleep != null) && (threadSleep > 0)) {
                    scanLog(3, "Sleep=" + threadSleep);
                    Thread.sleep(threadSleep);
                }
                
                scanLog(5, "runContinue=" + runContinue);
                if (!runContinue) break;
            }
            runStatus = RunStatus.stopped;

        } catch(Exception ex)  {
            exception = ex;
            runStatus = RunStatus.failed;
    
        } finally {
        }
    }
    
    public boolean iterates(DoScan doScan)
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            doScan.setConnection(connection);
            System.out.println(MESSAGE + "Start processing DoScan:"
                    + " - nodeNum=" + nodeNum
                    + " - afterKey=" + afterKey
                    + " - maxKeys=" + maxKeys
                    + " - iterate=" + iterate
            );
            
            // create new connection to avoid connection timeout
            scanLog(5, "start iterations");
            for(int i=1; i <= iterate; i++) {
                /*
                connection = resetConnect(connection);
                doScan.setConnection(connection);
            Connection tconnect = doScan.getConnection();
            testConnect("iterate", tconnect);
*/
                doScanInfo = doScan.process(afterKey, maxKeys);
                scanCnt++;
                System.out.println(doScanInfo.dump("Count Dump:" + i));
                scanLog(8, doScanInfo.dump("Count Dump:" + i));
                afterKey = doScanInfo.getLastProcessKey();
                
                if (doScanInfo.isEof()) {
                    scanLog(2, "eof stop");
                    invMaint.setMaintAdminDB("eof");
                    invMaint.setKey(afterKey);
                    rewriteAdminMaint(connection);
                    runStatus = RunStatus.eof;
                    return false;
                }
                
                if (!replicationInfo.isRunReplication() || threadStop) {
                    scanLog(2, "replication stop");
                    invMaint.setMaintAdminDB("stop");
                    invMaint.setKey(afterKey);
                    rewriteAdminMaint(connection);
                    return false;

                }
                scanLog(8, "end iteration");
            }
            
            connection = doScan.getConnection();
            testConnect("after iterate", connection);
            scanLog(8, "complete iterations");
            invMaint.setKey(afterKey);
            rewriteAdminMaint(connection);
            scanLog(5, "end iterations");
            return true;
                
        } catch(Exception ex)  {
            exception = ex;
            ex.printStackTrace();
            return false;
    
        } finally {
            try {
                connection.close();
            } catch (Exception ex)  { }
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
    
    public void rewriteAdminMaint(Connection connection)
        throws TException
    {
        try {
            DBAdd dbAdd = new DBAdd(connection, logger);
            if (!connection.isValid(10)) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "rewriteAdminMaint connection not valid");
            }
            connection.setAutoCommit(true);
            long ismseq = dbAdd.update(invMaint);
            //connection.commit();
            scanLog(8, "rewriteAdminMaint ismseq:" + ismseq + " - " + invMaint.dump("rewriteAdminMaint"));
                
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

    public DoScan getDoScan() {
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

