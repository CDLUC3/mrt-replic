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
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;

import org.cdlib.mrt.replic.basic.action.ReplicationWrapper;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.ThreadHandler;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.inv.utility.InvUtil;
import org.cdlib.mrt.replic.basic.action.ReplicCleanup;
import org.cdlib.mrt.replic.basic.content.CopyNodes;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

import org.cdlib.mrt.s3.service.NodeIO;

/**
 * This class performs the overall fixity functions.
 * Fixity runs as a background thread.
 * 
 * Fixity uses a relational database (here MySQL) to process the oldest entries 
 * first. These entries are pulled in as blocks. Each block is then processed and
 * the results are then collected before the next block is started.
 * 
 * The fixity tests whether either the extracted file has changed either size or digest.
 * Any change results in error information being saved to the db entry for that test.
 * 
 * Note that ReplicationState contains 2 flags used for controlling fixity handling:
 * 
 *  runFixity - this flag controls whether to start or stop fixity
 *            - true=fixity should be running or starting to run
 *            - false=stop fixity and exit routine
 *  fixityProcessing - this flag determines if fixity is running
 *            - true=fixity is now running
 *            - false=fixity has stopped
 * @author dloy
 */

public class RunReplication implements Runnable
{
    private static final String NAME = "RunReplic";
    private static final String MESSAGE = NAME + ": ";

    private static final boolean DEBUG = false;
    protected LoggerInf logger = null;
    protected DPRFileDB db = null;
    protected long maxout = 100000000;
    protected Exception exception = null;
    //protected ThreadHandler threads = null;
    protected String runSQL = null;


    protected int capacity = 250;
    protected LinkedList<InvNodeObject> queue = null;
    protected ReplicationRunInfo replicationInfo = null;
    protected NodeIO nodes = null;
    protected int poolSize = 4;

    /**
     * Top lever routine controlling fixity handling
     * @param replicationInfo state controlling starting and stopping fixity plus process
     * features such as number of threads
     * @param rewriteEntry special class to dynamically modify URLs used for extracting
     * content to be fixity checked
     * @param db database handler
     * @param logger Merritt logger
     * @throws TException - Merritt process exception
     */
    public RunReplication(
            ReplicationRunInfo replicationInfo,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        this.replicationInfo = replicationInfo;
        this.logger = logger;
        this.nodes = nodes;
        this.db = db;
        String replicQualify = replicationInfo.getReplicQualify();
        if (StringUtil.isAllBlank(replicQualify)) {
            replicQualify = "";
        }
        queue = new LinkedList<InvNodeObject>();
        runSQL = "select distinct inv_nodes_inv_objects.* "
                    + "from " + InvNodeObject.NODES_OBJECTS + "," 
                            + InvNodeObject.COLLECTION_NODES + "," 
                            + InvNodeObject.COLLECTIONS_OBJECTS + " "
                    + "where inv_collections_inv_objects.inv_collection_id = inv_collections_inv_nodes.inv_collection_id "
                    + "and inv_nodes_inv_objects.inv_object_id = inv_collections_inv_objects.inv_object_id "
                    + "and inv_nodes_inv_objects.role = 'primary' "
                    + "and inv_nodes_inv_objects.replicated is null "
                    + " " + replicQualify + " "
                    + "limit " + capacity + ";";
  
        System.out.println(MESSAGE + "runSQL=" + runSQL);
        poolSize = replicationInfo.getThreadPool();
    }



    protected void addEntry(InvNodeObject nodeObj, Connection connection)
        throws TException
    {
        try {
            if (!resetReplicated(nodeObj, connection, logger)) {
                if (DEBUG) System.out.println(nodeObj.dump("***Entry fails add***"));
                return;
            }
            queue.add(nodeObj);

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    /**
     * Extract the oldest block of untested entries and add entries to a queue for
     * testing
     * @return number of entries extracted
     * @throws TException 
     */
    protected int addSQLEntries()
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(true);
            
            /* Original
            String sql = "select * "
                    + "from " + InvNodeObject.NODES_OBJECTS + " "
                    + "where role='primary' "
                    + "and replicated is null "
                    + "limit " + capacity + ";";
            */
            
            Properties [] props = DBUtil.cmd(connection, runSQL, logger);
            if ((props == null) || (props.length==0)) return 0;
            for (Properties propEntry : props) {
                if (!replicationInfo.isRunReplication()) break;
                InvNodeObject nodeObj = new InvNodeObject(propEntry, logger);
                System.out.println(PropertiesUtil.dumpProperties("***addSQLEntries***", propEntry));
                addEntry(nodeObj, connection);
            }
            System.out.println("***addSQLEntries completed successfully");
            
            log("END ADD");
            return queue.size();

        } catch (TException fe) {
            queue.clear();
            System.out.println("***addSQLEntries exception:" + fe);
            throw fe;

        } catch(Exception e)  {
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
            queue.clear();
            System.out.println("***addSQLEntries exception:" + e);
            throw new TException(e);

        } finally {
            try {
                connection.close();
            } catch (Exception ex) {}
        }
    }
    
    protected void endSQLEntries()
        throws TException
    {
        Connection connection = null;
        try {
            connection = db.getConnection(false);
            while (true) {
                InvNodeObject nodeObj = getEntry();
                if (nodeObj == null) break;
                resetReplicatedRetry(nodeObj, connection);
            }

        } catch (TException fe) {
            throw fe;

        } catch(Exception e)  {
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
            throw new TException(e);

        } finally {
            try {
                connection.commit();
                connection.close();
            } catch (Exception ex) {}
        }
    }

    /**
     * Thread run method used for handling the background thread handling
     */
    @Override
    public void run()
    {
        try {
            replicationInfo.setReplicationProcessing(true);
            Thread.sleep(5);
            for (long outcnt=0; outcnt < maxout; outcnt += capacity) {
                log("PROCESS BLOCK:" + outcnt);
                processBlock();
                if (!replicationInfo.isRunReplication()) {
                    log("SHUTDOWN detected");
                    break;
                }
            }
            log("************leaving RunReplica");

        } catch (TException fe) {
            fe.printStackTrace();
            setEx(fe);

        } catch(Exception e)  {

            e.printStackTrace();
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
            setEx(e);

        } finally {
            replicationInfo.setReplicationProcessing(false);
            System.out.println("************END RunReplica");
        }
    }

    /**
     * This method handles the overall processing of a queued set of entries.
     * The entries are processed by a dynamically allocated fixed set of threads.
     *
     * @throws TException 
     */
    protected void processBlock()
        throws TException
    {
        try {
            if (addSQLEntries() == 0) {
                log("No ITEM content - sleep 1 minute");
                Thread.sleep(60000);
                return;
            }
            
            ThreadHandler localThreads = ThreadHandler.getThreadHandler(250, poolSize, logger);
            log("Thread pool count:" + localThreads.getThreadCnt());
            if (!replicationInfo.isRunReplication()) return;
            for(int i = 0; i < capacity; i++){
                if (!replicationInfo.isRunReplication()) break;
                InvNodeObject nodeObject = getEntry();
                if (nodeObject == null) break;
                sleepInterval(nodeObject);
                log("PROCESS:" + nodeObject.getObjectsid());
                ReplicationWrapper wrapper
                        = new ReplicationWrapper(nodeObject, replicationInfo, nodes, db, logger);
                localThreads.runThread(wrapper);
            }
            localThreads.shutdown();
            endSQLEntries();
            log("************Termination of threads");

        } catch (TException fe) {
            fe.printStackTrace();
            throw fe;

        } catch(Exception e)  {

            e.printStackTrace();
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
            throw new TException(e);
        }
    }

    /**
     * This routine throttles fixity by using two type of intervals. 
     * The queue sleep interval issues a thread sleep between the process 
     * of each queued entry. This can be used to spread out the verification dates if
     * they are too clumped.
     * 
     * The standard interval is the minimum number of days between fixity tests being
     * performed on a single entry. This can be used to spread out the verification dates if
     * they are too clumped.
     * 
     * @param entry to be processed
     * @throws TException 
     */
    protected void sleepInterval(InvNodeObject nodeObject)
        throws TException
    {
        return;
        /*
        try {
            if (!replicationInfo.isRunReplication()) return;

            long queueSleep = replicationInfo.getQueueSleepMs();
            if (queueSleep > 0) {
                long queueSleepMilleseconds = queueSleep;
                log("sleepInterval - "
                    + " - queueSleep=" + queueSleep
                    + " - queueSleepMilleseconds=" + queueSleepMilleseconds
                    );
                Thread.sleep(queueSleepMilleseconds);
            }
            long intervalDay = replicationInfo.getIntervalDays();
            if (intervalDay == 0) return;
            long intervalSeconds = intervalDay * (24*60*60);
            long entryTime = nodeObject.getBackup().getTimeLong();
            log("sleepInterval - "
                    + " - intervalDay=" + intervalDay
                    + " - intervalSeconds=" + intervalSeconds
                    + " - entryTime=" + entryTime
                    + " - currentTime=" + new DateState().getTimeLong()
                    );
            while (true) {
                if (!replicationInfo.isRunReplication()) return;
                long currentTime = new DateState().getTimeLong();
                if ((entryTime + intervalSeconds) > currentTime) return;
                Thread.sleep(30000);
            }
        } catch (Exception ex) {
            throw new TException(ex);
        }
        */
    }

    /**
     * Return entry from queue and modify the entry if a rewrite rule is applied.
     * @return entry to be processed - rewrite applied if necessary
     * @throws TException 
     */
    protected InvNodeObject getEntry()
        throws TException
    {
        try {
            if (queue.size() < 1) {
                return null;
            }
            InvNodeObject nodeObject = queue.pop();
            return nodeObject;

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public boolean resetReplicated(InvNodeObject nodeObj, Connection connection, LoggerInf logger)
    {
        DateState zeroDate = new DateState(28800000);
        long id = nodeObj.getId();
        String initialDate = InvUtil.getDBDate(zeroDate);
        nodeObj.setReplicated(zeroDate);
        
        String sql = "update inv_nodes_inv_objects "
            + "set replicated = '" + initialDate + "' "
            + "where id=" + id + " "
            + "and replicated is null ";
        
        try {
            int updates = DBUtil.update(connection, sql, logger);
            System.out.println("***updates(" + id + "):" + updates);
        
            if (updates == 1) {
                log(nodeObj.dump("ADD"));
                return true;

            } 
            if (DEBUG) System.out.println("***Update fails:" + sql);
            return false;

        } catch (Exception ex) {
            return false;
        }
    }
    
    protected boolean resetReplicatedStart(InvNodeObject primaryNodeObject, Connection connection)
    {
        return ReplicDB.resetReplicatedDayOne(connection, primaryNodeObject, logger);
    }
    
    protected boolean resetReplicatedRetry(InvNodeObject primaryNodeObject)
    {
        return ReplicDB.resetReplicatedNull(db, primaryNodeObject, logger);
    }
    
    protected boolean resetReplicatedRetry(InvNodeObject primaryNodeObject, Connection connection)
    {
        return ReplicDB.resetReplicatedNull(connection, primaryNodeObject, logger);
    }
    
    protected void log(String msg)
    {
        log(msg,15);
    }
    
    protected void log(String msg, int lvl)
    {
        logger.logMessage(msg, lvl, true);
        if (!DEBUG) return;
        System.out.println(MESSAGE + msg);
    }

    public Exception getEx() {
        return exception;
    }

    public void setEx(Exception ex) {
        this.exception = ex;
    }


}
