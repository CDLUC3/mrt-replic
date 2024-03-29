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
import java.util.Properties;

import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.ThreadHandler;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;


import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.ReplicEmailWrapper;
import org.cdlib.mrt.replic.basic.action.ReplaceWrapper;
import org.cdlib.mrt.replic.basic.action.CollectionNodeHandler;
import org.cdlib.mrt.replic.basic.action.DeleteInv;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.basic.action.DeleteSecondary;
import org.cdlib.mrt.replic.basic.action.Match;
import org.cdlib.mrt.replic.basic.action.NodesObjects;
import org.cdlib.mrt.replic.basic.action.Replicator;
import org.cdlib.mrt.replic.basic.action.ReplicAddInv;
import org.cdlib.mrt.replic.basic.action.ReplicCleanup;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;

/**
 * Base properties for Replication
 * @author  dloy
 */

public class ReplicationServiceHandler
{
    private static final String NAME = "ReplicationServiceHandler";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;

    protected int terminationSeconds = 600;
    //protected Properties serviceProperties = null;
    protected ReplicationConfig replicConfig = null;
    protected File replicationServiceF = null;
    protected File replicationInfoF = null;
    protected DPRFileDB db = null;
    protected LoggerInf logger = null;
    protected ReplicationServiceStateManager serviceStateManager = null;
    protected ReplicationRunInfo replicationInfo = null;
    protected ExecutorService replicationExecutor = null;
    protected NodeIO nodes = null;
    protected boolean shutdown = true;
    protected ThreadHandler addQueue = null; 

    public static ReplicationServiceHandler getReplicationServiceHandler(ReplicationConfig replicConfig)
        throws TException
    {
        return new ReplicationServiceHandler(replicConfig);
    }

    protected ReplicationServiceHandler(ReplicationConfig replicConfig)
        throws TException
    {
        try {
            this.replicConfig = replicConfig;
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties(MESSAGE + "setupProperties", this.replicConfig.cleanupEmailProp));
            logger = replicConfig.getLogger();
            
            serviceStateManager 
                    = ReplicationServiceStateManager.getReplicationServiceStateManager(replicConfig);
            //ReplicationServiceState serviceState = serviceStateManager.retrieveBasicServiceState();
            replicationInfo = new ReplicationRunInfo(replicConfig);

            ReplicationScheme scheme = replicationInfo.getReplicationScheme();
            if (scheme != null) {
                scheme.buildNamasteFile(replicationServiceF);
            }
            nodes = replicConfig.getNodeIO();
            nodes.printNodes(NAME);
            addQueue = ThreadHandler.getThreadHandler(30000, 3, logger);
            startup();

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public DPRFileDB getNewDb()
        throws TException
    {
        return replicConfig.startDB(logger);
    }

    public void refresh()
        throws TException
    {
        try {
            replicationInfo.set();

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public ReplicationServiceState getReplicationServiceState()
        throws TException
    {
        Connection connection = null;
        try {

            if (DEBUG) System.out.println("*********IS "
                    + " - SHUTDOWN:" + isShutdown()
                    + " - replicationInfo.isRunReplication():" + replicationInfo.isReplicationSQL()
                    + " - replicationInfo.isReplicationProcessing():" + replicationInfo.isReplicationProcessing()
                    ); //!!!!
            
            
            connection = getConnection(true);
            ServiceStatus runStatus = getRunStatus();
            ReplicationServiceState state = 
                    serviceStateManager.getReplicationServiceState(connection, runStatus);
            state.setAllowScan(replicationInfo.isAllowScan());
            return state;

        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
            try {
                connection.close();
            } catch (Exception cex) { }
        }
    }
    
    public ReplicationServiceState getReplicationServiceStatus()
        throws TException
    {
        try {

            if (DEBUG) System.out.println("*********IS "
                    + " - SHUTDOWN:" + isShutdown()
                    + " - replicationInfo.isRunReplication():" + replicationInfo.isReplicationSQL()
                    + " - replicationInfo.isReplicationProcessing():" + replicationInfo.isReplicationProcessing()
                    ); //!!
            ServiceStatus runStatus = getRunStatus();
            ReplicationServiceState state = 
                    serviceStateManager.getReplicationServiceStatus(runStatus, replicationInfo);
            state.setAddQueueCnt(activeAddQueue());
            if (DEBUG) System.out.println(MESSAGE + "isAllowScan:" + replicationInfo.isAllowScan());
            state.setAllowScan(replicationInfo.isAllowScan());
            return state;

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected ServiceStatus getRunStatus()
    {
               
        if (replicationInfo.isRunReplication()) {
            if (!replicationInfo.isReplicationProcessing()) {
                return ServiceStatus.unknown;
            } else {
                return ServiceStatus.running;
            }
        } else {
            if (replicationInfo.isReplicationProcessing()) {
                return ServiceStatus.shuttingdown;
            } else {
                return ServiceStatus.shutdown;
            }
        }
    }

    public DPRFileDB getDb() {
        return db;
    }

    public File getReplicationInfo() {
        return replicationInfoF;
    }

    public File getReplicationService() {
        return replicationServiceF;
    }

    public ReplicationRunInfo getReplicationRunInfo() {
        return replicationInfo;
    }

    public LoggerInf getLogger() {
        return logger;
    }

    public ReplicationServiceStateManager getServiceStateManager() {
        return serviceStateManager;
    }

    public void dbShutdown()
        throws TException
    {
        if (db == null) return;
        db.shutDown();
        db = null;
        replicationInfo.setReplicationSQL(false);
    }

    public void dbStartup()
        throws TException
    {
        if (db != null) return;
        db = getNewDb();
        replicationInfo.setReplicationSQL(true);
    }

    public boolean isSQL()
        throws TException
    {
        if ((db != null)) {
            if (replicationInfo.isReplicationSQL()) return true;
            Connection testConnection = db.getConnection(true);
            try {
                if (testConnection.isValid(20)) {
                    replicationInfo.setReplicationSQL(true);
                    return true;
                } else {
                    return false;
                }
            } catch (Exception ex) {
                return false;
            } finally {
                try {
                    testConnection.close();
                } catch (Exception ex) { }
            }
        } else {
            return false;
        }
        
    }
    
    /**
     * Add replications based on primary node
     * @param nodeNum primary node
     * @param objectID object to copy
     * @return
     * @throws TException 
     */
    public ReplicationAddState addObject(
            Identifier objectID)
        throws TException
    {
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("add attempted - MySQL not running");
            }
            Connection connect = getConnection(true);
            InvNodeObject invNodeObject = InvDBUtil.getNodeObjectPrimary(objectID, connect, logger);
            if (invNodeObject == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object not found=" + objectID.getValue());
            }
            try {
                connect.close();
            } catch (Exception ex) {}
            System.out.println(PropertiesUtil.dumpProperties("Add IN:" + objectID.getValue(), invNodeObject.retrieveProp()));
            Replicator replicator =  Replicator.getReplicator(invNodeObject, nodes, db, logger);
            replicator.run();
            if (replicator.getException() != null)  {
                Exception ex = replicator.getException();
                if (ex instanceof TException) {
                    throw ex;
                } else {
                    throw new TException(ex);
                }
            }
            System.out.println(PropertiesUtil.dumpProperties(MESSAGE + "addObject completed:"+ objectID.getValue(), invNodeObject.retrieveProp()));
            return replicator.getResult();
            
        } catch (TException tex) {
            System.out.println("Add exception - "+ objectID.getValue() + ":" + tex);
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } 
    }
    
    /**
     * Add replications based on primary node
     * @param nodeNum primary node
     * @param objectID object to copy
     * @return
     * @throws TException 
     */
    public ReplicationAddState addReplicInv(
            Identifier objectID)
        throws TException
    {
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("add attempted - MySQL not running");
            }
            Connection connect = getConnection(true);
            InvNodeObject invNodeObject = InvDBUtil.getNodeObjectPrimary(objectID, connect, logger);
            if (invNodeObject == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object not found=" + objectID.getValue());
            }
            try {
                connect.close();
            } catch (Exception ex) {}
            System.out.println(PropertiesUtil.dumpProperties("Add IN:" + objectID.getValue(), invNodeObject.retrieveProp()));
            ReplicAddInv replicAddInv =  ReplicAddInv.getReplicAddInv(invNodeObject, nodes, db, logger);
            replicAddInv.run();
            if (replicAddInv.getException() != null)  {
                Exception ex = replicAddInv.getException();
                if (ex instanceof TException) {
                    throw ex;
                } else {
                    throw new TException(ex);
                }
            }
            System.out.println(PropertiesUtil.dumpProperties(MESSAGE + "addObject completed:"+ objectID.getValue(), invNodeObject.retrieveProp()));
            return replicAddInv.getResult();
            
        } catch (TException tex) {
            System.out.println("Add exception - "+ objectID.getValue() + ":" + tex);
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } 
    }
    
    public ReplicationDeleteState deleteObjectNode(
            boolean doDeleteStore,
            boolean deleteInvWhenStoreMissing,
            int nodeNum,
            Identifier objectID)
        throws TException
    {
        
        Connection connect = null;
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("delete attempted - MySQL not running");
            }
            connect = getConnection(true);
            InvNodeObject invNodeObject = InvDBUtil.getNodeObject(nodeNum, objectID, connect, logger);
            if (invNodeObject == null) {
                ReplicationDeleteState nullDeleteState =  new ReplicationDeleteState(deleteInvWhenStoreMissing, objectID, 0);
                return nullDeleteState;
            }
            if (invNodeObject.getRole() == Role.primary) {
                throw new TException.INVALID_OR_MISSING_PARM("Replication delete may not be used on primary data"
                        + " - nodeNum=" + nodeNum
                        + " - objectID=" + objectID.getValue()
                );
            }
            System.out.println(PropertiesUtil.dumpProperties("DELETE IN", invNodeObject.retrieveProp()));
            ReplicNodesObjects replicNodesObject = new ReplicNodesObjects(invNodeObject, nodeNum, logger);
            Deletor deletor = Deletor.getDeletor(replicNodesObject, deleteInvWhenStoreMissing, connect, nodes, logger);
            deletor.setDoDeleteStore(doDeleteStore);
            deletor.run();
            Exception se = deletor.getSaveException();
            if (se != null) {
                if (se instanceof TException) {
                    throw se;

                }  else {
                    se.printStackTrace();
                    throw new TException(se);
                }
            }
            
            
            ReplicationDeleteState deleteState = deletor.getReturnState();
            deleteState.addSecondaryNodes(replicNodesObject);
            deleteState.bumpDeleteCnt();
            return deleteState;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception cex) { }
        }
    }
    
    public ReplicationDeleteState delete(
            int nodeNum,
            Identifier objectID)
        throws TException
    {
        
        Connection connect = null;
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("delete attempted - MySQL not running");
            }
            connect = getConnection(true);
            InvNodeObject invNodeObject = InvDBUtil.getNodeObject(nodeNum, objectID, connect, logger);
            if (invNodeObject == null) {
                ReplicationDeleteState nullDeleteState =  new ReplicationDeleteState(false, objectID, 0);
                return nullDeleteState;
            }
            if (invNodeObject.getRole() == Role.primary) {
                throw new TException.INVALID_OR_MISSING_PARM("Replication delete may not be used on primary data"
                        + " - nodeNum=" + nodeNum
                        + " - objectID=" + objectID.getValue()
                );
            }
            /*
            ArrayList<InvCollectionNode> list = InvDBUtil.getCollectionNodes(objectID, nodeNum, connect, logger);
            if (list != null) {
                InvCollectionNode entry = list.get(0);
                throw new TException.INVALID_OR_MISSING_PARM(
                        "Replication delete on node may not be performed "
                        + "if existing collections-nodes entry for object"
                        + " - nodeNum=" + nodeNum
                        + " - objectID=" + objectID.getValue()
                        + " " + entry.dump("found entry")
                );
            }
            */
            DeleteSecondary delete = DeleteSecondary.getDeleteSecondary(objectID, db, nodes, logger);
            long nodeNumL = nodeNum;
            delete.setSingleNode(nodeNumL);
            ReplicationDeleteState state = delete.process();
            return state;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception cex) { }
        }
    }
    
    public ReplicationDeleteState deleteSecondary(
            Identifier objectID)
        throws TException
    {
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("delete attempted - MySQL not running");
            }
            DeleteSecondary delete = DeleteSecondary.getDeleteSecondary(objectID, db, nodes, logger);
            ReplicationDeleteState state = delete.process();
            return state;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public NodesObjectsState getStateSecondary(
            Identifier objectID)
        throws TException
    {
        Connection connect = null;
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("delete attempted - MySQL not running");
            }
            connect = getConnection(true);
            NodesObjects nos = NodesObjects.getNodeObjects(objectID, connect, logger);
            NodesObjectsState state = nos.getNodesObjectState();
            return state;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception cex) { }
        }
    }
    
    
    /**
     * Match duplicated objects on different nodes
     * @param sourceNode input node
     * @param targetNode copied or virtual  node
     * @param objectID object identifier
     * @return results of match
     * @throws TException 
     */
    public MatchObjectState matchObjects(
            int sourceNode,
            Integer targetNode,
            Identifier objectID)
        throws TException
    {
        Connection connect = null;
        try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("matchObjects attempted - MySQL not running");
            }
            
            connect = getConnection(true);
            Match match = Match.getMatch(objectID, nodes,sourceNode,targetNode, connect, logger);
            
            MatchObjectState objectState = match.process();
            return objectState;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception e) { }
        }
    }
    
    public ReplicationPropertiesState doCleanup()
        throws TException
    {
        
        if (!isSQL()) {
            throw new TException.SQL_EXCEPTION("matchObjects attempted - MySQL not running");
        }
        try {
            Properties setupProperties = replicConfig.getCleanupEmailProp();
            System.out.println(PropertiesUtil.dumpProperties(NAME+":doCleanup", setupProperties));
            ReplicCleanup rc = ReplicCleanup.getReplicCleanup(setupProperties, nodes, db, logger);
            System.out.println("from:" + rc.getEmailFrom());
            System.out.println("to:" + rc.getEmailTo());
            System.out.println("subject:" + rc.getEmailSubject());
            System.out.println("msg:" + rc.getEmailMsg());
            ReplicEmailWrapper cleanupWrapper = new ReplicEmailWrapper(
                rc,
                true,
                rc.getEmailTo(),
                rc.getEmailFrom(),
                rc.getEmailSubject(),
                rc.getEmailMsg(),
                "xml",
                db,
                setupProperties,
                logger);
            
            ExecutorService threadExecutor = Executors.newFixedThreadPool( 1 );
            threadExecutor.execute(cleanupWrapper ); // start task1
            threadExecutor.shutdown();
            Thread.sleep(3000);
            Properties retProp = new Properties();
            retProp.setProperty("operation", "Replication Cleanup");
            retProp.setProperty("submmitted", DateUtil.getCurrentIsoDate());
            retProp.setProperty("mailto", rc.getEmailTo());
            Properties [] retProps = new Properties[1];
            retProps[0] = retProp;
            ReplicationPropertiesState retState = new ReplicationPropertiesState(retProps);
            return retState;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public ReplicationAddMapState addMap(
            Identifier collectionID,
            int nodeNum)
        throws TException
    {       try {
            if (!isSQL()) {
                throw new TException.SQL_EXCEPTION("delete attempted - MySQL not running");
            }
            Connection connect = getConnection(false);
            CollectionNodeHandler collectionMap = CollectionNodeHandler.getCollectionNodeHandler(connect, logger);
            long nodeObjectCount = collectionMap.process(collectionID, nodeNum);
            InvCollectionNode map = collectionMap.getInvCollectionNode();
            ReplicationAddMapState state = new ReplicationAddMapState(collectionID, nodeNum, nodeObjectCount, map);
            return state;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public Connection getConnection(boolean autoCommit)
        throws TException
    {
        if (db == null) return null;
        return db.getConnection(autoCommit);
    }

    public boolean isShutdown() {
        if (db == null) return true;
        return shutdown;
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
        if (DEBUG) System.out.println("*********SET SHUTDOWN:" + isShutdown()); //!!!!
    }
    
    public void setAllowScan(boolean allow)
    {
        replicationInfo.setAllowScan(allow);
    }
    
    public void pauseReplication()
        throws TException
    {
        if (replicationExecutor == null) {
            replicationInfo.setReplicationProcessing(false);
            return;
        }
        replicationInfo.setRunReplication(false);
        for(int cnt=0; cnt<terminationSeconds; cnt++) {
            if (!replicationInfo.isReplicationProcessing()) break;
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                break;
            }
        }
        
        try {
            replicationExecutor.shutdown();
            replicationExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            replicationExecutor = null;
            
        } catch (Exception ex) {
            System.out.println("***Replication termination fails");
        }
    }
    
    public void startupReplication()
        throws TException
    {
        if (!isSQL()) {
            throw new TException.SQL_EXCEPTION(MESSAGE + "startupReplication: MySQL down");
        }
        System.out.println("startup Replication called");
        replicationInfo.setRunReplication(true);
        RunReplication test = new RunReplication(replicationInfo, nodes, db, logger);
        replicationExecutor = Executors.newFixedThreadPool( 1 );
        replicationExecutor.execute( test ); // start task1
    }
    
    public void shutdownReplication()
        throws TException
    {
        if (replicationExecutor == null) {
            replicationInfo.setReplicationProcessing(false);
            return;
        }
        replicationInfo.setRunReplication(false);
        stopAddQueue();
        for(int cnt=0; cnt<terminationSeconds; cnt++) {
            if (!replicationInfo.isReplicationProcessing()) break;
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
                break;
            }
        }
        
        try {
            replicationExecutor.shutdown();
            replicationExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            replicationExecutor = null;
            
        } catch (Exception ex) {
            System.out.println("***Replication termination fails");
        }
    }
    
    public void startScan()
        throws TException
    {
        replicationInfo.setAllowScan(true);
    }
    
    public void stopScan()
        throws TException
    {
        replicationInfo.setAllowScan(false);
    }
    
    public void startup()
        throws TException
    {
        dbStartup();
        setShutdown(false);
        //startupReplication();
    }

    public NodeIO getNodes() {
        return nodes;
    }
    
    public void shutdown()
        throws TException
    {
        shutdownReplication();
        dbShutdown();
        setShutdown(true);
    }

    public void stopAddQueue() 
        throws TException
    {
        addQueue.shutdown();
    }

    public void newAddQueue(ReplaceWrapper add) {
        addQueue.runThread(add);
    }

    public int activeAddQueue() 
        throws TException
    {
        return addQueue.getActiveCnt();
    }

    public ReplicationConfig getReplicConfig() {
        return replicConfig;
    }
    
}
