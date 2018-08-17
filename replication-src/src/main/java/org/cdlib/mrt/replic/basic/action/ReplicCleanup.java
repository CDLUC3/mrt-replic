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
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Callable;
/*
import org.cdlib.mrt.audit.db.FixityItemDB;
import org.cdlib.mrt.audit.db.FixityMRTEntry;
import org.cdlib.mrt.audit.service.FixitySelectState;
import org.cdlib.mrt.audit.utility.FixityDBUtil;
import org.cdlib.mrt.audit.db.InvAudit;
import org.cdlib.mrt.audit.service.FixityServiceProperties;
*/
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.replic.basic.service.ReplicationPropertiesState;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.replic.basic.service.ReplicationAddState;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Run fixity
 * @author dloy
 */
public class ReplicCleanup
        extends ReplicActionAbs
        implements Callable, Runnable,ReplicActionInf
{
    protected static final String NAME = "ReplicCleanup";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    protected Properties results = new Properties();
    protected enum CleanupStatus {noprime, exception, replicated, primeEQsecond, notset};
    
    protected enum SelectType {none, missing, tooSmall, test};
    
    protected DPRFileDB db = null;
    protected String selectMissing =
            "select o.ark "
            + "from inv_nodes_inv_objects as no, "
            + "inv_objects as o, "
            + "inv_collections_inv_nodes as cn, "
            + "inv_collections_inv_objects as co "
            + "where no.inv_object_id = co.inv_object_id "
            + "and cn.inv_collection_id = co.inv_collection_id "
            + "and no.inv_object_id=o.id "
            + "and no.inv_object_id in ( "
            + "select o.id "
            + "from inv_objects as o, "
            + "inv_nodes as n, "
            + "inv_nodes_inv_objects as no "
            + "where no.role = 'primary' "
            + "and no.inv_object_id = o.id "
            + "and no.inv_node_id = n.id "
            + "and not o.ark like '%99999%' "
            + "and not no.inv_object_id in ( "
            + "select inv_object_id from inv_nodes_inv_objects "
            + "where role = 'secondary' "
            + ") "
            + "); ";

    protected String selectTooSmall =
            "select o.ark "
            + "from "
            + "inv_objects as o, "
            + "inv_nodes_inv_objects as no "
            + "where "
            + "not o.version_number = no.version_number "
            + "and o.id = no.inv_object_id "
            + "and no.role = 'secondary'; ";
    
    protected Properties [] rows = null;
    protected Properties setupProperties = null;
    protected String msg = null;
    protected String emailFrom = null;
    protected String emailTo = null;
    protected String emailSubject = null;
    protected String emailMsg = null;
    protected NodeIO nodes = null;
    protected ArrayList<ReplicAction> replicActions = new ArrayList<ReplicAction>();
    protected String arks[] = null;
    
    public static ReplicCleanup getReplicCleanup(
            Properties setupProperties,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        return new ReplicCleanup(setupProperties, nodes, db, logger);
    }
    
    protected ReplicCleanup(
            Properties setupProperties,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.nodes = nodes;
        this.db = db;
        this.setupProperties = setupProperties;
        if (setupProperties != null) {
            buildEmail();
        }
        validate();
    }
    
    
    public static ReplicCleanup getReplicCleanup(
            String [] arks,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        return new ReplicCleanup(arks, nodes, db, logger);
    }
    
    protected ReplicCleanup(
            String [] arks,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.nodes = nodes;
        this.arks = arks;
        this.db = db;
        validate();
    }
    
    private void validate()
        throws TException
    {
        if (this.db == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "database required");
        }
        
        if (this.nodes == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "NodeIO required");
        }
        
        if (this.arks == null) {
            if (this.setupProperties == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "setup Properites required");
            }
        }
    }
    
    @Override
    public void run()
    {
        Properties [] rows = null;
        try {
            log("run entered");
            if (arks == null) {
                doSelect();
            } else {
                doTest();
            }
            if (replicActions.size() > 0) {
                processList();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.logError(msg, 2);
            setException(ex);
        } 
    }
    public void doSelect()
    {
        Properties [] rows = null;
        try {
            log("run entered");
            Integer missingCnt = getReplicActions(selectMissing, SelectType.missing);
            System.out.println("missingCnt:" + missingCnt);
            Integer tooSmallCnt = getReplicActions(selectTooSmall, SelectType.tooSmall);
            System.out.println("tooSmallCnt:" + tooSmallCnt);

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.logError(msg, 2);
            setException(ex);
        } 
    }
    public void doTest()
    {
        Properties [] rows = null;
        try {
            log("doTest entered");
            Integer testCnt = getReplicTest();
            System.out.println("testCnt:" + testCnt);

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.logError(msg, 2);
            setException(ex);
        } 
    }
    protected void processList()
       throws TException
    {
        try {
            log("run processList");
            
            for (ReplicAction ra : replicActions) {
                
                Properties prop = ra.retrieveProp();
                log(PropertiesUtil.dumpProperties("rno", prop));
                doCleanup(ra);
                
                prop = ra.retrieveProp();
                log(PropertiesUtil.dumpProperties("***return prop", prop));
                //replicationPropertiesState.addRow(prop);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.logError(msg, 2);
            setException(ex);

        }
        
    }
    
    
    protected ReplicationAddState doCleanup(ReplicAction ra)
       throws TException
    {
        Connection connect = null;
        Identifier objectID = null;
        try {
            log("run entered");
            objectID = new Identifier(ra.ark);
            if (DEBUG) System.out.println("***Process:" + objectID.getValue());
            connect = db.getConnection(true);
            if (!connect.isValid(0)) {
                throw new TException.SQL_EXCEPTION(MESSAGE + "connection not found=" + objectID.getValue());
            }
            InvNodeObject invNodeObject = InvDBUtil.getNodeObjectPrimary(objectID, connect, logger);
            if (invNodeObject == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object not found=" + objectID.getValue());
            }
            log(PropertiesUtil.dumpProperties("Add IN:" + objectID.getValue(), invNodeObject.retrieveProp()));
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
            
            ra.cleanupStatus = CleanupStatus.replicated;
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties(MESSAGE + "addObject completed:"+ objectID.getValue(), invNodeObject.retrieveProp()));
            return replicator.getResult();
            
        } catch (TException tex) {
            System.out.println("Add exception - "+ objectID.getValue() + ":" + tex);
            ra.ex = tex;
            System.out.println("error:" + tex.getError());
            System.out.println("description:" + tex.getDescription());
            System.out.println("detail:" + tex.getDetail());
            if (tex.getDetail().contains("Missing:")) {
                ra.cleanupStatus = CleanupStatus.noprime;
            } else if (tex.getDetail().contains("Primary nodeid matches secondary")) {
                ra.cleanupStatus = CleanupStatus.primeEQsecond;
            } else {
                ra.cleanupStatus = CleanupStatus.exception;
            }
            return null;
            
        } catch (Exception ex) {
            System.out.println("Add exception - "+ objectID.getValue() + ":" + ex);
            ra.ex = ex;
            ra.cleanupStatus = CleanupStatus.exception;
            return null;
            
        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
    }
    protected Integer getReplicActions(String sql, SelectType selectType)
        throws TException
    {
        
        Properties [] rows = null;
        Connection connect = null;
        try {
            log("run entered");
            connect = db.getConnection(true);
            Properties[] propArray = DBUtil.cmd(connect, sql, logger);
        
            if ((propArray == null)) {
                log("InvDBUtil - prop null");
                return null;

            } else if (propArray.length == 0) {
                log("InvDBUtil - length == 0");
                return 0;
            }
            log("DUMP" + PropertiesUtil.dumpProperties("prop", propArray[0]));
            for (Properties prop : propArray) {
                String ark = prop.getProperty("ark");
                ReplicAction ra = new ReplicAction(ark, selectType);
                replicActions.add(ra);
            }
            return propArray.length;

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.logError(msg, 2);
            throw new TException(ex);

        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
    }
    protected Integer getReplicTest()
        throws TException
    {
        
        try {
            log("getReplicTest entered");
        
            for (String ark : arks) {
                ReplicAction ra = new ReplicAction(ark, SelectType.test);
                replicActions.add(ra);
            }
            return arks.length;

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.logError(msg, 2);
            throw new TException(ex);

        }
    }


    @Override
    public ReplicationPropertiesState call()
    {
        run();
        return getReplicationPropertiesState();
    }
    
    protected ReplicationPropertiesState getReplicationPropertiesState()
    {
        ReplicationPropertiesState rps = new ReplicationPropertiesState();
        for (ReplicAction ra : replicActions) {
            Properties prop = ra.retrieveProp();
            rps.addRow(prop);
        }
        return rps;
    }
    
    protected void log(String msg)
    {
        if (!DEBUG) return;
        System.out.println(MESSAGE + msg);
    }
    
    protected void buildEmail()
        throws TException
    {
        if ((setupProperties == null) || (setupProperties.size() == 0)) return;
        log(PropertiesUtil.dumpProperties("buildEmail", setupProperties));
        DateState dstate = new DateState();
        String ctime= dstate.getIsoDate();
        emailSubject = getMail(NAME + ".emailSubject", 
                "Replication Cleanup report");
        emailSubject += ": " + ctime;
        emailFrom = getMail(NAME + ".emailFrom","merritt@ucop.edu");
        emailTo =  getMail(NAME + ".emailTo", null);
        if (emailTo == null) {
            throw new TException.INVALID_OR_MISSING_PARM(NAME + ".emailTo required");
        }
        emailMsg = getMail(NAME + ".emailMsg.1", null);
        if (emailMsg == null) {
            emailMsg = NAME + " Results:\n";
        } else {
            emailMsg += "\n";
            for (int i=2; true; i++) {
                String testMsg = getMail(NAME + ".emailMsg."+i, null);
                if (testMsg == null) break;
                emailMsg += testMsg + "\n";
            }
        }
    }
    
    protected String getMail(String key, String def)
    {
        if (setupProperties == null) return def;
        if (StringUtil.isAllBlank(key)) return null;
        String value = setupProperties.getProperty(key);
        if (value == null) value = def;
        return value;
    }

    public String getEmailFrom() {
        return emailFrom;
    }

    public void setEmailFrom(String emailFrom) {
        this.emailFrom = emailFrom;
    }

    public String getEmailTo() {
        return emailTo;
    }

    public void setEmailTo(String emailTo) {
        this.emailTo = emailTo;
    }

    public String getEmailSubject() {
        return emailSubject;
    }

    public void setEmailSubject(String emailSubject) {
        this.emailSubject = emailSubject;
    }

    public String getEmailMsg() {
        return emailMsg;
    }

    public void setEmailMsg(String emailMsg) {
        this.emailMsg = emailMsg;
    }
    
    public static class ReplicAction {
        public long replicid = 0;
        public CleanupStatus cleanupStatus = CleanupStatus.notset;
        public Exception ex = null;
        public String ark = null;
        public DateState replicDate = null;
        public SelectType selectType = SelectType.none;
        public ReplicAction() { }
        public ReplicAction(String ark, SelectType selectType) {
            replicDate = new DateState();
            this.selectType = selectType;
            this.ark = ark;
        }
        public Properties retrieveProp()
        {
            Properties retProp = new Properties();
            retProp.setProperty("ark", ark);
            
            retProp.setProperty("selectType", selectType.name());
            retProp.setProperty("cleanupStatus", cleanupStatus.name());
            retProp.setProperty("replicDate", replicDate.getIsoDate());
            if (ex != null) {
                if (ex instanceof TException) {
                    retProp.setProperty("error", ((TException)ex).getDetail());
                } else {
                    retProp.setProperty("error", ex.toString());
                }
                
            }
            return retProp;
        }
    }
}


