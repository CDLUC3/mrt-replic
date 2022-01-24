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
import org.cdlib.mrt.inv.action.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.http.HttpResponse;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FixityStatusType;

import org.cdlib.mrt.replic.basic.action.ReplicActionAbs;
import org.cdlib.mrt.inv.content.InvFile;
import org.cdlib.mrt.inv.content.InvAudit;
import org.cdlib.mrt.inv.content.InvNode;
import org.cdlib.mrt.inv.content.InvNode.AccessMode;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import static org.cdlib.mrt.replic.basic.action.ObjectReplication.MESSAGE;
import static org.cdlib.mrt.replic.basic.action.Replicator.MESSAGE;
import org.cdlib.mrt.replic.basic.service.MatchObjectState;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.basic.service.ScanManager;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.replic.utility.ReplicDBUtil;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.CloudManifestCopyVersion;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Run fixity
 * @author dloy
 */
public class NodeObjectMaint
        extends ReplicActionAbs
{

    protected static final String NAME = "NodeObjectMaint";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
 
    
    protected ReplicationInfo info = null;
    
    protected InvNodeObject primaryNodeObject = null;
    protected List<ReplicationInfo.NodeObjectInfo> infoList = null;
    //protected ArrayList<InvNodeObject> secondaryNodeObjectList = null;
    protected String copyResponse = null;
    protected long maxPrimaryVersion = -1;
    protected boolean nearLineDelete = false;
    protected boolean doMatch = false;
    protected DPRFileDB db = null;
    
    public static void main(String args[])
    {
        main_format(args);
    }
    
    
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
            Long[] ret = ReplicDBUtil.getVersionSize(2703L, 3L ,connection, logger);
            if (ret == null) {
                System.out.println("dump null");
                return;
            }
            System.out.println("dump"
                    + " - ret0=" + ret[0] 
                    + " - ret1=" + ret[1] 
            );
            
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
    
    public static NodeObjectMaint getNodeObjectMaint(ReplicationInfo info, DPRFileDB db)
        throws TException
    {
        return new NodeObjectMaint(info, db);
    }
    
    protected NodeObjectMaint(ReplicationInfo info, DPRFileDB db)
        throws TException
    {
        super(info.getLogger());
        try {
            if (info == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing ReplicationInfo");
            }
            this.info = info;
            
            this.infoList = info.getSecondaryList();
            this.primaryNodeObject = info.getPrimaryInvNodeObject();
            this.maxPrimaryVersion = setPrimaryVersionLong(info.getMaxPrimaryVersion());
            this.db = db;
            validate();
    
            
        } catch (Exception ex) {
            
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            else throw new TException(ex);
        }
    }
    
    private static long setPrimaryVersionLong(Integer primaryVersionI)
        throws TException
    {
        
        if (primaryVersionI == null)  {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "maxPrimaryVersion null");
        }
        int maxPrimaryVersionI = primaryVersionI;
        return (long)maxPrimaryVersionI;
    }
    
    private void validate()
        throws TException
    {
        if (this.primaryNodeObject == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing primaryNodeObject");
        }
        if (this.infoList == null) {
            return;
            //throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing secondaryNodeObjectList");
        }
        if (this.infoList.isEmpty()) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "empty secondaryNodeObjectList");
        }
        if (db == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "db not supplied");
        }
    }
    
    public InvNodeObject setSecondaryStart(
            InvNodeObject secondary)
        throws TException
    {
        Connection connection = db.getConnection(true);
        try {
            InvNodeObject retSec = setSecondaryStart(secondary, connection, logger);
            return retSec;
            
        } finally {
            closeConnection(connection);
        }
    }
    
    public static InvNodeObject setSecondaryStart(
            InvNodeObject secondary, 
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        long maxVer = 0;
        DateState start = new DateState();
        InvNodeObject.CompletionStatus completionStatus = InvNodeObject.CompletionStatus.unknown;
        try {
                if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("start setSecondaryStart", secondary.retrieveProp()));
                Long versionNumber = secondary.getVersionNumber();
                if (versionNumber == null) versionNumber = 0L;
                if (DEBUG) System.out.println("versionNumber=" + versionNumber);
                long secSize = getSecondaryLength(secondary, connection, logger);
                if (DEBUG) System.out.println("end setSecondaryStart"
                        + " - getNodesid=" + secondary.getNodesid()
                        + " - getObjectsid=" + secondary.getObjectsid()
                        + " - versionNumber=" + versionNumber
                        + " - secSize=" + secSize
                        + " - maxVer=" + maxVer
                );
                secondary.setReplicated(ReplicDB.getReplicatedDayOne());
                secondary.setSize(secSize);
                secondary.setStart(start);
                secondary.setCompletionStatus(completionStatus);
                secondary.setNote(null);
                return secondary;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            
            throw new TException(ex);
        }
    }
    
    public static void closeConnection(Connection connect)
    {
        try {
            if (connect == null) return;
            connect.close();
        } catch (Exception ex) { }
    }
    
    public InvNodeObject updatePrimaryStart()
        throws TException
    {
        InvNodeObject primary = primaryNodeObject;
        Long versionNumber = maxPrimaryVersion;
        Connection connection = db.getConnection(true);
        try {
            InvNodeObject retPrimary = setPrimaryStart(primary, versionNumber, infoList, connection, logger);
            resetPrimaryReplication(primary, connection);
            return retPrimary;
            
        } finally {
            closeConnection(connection);
        }
    }
    
    public static InvNodeObject setPrimaryStart(
            InvNodeObject primary,
            Long versionNumber,
            List<ReplicationInfo.NodeObjectInfo> infoList,
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        long maxVer = 0;
        DateState start = new DateState();
        InvNodeObject.CompletionStatus completionStatus = InvNodeObject.CompletionStatus.unknown;
        try {
                if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("start setPrimaryStart", primary.retrieveProp()));
                long secSize = getPrimaryLength(infoList, connection, logger);
                if (DEBUG) System.out.println("end setPrimaryStart"
                        + " - getNodesid=" + primary.getNodesid()
                        + " - getObjectsid=" + primary.getObjectsid()
                        + " - versionNumber=" + versionNumber
                        + " - secSize=" + secSize
                        + " - maxVer=" + maxVer
                );
                primary.setReplicated(ReplicDB.getReplicatedDayOne());
                primary.setSize(secSize);
                primary.setStart(start);
                primary.setCompletionStatus(completionStatus);
                primary.setVersionNumber(versionNumber);
                return primary;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            
            throw new TException(ex);
        }
    }
    
    public static Long getPrimaryLength(
            List<ReplicationInfo.NodeObjectInfo> infoList,
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        long updateSize = 0;
        try {
            
            for (ReplicationInfo.NodeObjectInfo nodeObjectInfo : infoList) {
                if (nodeObjectInfo.isPrimary()) continue;
                InvNodeObject nodeObject = nodeObjectInfo.getInvNodeObject();
                long secSize = getSecondaryLength(nodeObject, connection, logger);
                updateSize += secSize;
            }
                if (DEBUG) System.out.println("getPrimaryLength"
                        + " - updateSize=" + updateSize
                );
            return updateSize;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static InvNodeObject.CompletionStatus getPrimaryStatus(
            long version,
            List<ReplicationInfo.NodeObjectInfo> infoList,
            LoggerInf logger)
        throws TException
    {
        InvNodeObject.CompletionStatus completionStatus = InvNodeObject.CompletionStatus.ok;
        try {
            boolean allOK = true;
            boolean allFail = true;
            for (ReplicationInfo.NodeObjectInfo nodeObjectInfo : infoList) {
                if (nodeObjectInfo.isPrimary()) continue;
                InvNodeObject nodeObject = nodeObjectInfo.getInvNodeObject();
                InvNodeObject.CompletionStatus localStatus = nodeObject.getCompletionStatus();
                if (localStatus != InvNodeObject.CompletionStatus.ok) {
                    allOK = false;
                } else {
                    allFail = false;
                }
                Long procVersions = nodeObject.getVersionNumber();
                if (procVersions == null) procVersions = 0L;
                if (procVersions != version) {
                    allOK = false;
                } else {
                    allFail = false;
                }
            }
            if (allOK) {
                return InvNodeObject.CompletionStatus.ok;
            }
            if (allFail) {
                return InvNodeObject.CompletionStatus.fail;
            }
            return InvNodeObject.CompletionStatus.partial;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static Long getSecondaryLength(
            InvNodeObject secondary, 
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        long maxVer = 0;
        try {
                
                if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("secondary", secondary.retrieveProp()));
                Long versionNumber = secondary.getVersionNumber();
                if (versionNumber == null) {
                    versionNumber = 0L;
                    secondary.setVersionNumber(versionNumber);
                }
                if (DEBUG) System.out.println("versionNumber=" + versionNumber);
                Long[] retSecondaryLength = ReplicDBUtil.getVersionSize( secondary.getObjectsid(), versionNumber, connection, logger);
                if (retSecondaryLength == null) {
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "secondary version not found:" + secondary.getId());
                }
                long secSize = retSecondaryLength[0];
                if (DEBUG) System.out.println("getSecondaryLength"
                        + " - getNodesid=" + secondary.getNodesid()
                        + " - secSize=" + secSize
                );
                return secSize;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            
            throw new TException(ex);
        }
    }
    
    
    public InvNodeObject setSecondaryVersion(
            InvNodeObject secondary,
            InvNodeObject.CompletionStatus completionStatus,
            Long versionNumber)
        throws TException
    {
        Connection connection = db.getConnection(true);
        try {
            InvNodeObject retSec = setSecondaryVersion(secondary, completionStatus, versionNumber, logger);
            resetPrimaryReplication(secondary, connection);
            return retSec;
            
        } finally {
            closeConnection(connection);
        }
    }
    
    public static InvNodeObject setSecondaryVersion(
            InvNodeObject secondary,
            InvNodeObject.CompletionStatus completionStatus,
            Long versionNumber,
            LoggerInf logger)
        throws TException
    {
        DateState start = new DateState();
        try {
            secondary.setCompletionStatus(completionStatus);
            secondary.setVersionNumber(versionNumber);
            return secondary;
            
        } catch (Exception ex) {
            
            throw new TException(ex);
        }
    }
    
    
    public InvNodeObject setSecondaryEnd(
            InvNodeObject secondary,
            Exception runException)
        throws TException
    {
        Long versionNumber = maxPrimaryVersion;
        try {
            InvNodeObject retSec = setSecondaryEnd(secondary, versionNumber, runException, logger);
            return retSec;
            
        } finally {
            //closeConnection(connection);
        }
    }
    
    public static InvNodeObject setSecondaryEnd(
            InvNodeObject secondary,
            Long versionNumber,
            Exception runException, 
            LoggerInf logger)
        throws TException
    {
        DateState start = new DateState();
        try {
            if (runException != null) {
                String jsonEx = NodeObjectMaint.getExceptionNote(runException);
                secondary.setNote(jsonEx);
                secondary.setCompletionStatus(InvNodeObject.CompletionStatus.fail);
                secondary.setReplicated(ReplicDB.getReplicatedYear());
                secondary.setVersionNumber((Long)null);
                
            } else {
                secondary.setNote("");
                secondary.setReplicated(start);
                secondary.setVersionNumber(versionNumber);
                secondary.setCompletionStatus(InvNodeObject.CompletionStatus.ok);
                
            }
            return secondary;
            
        } catch (Exception ex) {
            
            throw new TException(ex);
        }
    }
    
    public InvNodeObject updatePrimaryEnd()
        throws TException
    {
        InvNodeObject primary = primaryNodeObject;
        Long versionNumber = maxPrimaryVersion;
        Connection connection = db.getConnection(true);
        try {
            InvNodeObject retPrimary = setPrimaryEnd(primary, versionNumber, infoList, logger);
            resetPrimaryReplication(primary, connection);
            return retPrimary;
            
        } finally {
            closeConnection(connection);
        }
    }
    
    public static InvNodeObject setPrimaryEnd(
            InvNodeObject primary, 
            Long versionNumber,
            List<ReplicationInfo.NodeObjectInfo> infoList,
            LoggerInf logger)
        throws TException
    {
        long maxVer = 0;
        DateState start = new DateState();
        InvNodeObject.CompletionStatus completionStatus = getPrimaryStatus(versionNumber, infoList, logger);
        try {
               
                primary.setCompletionStatus(completionStatus);
                switch (completionStatus) {
                    case ok:
                        primary.setReplicated(start);
                        primary.setNote("");
                        break;
                        
                    case fail:
                        primary.setNote(completionStatus.toString());
                        primary.setReplicated(ReplicDB.getReplicatedYear());
                        break;
                        
                    case partial:
                        primary.setNote(completionStatus.toString());
                        primary.setReplicated(ReplicDB.getReplicatedYear());
                        break;
                        
                    default:
                        throw new TException.INVALID_CONFIGURATION(MESSAGE + "CompletionStatus invalid");
                }
                return primary;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static String getExceptionNote(Exception localEx)
        throws Exception
    { 
        try {
            System.out.println("localEx:" + localEx.toString());
            JSONObject errorJSON = new JSONObject();
            String msg = localEx.toString();
            StackTraceElement [] ste = localEx.getStackTrace();
            if (!StringUtil.isAllBlank(msg)) {
                errorJSON.put("message", msg);
            }
            if ((ste != null) && (ste.length > 0)) {
                JSONArray traceJSON = new JSONArray();
                int lvl = 0;
                for (StackTraceElement ele : ste) {
                    JSONObject traceEle = new JSONObject();
                    if (ele.getClassName() != null) {
                        traceEle.put("class", ele.getClassName());
                    }
                    if (ele.getFileName() != null) {
                        traceEle.put("file", ele.getFileName());
                    }
                    if (ele.getMethodName() != null) {
                        traceEle.put("method", ele.getMethodName());
                    }
                    traceEle.put("line", "" + ele.getLineNumber());
                    traceJSON.put(lvl, traceEle);
                    lvl++;
                }
                errorJSON.put("trace", traceJSON);
            }
            
            return errorJSON.toString(2);
            
        } catch (Exception ex) {
            System.out.println(ex);
            throw ex;
        }
        
    }
    
    protected void resetPrimaryReplication(InvNodeObject nodeObject, Connection connect) 
        throws TException
    {
        try {
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("***WRITE***", nodeObject.retrieveProp()));
            if (false) return;
            ReplicDB.resetReplicatedAuto(connect, nodeObject, logger);
            logger.logMessage(PropertiesUtil.dumpProperties("resetReplication", nodeObject.retrieveProp()),3,true);
            
        } catch (Exception ex) {
            logger.logError("resetReplication Exception" + ex, 1);
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION("Exception:" + ex);
        } 
    }

    public long getMaxPrimaryVersion() {
        return maxPrimaryVersion;
    }

    public List<ReplicationInfo.NodeObjectInfo> getInfoList() {
        return infoList;
    }
    
    protected void log(String msg)
    {
        logger.logMessage(msg, 5, true);
    }
    
    protected void log(String msg, int lvl)
    {
        logger.logMessage(msg, lvl, true);
    }
}