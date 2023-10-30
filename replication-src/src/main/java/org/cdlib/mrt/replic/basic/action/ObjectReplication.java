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
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import static org.cdlib.mrt.replic.basic.action.Replicator.MESSAGE;
import org.cdlib.mrt.replic.basic.logging.LogReplicAdd;
import org.cdlib.mrt.replic.basic.service.MatchObjectState;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.CloudManifestCopyVersion;

/**
 * Run fixity
 * @author dloy
 */
public class ObjectReplication
        extends ReplicActionAbs
{

    protected static final String NAME = "ObjectReplication";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean DUMPTALLY = false;
    protected static final boolean EACHTALLY = true;
    
    protected ReplicationInfo info = null;
    protected List<ReplicationInfo.NodeObjectInfo> nodeObjectList = null;
    protected String copyResponse = null;
    protected NodeIO nodes = null;
    protected boolean nearLineDelete = false;
    protected boolean doMatch = false;
    protected NodeObjectMaint nodeObjectMaint = null;
    
    public static ObjectReplication getObjectReplication(ReplicationInfo info, DPRFileDB db, NodeIO nodes)
        throws TException
    {
        return new ObjectReplication(info, db, nodes);
    }
    
    protected ObjectReplication(ReplicationInfo info, DPRFileDB db, NodeIO nodes)
        throws TException
    {
        super(db, info.getLogger());
        try {
            if (info == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing ReplicationInfo");
            }
            this.info = info;
            if (nodes == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing CopyNodes");
            }
            this.nodes = nodes;
            nodeObjectMaint = NodeObjectMaint.getNodeObjectMaint(info, db);
            nodeObjectList = info.getSecondaryList();
            //logger.logMessage("***Replication begins:" + info.getObjectID().getValue(), 1, true);
    
            
        } catch (Exception ex) {
            
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            else throw new TException(ex);
        }
    }
    
    
   
    public void process()
        throws TException
    {
        if ((nodeObjectList == null) || (nodeObjectList.size() == 0)) {
            log(MESSAGE + "nothing to process",1);
            return;
        }
        try {
            nodeObjectMaint.updatePrimaryStart();
            for (ReplicationInfo.NodeObjectInfo nodeObjectInfo : nodeObjectList) {
                log(
                        "***Replication starts::" + info.getObjectID().getValue()
                        + " from node:" + nodeObjectInfo.getPrimaryInvNode().getNumber()
                        + " to node:" + nodeObjectInfo.getSecondaryInvNode().getNumber()
                        , 1); 
                CloudManifestCopyVersion.Stat stat = processNodeObject(nodeObjectInfo);
                log(
                        "***Replication complete::" + info.getObjectID().getValue()
                        + " from node:" + nodeObjectInfo.getPrimaryInvNode().getNumber()
                        + " to node:" + nodeObjectInfo.getSecondaryInvNode().getNumber()
                        + " cnt:" + stat.objCnt
                        + " size:" + stat.objSize
                        + " getTime:" + stat.getTime
                        + " putTime:" + stat.putTime
                        , 1);
                
                nodeObjectMaint.updatePrimaryEnd();
                LogReplicAdd replicEntry = LogReplicAdd.getLogReplicAdd(stat, nodeObjectInfo);
                replicEntry.addEntry();
            }
            nodeObjectMaint.updatePrimaryEnd();
                   
        } catch (TException tex) {
            logger.logError(
                        "***Replication fails::" + info.getObjectID().getValue() + "- Exception:" + tex
                        , 1); 
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            logger.logError(
                        "***Replication fails::" + info.getObjectID().getValue() + "- Exception:" + ex
                        , 1); 
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }    
   
    public void processInv()
        throws TException
    {
        if ((nodeObjectList == null) || (nodeObjectList.size() == 0)) {
            log(MESSAGE + "nothing to process",1);
            return;
        }
        try {
            for (ReplicationInfo.NodeObjectInfo nodeObjectInfo : nodeObjectList) {
                processInv(nodeObjectInfo);
            }
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
    
    protected CloudManifestCopyVersion.Stat processNodeObject(ReplicationInfo.NodeObjectInfo nodeObjectInfo) 
        throws TException
    {
        try {           
            CloudManifestCopyVersion.Stat stat = new  CloudManifestCopyVersion.Stat(nodeObjectInfo.getObjectID().getValue());
            InvNodeObject secondary = nodeObjectInfo.getInvNodeObject();
            secondary = nodeObjectMaint.setSecondaryStart(secondary);
            try {
                
                if (false && (secondary.getNodesid() == 18) && (secondary.getObjectsid() == 58178)) {
                    throw new TException.GENERAL_EXCEPTION("Test Something bad happened");
                }
                
                copyContentCloud(nodeObjectInfo, stat);
                secondary = nodeObjectMaint.setSecondaryEnd(secondary, null);
            } catch (Exception exCopy) {
                secondary = nodeObjectMaint.setSecondaryEnd(secondary, exCopy);
            }
            processInv(nodeObjectInfo);
            InvNode targetNode = nodeObjectInfo.getSecondaryInvNode();
            if ( doMatch ) {
                match(nodeObjectInfo);
            }
            return stat;
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    }
    
    public void processInv(ReplicationInfo.NodeObjectInfo nodeObjectInfo) 
        throws TException
    {
        Connection connect = null;
        InvNodeObject primary = info.getPrimaryInvNodeObject();
        InvNodeObject secondary = nodeObjectInfo.getInvNodeObject();
        try {
            connect = db.getConnection(false);
            if (secondary.getCompletionStatus() == InvNodeObject.CompletionStatus.ok) {
                long versionCount = setAudits(nodeObjectInfo, connect);
            } else {
                log(PropertiesUtil.dumpProperties("audit fail", secondary.retrieveProp()),5);
            }
            addNodeObjectSecondary(nodeObjectInfo, connect);
            
            ReplicDB.resetReplicatedCurrent(connect, primary, logger);
            log(PropertiesUtil.dumpProperties("***Replicator***", primary.retrieveProp()));
            connect.commit();
            
        } catch (Exception ex) {
            String msg = MESSAGE + "Exception for nodeid=" + primary.getNodesid() 
                    + " - objectid=" + primary.getObjectsid();
            try {
                Properties entryProp = primary.retrieveProp();
                msg = msg + " - " + PropertiesUtil.dumpProperties("entry", entryProp);
            } catch (Exception xx) { 
            }
            
            System.out.println(MESSAGE + msg + " - Exception:" + ex);
            ex.printStackTrace();
            logger.logError(msg, 2);
            try {
                connect.rollback();
            } catch (Exception cex) {
                System.out.println("WARNING: rollback Exception:" + cex);
            }
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
    }
    
    /**
     * Try cloud copy if that fails try normal store copy
     * @param nodeObject node object with info on copy
     * @return true=success, false=failure
     * @throws TException 
     */
    protected boolean copyContent(ReplicationInfo.NodeObjectInfo nodeObject, CloudManifestCopyVersion.Stat stat) 
        throws TException
    {
        if (copyContentCloud(nodeObject, stat)) {
            return true;
        } else {
            return copyContentStore(nodeObject);
        }
    }
    
    protected boolean copyContentCloud(
            ReplicationInfo.NodeObjectInfo nodeObject, 
            CloudManifestCopyVersion.Stat stat) 
        throws TException
    {
        try {
            Identifier objectID = nodeObject.getObjectID();
            long nodeFrom = nodeObject.primaryInvNode.getNumber();
            long nodeTo = nodeObject.secondaryInvNode.getNumber();
            NodeIO.AccessNode inNode = nodes.getAccessNode(nodeFrom);
            NodeIO.AccessNode outNode = nodes.getAccessNode(nodeTo);
            if ((inNode == null) || (outNode == null)) return false;
            CloudManifestCopyVersion cmct = new CloudManifestCopyVersion(
                    inNode.service,
                    inNode.container,
                    outNode.service,
                    outNode.container,
                    logger);
            log("***CloudCloudCopy:"
                + " - in container=" + inNode.container
                + " - in node=" + nodeFrom
                + " - out container=" + outNode.container
                + " - out node=" + nodeTo
            );
            
            cmct.copyObject(objectID.getValue(), stat);
            return true;
            
        } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
            throw rinf;
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }
    
    protected void match(
            ReplicationInfo.NodeObjectInfo nodeObject) 
        throws TException
    {
        Connection connect = null;
        try {
            connect = db.getConnection(false);
            Identifier objectID = nodeObject.getObjectID();
            long nodeFrom = nodeObject.primaryInvNode.getNumber();
            long nodeTo = nodeObject.secondaryInvNode.getNumber();
            Match match = Match.getMatch(objectID, nodes, (int)nodeFrom, (int)nodeTo, connect, logger);
            MatchObjectState state = match.process();
            String combine = state.getCombine();
            if (state.getMatchManifestInv() && state.getMatchManifestStore()) {
                log(
                        "***Match OK:" + info.getObjectID().getValue()
                        + " from node:" + nodeFrom 
                        + " to node:" + nodeTo
                        + ":" + combine
                        , 1); 
            } else {
                log(
                        "***Match FAIL:" + info.getObjectID().getValue()
                        + " from node:" + nodeFrom 
                        + " to node:" + nodeTo
                        + ":" + combine
                        , 1); 
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
    }
    
    protected boolean copyContentStore(ReplicationInfo.NodeObjectInfo nodeObject) 
        throws TException
    {
        try {
            String storeURLS = nodeObject.getStoreURL();
            log("copyContentStore: ObjectReplication url=" + storeURLS);
            //HttpResponse HTTPUtil.postHttpResponse(String requestURL, Properties prop, int timeout)
            Properties httpProp = new Properties();
            httpProp.setProperty("t", "xml");
            HttpResponse response = HTTPUtil.postHttpResponse(storeURLS, httpProp, 8640000); // 1 day
            Properties respProp = HTTPUtil.response2Property(response);
            String statusS = respProp.getProperty("response.status");
            int status = Integer.parseInt(statusS);
            if (status == 404) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Missing:" + storeURLS);
            }
            if ((status < 200) || (status >= 300)) {
                throw new TException.REMOTE_IO_SERVICE_EXCEPTION("Storage copy fails" 
                        + " - status=" + status
                        + " - url=" + storeURLS
                        + " - response.line=" + respProp.getProperty("response.line")
                        );
            }
            nodeObject.copyResponse = respProp.getProperty("response.value");
            log(PropertiesUtil.dumpProperties("copyContent",respProp));
            return true;
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }
    
    protected long setAudits(ReplicationInfo.NodeObjectInfo nodeObject, Connection connect) 
        throws TException
    {
        try {
            
            DBAdd dbAdd = new DBAdd(connect, logger);
            InvNodeObject invNodeObject = nodeObject.getInvNodeObject();
            InvNode primaryNode = nodeObject.getPrimaryInvNode();
            InvNode targetNode = nodeObject.getSecondaryInvNode();
            long primaryNodeseq = primaryNode.getId();
            String targetBase = targetNode.getBaseURL();
            long node = targetNode.getNumber();
            long targetNodeseq = targetNode.getId();
            long objectseq = invNodeObject.getObjectsid();
            Identifier objectID = nodeObject.getObjectID();
            if (nearLineDelete && (targetNode.getAccessMode() != AccessMode.onLine)) {
                return InvDBUtil.getMaxVersion(objectseq, connect, logger);
            }
            
            List<Properties> audits = InvDBUtil.getAudits(primaryNodeseq, objectseq, connect, logger);
            for (Properties auditProp : audits) {
                InvAudit primaryAudit = new InvAudit(auditProp, logger);
                long primaryFileseq = primaryAudit.getFileid();
                InvAudit targetAudit = InvDBUtil.getAudit(targetNodeseq, objectseq, primaryFileseq, connect, logger);
                if (targetAudit != null) {
                    log(PropertiesUtil.dumpProperties("setAudits exists", auditProp));
                    continue;
                }
                targetAudit = new InvAudit(auditProp, logger);
                targetAudit.setId(0);
                targetAudit.setNodeid(targetNodeseq);

                long fileseq = targetAudit.getFileid();
                long versionseq = targetAudit.getVersionid();
                InvVersion invVersion = InvDBUtil.getVersionFromId(versionseq, connect, logger);
                long versionID = invVersion.getNumber();
                InvFile invFile = InvDBUtil.getFile(fileseq, connect, logger);

                String url = targetBase + "/" + "content/" + node + "/"
                    + URLEncoder.encode(objectID.getValue(), "utf-8") 
                    + "/" + versionID 
                    + "/" + URLEncoder.encode(invFile.getPathName(), "utf-8") 
                    + "?fixity=no"
                    ;
                targetAudit.setUrl(url);
                targetAudit.setStatus(FixityStatusType.unknown);
                targetAudit.setVerified((DateState)null);
                long fixityid = dbAdd.insert(targetAudit);
                targetAudit.setId(fixityid);
                
                log(PropertiesUtil.dumpProperties("setAudits new", auditProp));
            }
            return InvDBUtil.getMaxVersion(objectseq, connect, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
                    
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
 
        }
            
        
    }
    
    /**
     * Add node object for secondary content
     * @param nodeObject
     * @throws TException 
     */
    protected void addNodeObjectSecondary(ReplicationInfo.NodeObjectInfo nodeObjectInfo, 
            Connection connect) 
        throws TException
    {
        try {
            DBAdd dbAdd = new DBAdd(connect, logger);
            InvNodeObject invNodeObject = nodeObjectInfo.getInvNodeObject();
            //invNodeObject.setVersionNumber(versionCount);
            InvNodeObject secondaryNodeObject = InvDBUtil.getNodeObject(
                    invNodeObject.getNodesid(), invNodeObject.getObjectsid(), connect, logger);
            long id = 0;
            if (secondaryNodeObject == null) {
                log(PropertiesUtil.dumpProperties("New secondary", invNodeObject.retrieveProp()));
                id = dbAdd.insert(invNodeObject);
                invNodeObject.setId(id);
            } else {
                invNodeObject.setId(secondaryNodeObject.getId());
                log(PropertiesUtil.dumpProperties("Old secondary", invNodeObject.retrieveProp()));
                id = dbAdd.update(invNodeObject);   
            }
            log(PropertiesUtil.dumpProperties("addNode", invNodeObject.retrieveProp()));
            
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }

    public boolean isDoMatch() {
        return doMatch;
    }

    public void setDoMatch(boolean doMatch) {
        this.doMatch = doMatch;
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