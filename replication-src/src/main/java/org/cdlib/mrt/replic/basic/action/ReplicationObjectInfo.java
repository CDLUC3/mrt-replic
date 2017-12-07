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

//import org.cdlib.mrt.inv.action.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvNode;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * Run fixity
 * @author dloy
 */
public class ReplicationObjectInfo
        extends ReplicActionAbs
{

    protected static final String NAME = "ReplicationObjectInfo";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean DUMPTALLY = false;
    protected static final boolean EACHTALLY = true;
    
    protected Identifier objectID = null;
    protected NodeObjectInfo primaryNodeObject = null;
    protected HashMap<Long, NodeObjectInfo> objectNodes = new HashMap(20);
    protected ArrayList<InvNodeObject> invNodeObjects = null;
    protected InvNodeObject primaryInvNodeObject = null;
    protected InvNode primaryInvNode = null;
    protected InvObject invObject = null;
    protected int maxPrimaryVersion = 0;
    //protected long primaryNodeseq = 0;
    //protected int primaryNodeID = 0;
    
    
    public static ReplicationObjectInfo getReplicationInfo(
            Identifier objectID,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return new ReplicationObjectInfo(objectID, connection, logger);
    }
    
    protected ReplicationObjectInfo(
            Identifier objectID,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        try {
            if (objectID == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "missing objectID");
            }
            this.objectID = objectID;
            
        } catch (Exception ex) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ex2) { }
            
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            else throw new TException(ex);
        }
    }
    
    
    
    public void buildInfo()
        throws TException
    {
        try {
            invObject = getObject();
            addPrimaryInfo(invObject.getId());
            
            getMaxVersions();
            getNodeObjects();
            addMapNode();
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connection.close();
            } catch (Exception ex) { }
        }
    }
    protected void addPrimaryInfo(long objectseq)
        throws TException
    {
        try {
            invNodeObjects = InvDBUtil.getObjectNodes(objectseq, connection, logger);
            int primeCnt = 0;
            for (InvNodeObject invNodeObject : invNodeObjects) {
                if (invNodeObject.role == Role.primary) {
                    primeCnt++;
                    primaryInvNodeObject = invNodeObject;
                }
                
            }
            if (primeCnt != 1) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "primary count invalid=" + primeCnt);
            }
            long nodeseq = primaryInvNodeObject.getNodesid();
            primaryInvNode = InvDBUtil.getNodeFromId(nodeseq, connection, logger);
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected InvObject getObject()
        throws TException
    {
        try {
            InvObject invLocalObject = InvDBUtil.getObject(objectID, connection, logger);
            if (invLocalObject == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "object not found:" + objectID.getValue());
            }
            return invLocalObject;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected void getMaxVersions()
        throws TException
    {
        try {
            maxPrimaryVersion = InvDBUtil.getVersionCnt(invObject.getArk(), connection, logger);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public int addMapNode()
        throws TException
    {
        try {
            long objectseq = invObject.getId();
            ArrayList<Long> collectionNodes = InvDBUtil.getCollectionNodes(objectseq, connection, logger);
            if (collectionNodes  == null) {
                return 0;
            }
            int addCnt = 0;
            NodeObjectInfo nodeObjectInfo = null;
            for (Long node: collectionNodes) {
                nodeObjectInfo = objectNodes.get(node);
                if (nodeObjectInfo == null) {
                    InvNodeObject invNodeObject = new InvNodeObject(logger);
                    invNodeObject.setObjectsid(objectseq); 
                    invNodeObject.setNodesid(node);
                    invNodeObject.setRole(Role.secondary);
                    invNodeObject.setReplicatedCurrent();
                    addNodesList(invNodeObject, false);
                    addCnt++;
                }
            }
            return addCnt;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void getNodeObjects()
        throws TException
    {
        try {
            long objectseq = invObject.getId();
            
            for (InvNodeObject invNodeObject: invNodeObjects) {
                addNodesList(invNodeObject, true);
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void addNodesList(InvNodeObject invNodeObject, boolean exists)
        throws TException
    {
        NodeObjectInfo nodeObjectInfo = null;
        if (invNodeObject.getRole() == Role.primary) return;
        long nodeseq = invNodeObject.getNodesid();
        nodeObjectInfo = objectNodes.get(nodeseq);
        if (nodeObjectInfo != null) return;
        int maxAuditVersion = InvDBUtil.getAuditVersionCnt(nodeseq, invNodeObject.getObjectsid(), connection, logger);
        int diff = testAudits(maxAuditVersion);
        if (diff == 0) return; // matching version and audit counts
        long nodeNum = primaryInvNode.getNumber();
        InvNode secondaryInvNode = InvDBUtil.getNodeFromId(nodeseq, connection, logger);
        
        nodeObjectInfo 
                = new NodeObjectInfo(
                    invNodeObject, 
                    primaryInvNode, 
                    secondaryInvNode, 
                    objectID, 
                    maxAuditVersion, 
                    exists);
        objectNodes.put(nodeseq, nodeObjectInfo);
        if (nodeObjectInfo.primary) {
            primaryNodeObject = nodeObjectInfo;
        }
    }
    
    protected int testAudits(int maxSecondaryVersion)
        throws TException
    {
        try {
            if (maxPrimaryVersion == 0) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "primary version count=0");
            }
            if (maxSecondaryVersion > maxPrimaryVersion) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "primary > secondary"
                        + " - maxSecondaryVersion=" + maxSecondaryVersion
                        + " - maxPrimaryVersion=" + maxPrimaryVersion
                        );
            }
            return maxPrimaryVersion - maxSecondaryVersion;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public void dump(String header)
    {
        System.out.println(header + "\n");
        /**
         * protected int maxPrimaryVersion = 0;
    protected long primaryNodeseq = 0;
    protected int primaryNodeID = 0;
         */
        System.out.println(""
                + " - maxPrimaryVersion=" + maxPrimaryVersion 
                + "\n"
                );
        Set<Long> keys = objectNodes.keySet();
        for (Long key : keys) {
            NodeObjectInfo info = objectNodes.get(key);
            String dump = info.dump();
            System.out.println("NODE=" + key + "\n" + dump);
        }
            
    }
    
    public int getSecondaryCount()
    {
        return objectNodes.size();
    }
    
    public List<NodeObjectInfo> getSecondaryList()
    {
        if (objectNodes.isEmpty()) {
            return null;
        }
        Set<Long> keys = objectNodes.keySet();
        ArrayList<NodeObjectInfo> returnList = new ArrayList(keys.size());
        for (Long key : keys) {
            NodeObjectInfo info = objectNodes.get(key);
            returnList.add(info);
        }
        return returnList;
    }
    
    public static class NodeObjectInfo
    {
        public InvNodeObject invNodeObject = null;
        public InvNode primaryInvNode = null;
        public InvNode secondaryInvNode = null;
        public int maxAuditVersion = 0;
        public Identifier objectID = null;
        public boolean exists = false;
        public boolean primary = false;
        public int copied = 0;
        public String storeURL = null;
        
        public NodeObjectInfo(
                InvNodeObject invNodeObject, 
                InvNode primaryInvNode, 
                InvNode secondaryInvNode,
                Identifier objectID,
                int maxAuditVersion, 
                boolean exists)
            throws TException
        {
            this.invNodeObject = invNodeObject;
            this.primaryInvNode = primaryInvNode;
            this.secondaryInvNode = secondaryInvNode;
            this.objectID = objectID;
            this.exists = exists;
            this.maxAuditVersion = maxAuditVersion;
            if (invNodeObject.getRole() == Role.primary) {
                this.primary = true;
            }
            validate();
            buildStoreURL();
        }

        private void validate()
            throws TException
        {
            if (invNodeObject == null) {
                throw new TException.INVALID_OR_MISSING_PARM("invNodeObject missing");
            }
            if (primaryInvNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM("primaryInvNode missing");
            }
            if (secondaryInvNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM("secondaryInvNode missing");
            }
        }
        
        protected void buildStoreURL()
            throws TException
        {
            try {
                String baseURL = secondaryInvNode.getBaseURL();
                long nodeSecondary = secondaryInvNode.getNumber();
                long nodePrimary = primaryInvNode.getNumber();
                String encObjectID = URLEncoder.encode(objectID.getValue(), "utf-8");
                storeURL = baseURL + "/copy/" + nodePrimary + '/' + nodeSecondary + '/' + encObjectID;
            } catch (Exception ex) {
                throw new TException(ex);
            }
        }
        
        public InvNodeObject getInvNodeObject() {
            return invNodeObject;
        }

        public void setInvNodeObject(InvNodeObject invNodeObject) {
            this.invNodeObject = invNodeObject;
        }

        public int getMaxAuditVersion() {
            return maxAuditVersion;
        }

        public void setMaxAuditVersion(int maxAuditVersion) {
            this.maxAuditVersion = maxAuditVersion;
        }

        public long getNodeseq() {
            return invNodeObject.getNodesid();
        }

        public long getObjectseq() {
            return invNodeObject.getObjectsid();
        }

        public boolean isExists() {
            return exists;
        }

        public void setExists(boolean exists) {
            this.exists = exists;
        }

        public boolean isPrimary() {
            return primary;
        }

        public void setPrimary(boolean primary) {
            this.primary = primary;
        }

        public int getCopied() {
            return copied;
        }

        public void setCopied(int copied) {
            this.copied = copied;
        }

        public InvNode getPrimaryInvNode() {
            return primaryInvNode;
        }

        public InvNode getSecondaryInvNode() {
            return secondaryInvNode;
        }

        public Identifier getObjectID() {
            return objectID;
        }

        public String getStoreURL() {
            return storeURL;
        }
       
        public String dump()
        {
            StringBuilder buf = new StringBuilder();
            buf.append("\n***NodeObjectInfo*** objectID=" + objectID.getValue() + "\n");
            buf.append(invNodeObject.dump("invNodeObject") + "\n");
            buf.append(primaryInvNode.dump("primaryInvNode") + "\n");
            buf.append(secondaryInvNode.dump("secondaryInvNode") + "\n");
            buf.append("storeURL=" + storeURL + "\n");
            buf.append(""
                    + " - maxAuditVersion=" + maxAuditVersion
                    + " - exists=" + exists
                    + " - primary=" + primary
                    + " - copied=" + copied
                    + "\n"
                    );
            return buf.toString();
        }
    }
}