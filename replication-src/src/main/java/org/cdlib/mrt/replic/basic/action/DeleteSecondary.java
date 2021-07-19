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
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.replic.basic.service.NodesObjectsState;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class DeleteSecondary
        extends ReplicActionAbs
{

    protected static final String NAME = "Deletor";
    protected static final String MESSAGE = NAME + ": ";

    protected ReplicationDeleteState replicationDeleteState = null;
    protected NodesObjectsState nodesObjectsState = null;
    protected Identifier objectID = null;
    protected DPRFileDB db = null;
    
    protected NodeIO nodes = null;
    protected Long singleNode = null;
    
    public static DeleteSecondary getDeleteSecondary(
            Identifier objectID,
            DPRFileDB db,
            NodeIO nodes,
            LoggerInf logger)
        throws TException
    {
        return new DeleteSecondary(objectID, db, nodes, logger);
    }
    
    protected DeleteSecondary(
            Identifier objectID,
            DPRFileDB db,
            NodeIO nodes,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.objectID = objectID;
        this.db = db;
        this.nodes = nodes;
        try {
            connection = db.getConnection(true);
        } catch (Exception ex) {
            throw new TException.SQL_EXCEPTION(ex);
        } 
    }
    
    public ReplicationDeleteState process()
        throws TException
    {
        
        try {
            boolean doDeleteStore = true;
            replicationDeleteState = new ReplicationDeleteState(doDeleteStore, objectID, 0);
            NodesObjects nos = NodesObjects.getNodeObjects(objectID, connection, logger);
            
            nodesObjectsState = nos.getNodesObjectState();
            log( 10,"Secondary count:" + nodesObjectsState.retrieveSecondaryCount());
            if (nodesObjectsState.retrieveSecondaryCount() == 0) {
                return replicationDeleteState;
            }
            List<ReplicNodesObjects> secondaryList = nodesObjectsState.getSecondaryNodes();
            
            boolean deleteInvWhenStoreMissing = true;
            int deleteInvCnt = 0;
            int deleteSecondaryCnt = 0;
            for (ReplicNodesObjects secondary : secondaryList) {
                System.out.println("for ReplicNodesObjects:"
                        + " - singleNode:" + singleNode
                        + " - secondary.nodeNumber:" + secondary.nodeNumber
                );
                if ((singleNode != null) && (singleNode != secondary.nodeNumber)) continue;
                log( 10,"Start delete::" 
                        + " - ark:" + objectID.getValue()
                        + " - node:" + secondary.getNodeNumber()
                );
                replicationDeleteState.addSecondaryNodes(secondary);
                Connection deleteConnect = null;
                try {
                    deleteConnect = db.getConnection(false);
                    Deletor deletor = Deletor.getDeletor(secondary, deleteInvWhenStoreMissing, deleteConnect, nodes, logger);
                    deletor.setDoDeleteStore(doDeleteStore);
                //if (true) continue;
                    deletor.run();
                    Exception se = deletor.getSaveException();
                    if (se != null) {
                        secondary.setException(se);
                    } else {
                        ReplicationDeleteState deleteState = deletor.getReturnState();
                        deleteSecondaryCnt++;
                        deleteInvCnt += secondary.getInvDeleteCount();
                        log( 2,"After delete::" 
                                + " - ark:" + objectID.getValue()
                                + " - node:" + secondary.getNodeNumber()
                                + " - deleteSecondaryCnt:" + deleteSecondaryCnt
                                + " - deleteInvCnt:" + deleteInvCnt
                        );
                        log( 10,PropertiesUtil.dumpProperties("dump", secondary.retrieveProp()));
                        replicationDeleteState.bumpDeleteCnt();
                    }
                           
                } catch (TException tex) {
                    log(0, MESSAGE + " Exception:" + tex);
                    tex.printStackTrace();
                    throw tex;
            
                } catch (Exception ex) {
                    log(0, MESSAGE + " Exception:" + ex);
                    ex.printStackTrace();
                    throw new TException(ex);
                    
                } finally {
                    if (deleteConnect != null) {
                        try {
                            deleteConnect.close();
                            log( 10,"Connection closed");
                        } catch (Exception ex) { }
                    }
                }
            }
            return replicationDeleteState;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try{
                if (connection != null) connection.close();
            } catch (Exception ex) { }
        }
    }
    
    public Long getSingleNode() {
        return singleNode;
    }

    public void setSingleNode(Long singleNode) {
        this.singleNode = singleNode;
    }

    public NodesObjectsState getNodesObjectsState() {
        return nodesObjectsState;
    }

    public Identifier getObjectID() {
        return objectID;
    }
}

