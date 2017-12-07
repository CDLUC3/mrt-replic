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
import java.util.Properties;

import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class Deletor
        extends ReplicActionAbs
{

    protected static final String NAME = "Deletor";
    protected static final String MESSAGE = NAME + ": ";

    protected DeleteInv deleteInv = null;
    protected DeleteStore deleteStore = null;
    protected InvObject invObject = null;
    protected Exception saveException = null;
    protected Properties storeResponse = null;
    protected int deleteCnt = 0;
    protected boolean doDeleteStore = true;
    protected boolean storeDelete = false;
    protected boolean deleteInvWhenStoreMissing = false;
    protected ReplicationDeleteState returnState = null;
    protected NodeIO nodes = null;
    
    public static Deletor getDeletor(
            ReplicNodesObjects nodeObject,
            boolean deleteInvWhenStoreMissing,
            Connection connection,
            NodeIO nodes,
            LoggerInf logger)
        throws TException
    {
        return new Deletor(nodeObject, deleteInvWhenStoreMissing, connection, nodes, logger);
    }
    
    protected Deletor(
            ReplicNodesObjects nodeObject,
            boolean deleteInvWhenStoreMissing,
            Connection connection,
            NodeIO nodes,
            LoggerInf logger)
        throws TException
    {
        super(nodeObject, connection, logger);
        this.deleteInvWhenStoreMissing = deleteInvWhenStoreMissing;
        this.nodes = nodes;
        try {
            connection.setAutoCommit(false);
        } catch (Exception ex) {
            throw new TException.SQL_EXCEPTION(ex);
        }
        if (nodeObject.getRole() != Role.secondary) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                    + " requires Role secondary:" + nodeObject.getRole());
        }
        log(10,MESSAGE + PropertiesUtil.dumpProperties("nodeObject", nodeObject.retrieveProp()));
        invObject = InvDBUtil.getObject(nodeObject.getObjectsid(), connection, logger);
        log( 5,MESSAGE + PropertiesUtil.dumpProperties("invObjectObject", invObject.retrieveProp()));
        deleteStore = DeleteStore.getDeleteStore(nodeObject, connection, nodes, logger);
        deleteInv = DeleteInv.getDeleteInv(nodeObject, connection, logger);
    }

    public void run()
    {
        if (doDeleteStore) {
            try {
                deleteStore.process();
                storeResponse = deleteStore.getStoreResponse();
                storeDelete = true;
                log( 10,MESSAGE + PropertiesUtil.dumpProperties("run", storeResponse));

            } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                log( 0,MESSAGE + "Info: - Requested item not found:" 
                        + " - deleteInvWhenStoreMissing=" + deleteInvWhenStoreMissing
                        + rinf);
                if (!deleteInvWhenStoreMissing) return;

            } catch (Exception ex) {
                saveException = ex;
                log( 0,"Info: Exception deleteStore" + ex 
                        + PropertiesUtil.dumpProperties("deleteStore", nodeObject.retrieveProp()));
                ex.printStackTrace();
                return;
            }
        } else {
            deleteInvWhenStoreMissing = true;
        }

        try {
            deleteCnt = deleteInv.delete();
            log( 10,MESSAGE + "inv deleteCnt=" + deleteCnt);
                
        } catch (Exception ex) {
            saveException = ex;
            log( 0,"Exception deleteInv" + ex 
                    + PropertiesUtil.dumpProperties("deleteStore", nodeObject.retrieveProp()));
            ex.printStackTrace();
            return;
        }
        ReplicationDeleteState returnState = new ReplicationDeleteState(storeDelete, invObject.getArk(), deleteCnt);
        this.returnState = returnState;
    }

    public Exception getSaveException() {
        return saveException;
    }

    public Properties getStoreResponse() {
        return storeResponse;
    }

    public ReplicationDeleteState getReturnState() {
        return returnState;
    }

    public boolean isDoDeleteStore() {
        return doDeleteStore;
    }

    public void setDoDeleteStore(boolean doDeleteStore) {
        this.doDeleteStore = doDeleteStore;
    }


}

