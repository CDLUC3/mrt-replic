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
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.NodesObjectsState;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class NodesObjects
        extends ReplicActionAbs
{

    protected static final String NAME = "NodesObjects";
    protected static final String MESSAGE = NAME + ": ";

    protected NodesObjectsState nodesObjectState = null;
    protected Exception saveException = null;
    protected Properties storeResponse = null;
    protected ReplicNodesObjects primaryNode = null;
    protected Identifier ark = null;
    protected ArrayList<ReplicNodesObjects> noList = new ArrayList<>();
    
    public static NodesObjects getNodeObjects(
            Identifier ark,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return new NodesObjects(ark, connection, logger);
    }
    
    protected NodesObjects(
            Identifier ark,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        this.ark = ark;
        if (this.ark == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "ark missing");
        }
        try {
            connection.setAutoCommit(true);
            Properties [] propArray = InvDBUtil.getNodeObjects(ark, connection, logger);
            if (propArray == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "not found - ark:" + ark.getValue());
            }
            
            for (Properties prop : propArray) {
                ReplicNodesObjects nodeObject = new ReplicNodesObjects(prop, logger);
                noList.add(nodeObject);
            }
            nodesObjectState = new NodesObjectsState(ark);
            setState();

            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.SQL_EXCEPTION(ex);
        } 
    }

    private void setState()
        throws TException
    {
        try {
            for (ReplicNodesObjects nodeObject : noList) {
                if (nodeObject.getRole() == Role.primary) {
                    if (nodesObjectState.getPrimaryNode() != null) {
                        throw new TException.INVALID_ARCHITECTURE("Multiple primaries for " + ark.getValue());
                    }
                    primaryNode = nodeObject;
                    nodesObjectState.setPrimaryNode(nodeObject);
                } else {
                    
                    nodesObjectState.addSecondaryNode(nodeObject);
                }
            }
            
        } catch (Exception ex) {
            throw new TException.SQL_EXCEPTION(ex);
        } 
        
    }

    public NodesObjectsState getNodesObjectState() {
        return nodesObjectState;
    }
}

