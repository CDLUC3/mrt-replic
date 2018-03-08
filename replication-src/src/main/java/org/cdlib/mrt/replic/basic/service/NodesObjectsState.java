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

import java.util.ArrayList;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;

/**
 *
 * @author loy
 */
public class NodesObjectsState 
        implements StateInf
{

    private static final String NAME = "NodeObjectsState";
    private static final String MESSAGE = NAME + ": ";
    
    protected Identifier ark = null;
    protected ReplicNodesObjects primaryNode = null;
    protected ArrayList<ReplicNodesObjects> secondaryNodes = new ArrayList<>();
    protected Integer numberDeletedSecondary = null;
    protected Integer numberDeletedInv = null;
    
    public NodesObjectsState() { }
    
    public NodesObjectsState(Identifier ark)
        throws TException
    { 
        this.ark = ark;
        if (ark == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "ark not supplied");
        }
    }
    
    public NodesObjectsState(Identifier ark, ReplicNodesObjects primaryNode)
        throws TException
    { 
        this.ark = ark;
        this.primaryNode = primaryNode;
        if (ark == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "ark not supplied");
        }
        if (primaryNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "primaryNode not supplied");
        }
    }
    
    public void addSecondaryNode(ReplicNodesObjects node)
        throws TException
    {
        secondaryNodes.add(node);
    }

    public Identifier getArk() {
        return ark;
    }

    public void setArk(Identifier ark) {
        this.ark = ark;
    }

    public ReplicNodesObjects getPrimaryNode() {
        return primaryNode;
    }

    public void setPrimaryNode(ReplicNodesObjects primaryNode) {
        this.primaryNode = primaryNode;
    }

    public ArrayList<ReplicNodesObjects> getSecondaryNodes() {
        return secondaryNodes;
    }

    public void setSecondaryNodes(ArrayList<ReplicNodesObjects> secondaryNodes) {
        this.secondaryNodes = secondaryNodes;
    }

    public int retrieveSecondaryCount() {
        return secondaryNodes.size();
    }

    public Integer getNumberDeletedSecondary() {
        return numberDeletedSecondary;
    }

    public void setNumberDeletedSecondary(Integer numberDeletedSecondary) {
        this.numberDeletedSecondary = numberDeletedSecondary;
    }

    public Integer getNumberDeletedInv() {
        return numberDeletedInv;
    }

    public void setNumberDeletedInv(Integer numberDeletedInv) {
        this.numberDeletedInv = numberDeletedInv;
    }
}
