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


import java.util.List;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.StateInf;

/**
 * Format container class for Fixity Service
 * @author dloy
 */
public class ReplicationAddState
        implements StateInf
{
    private static final String NAME = "ReplicationAddState";
    private static final String MESSAGE = NAME + ": ";

    protected Identifier objectID = null;
    protected List<NodeState> nodes = null;
    protected int replicatonCount = 0;
    protected DateState addDate = null;
    
    public ReplicationAddState() { }
    
    public ReplicationAddState(Identifier objectID, List<NodeState> nodes) { 
        this.objectID = objectID;
        this.nodes = nodes;
        this.replicatonCount = nodes.size();
        setAddDate();
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public DateState getAddDate() {
        return addDate;
    }

    public void setAddDate(DateState addDate) {
        this.addDate = addDate;
    }
    
    public void setAddDate() {
        this.addDate = new DateState();
    }

    public int getReplicationCount() {
        return replicatonCount;
    }

    public void setReplicationCount(int replicatonCount) {
        this.replicatonCount = replicatonCount;
    }
    
    public List<NodeState> getNodes() {
        return nodes;
    }
}
