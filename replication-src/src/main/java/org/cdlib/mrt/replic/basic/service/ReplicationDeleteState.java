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
import java.util.Properties;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.StateInf;

/**
 * Format container class for Fixity Service
 * @author dloy
 */
public class ReplicationDeleteState
        implements StateInf
{
    private static final String NAME = "ReplicDeleteState";
    private static final String MESSAGE = NAME + ": ";

    protected Identifier objectID = null;
    protected boolean storeDelete = false;
    protected int deleteCount = 0;
    protected DateState deleteDate = null;
    protected ArrayList<ReplicNodesObjects> secondaryNodes = new ArrayList();
    
    public ReplicationDeleteState() { }
    
    public ReplicationDeleteState(boolean storeDelete, Identifier objectID, int deleteCount) { 
        this.storeDelete = storeDelete;
        this.objectID = objectID;
        this.deleteCount = deleteCount;
        setDeleteDate();
    }

    public boolean isStoreDelete() {
        return storeDelete;
    }

    public void setStoreDelete(boolean storeDelete) {
        this.storeDelete = storeDelete;
    }

    public int getDeleteCount() {
        return deleteCount;
    }

    public void setDeleteCount(int deleteCount) {
        this.deleteCount = deleteCount;
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public DateState getDeleteDate() {
        return deleteDate;
    }

    public void setDeleteDate(DateState deleteDate) {
        this.deleteDate = deleteDate;
    }
    
    public void setDeleteDate() {
        this.deleteDate = new DateState();
    }

    public void addSecondaryNodes(ReplicNodesObjects addNode) {
        secondaryNodes.add(addNode);
    }

    public void bumpDeleteCnt() {
        deleteCount++;
    }

    public ArrayList<ReplicNodesObjects> getSecondaryNodes() {
        return secondaryNodes;
    }
    
    public int getSecondaryCount() {
        return secondaryNodes.size();
    }
}
