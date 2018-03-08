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
import java.util.Properties;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.StateInf;

/**
 * Format container class for Fixity Service
 * @author dloy
 */
public class MatchObjectState
        implements StateInf
{
    private static final long serialVersionUID = 31L;
    private static final String NAME = "MatchObjectState";
    private static final String MESSAGE = NAME + ": ";

    public Identifier objectID = null;
    public DateState date = new DateState();
    public Integer sourceNode = 0;
    public Integer targetNode = 0;
    public Integer sourceVersionCnt = 0;
    public Integer targetVersionCnt = 0;
    public Integer invVersionCnt = 0;
    public String storageBase = null;
    public List<String> storageError = null;
    public List<String> invError = null;
    protected Boolean matchManifestStore = null;
    protected Boolean matchManifestInv = null;
    protected String nodeName = null;
    
    public MatchObjectState() { }
    
    public MatchObjectState(
            Identifier objectID,
            String storageBase,
            Integer sourceNode,
            Integer targetNode
    )
    
    { 
        this.objectID = objectID;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.storageBase = storageBase;
    }
    
    public MatchObjectState(
            Identifier objectID,
            NodeIO nodeIO,
            Integer sourceNode,
            Integer targetNode
    )
    
    { 
        this.objectID = objectID;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.nodeName = nodeIO.getNodeName();
    }
    
    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer();
        buf.append(MESSAGE + header + "\n");
        buf.append("Date:" + date.getIsoDate() + "\n");
        buf.append("Inputs:\n"
                + " - objectID:" + objectID.getValue() + "\n"
                + " - storageBase:" + storageBase + "\n"
                + " - sourceNode:" + sourceNode + "\n"
                + " - targetNode:" + targetNode + "\n"
                + "--------------------------------------------\n"
        );
        buf.append("Results:\n"
                + " - sourceVersionCnt:" + sourceVersionCnt + "\n"
                + " - targetVersionCnt:" + targetVersionCnt + "\n"
                + " - invVersionCnt:" + invVersionCnt + "\n"
                + " - matchManifestStore:" + matchManifestStore + "\n"
                + " - matchManifestInv:" + matchManifestInv + "\n"
                + " - combine:" + getCombine() + "\n"
                + "--------------------------------------------\n"
        );
        if (storageError != null) {
            buf.append("--------------------------------------------\n");
            buf.append("Storage Error:\n");
            for (int i=0; i<storageError.size(); i++) {
                String err = storageError.get(i);
                buf.append("Err[" + i + "]:" + err);
            }
            buf.append("--------------------------------------------\n");
        }
        if (invError != null) {
            buf.append("Inv Error:\n");
            for (int i=0; i<invError.size(); i++) {
                String err = invError.get(i);
                buf.append("Err[" + i + "]:" + err);
            }
            buf.append("--------------------------------------------\n");
        }
        
        return buf.toString();
        
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public Integer getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(Integer sourceNode) {
        this.sourceNode = sourceNode;
    }

    public Integer getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(Integer targetNode) {
        this.targetNode = targetNode;
    }

    public Integer getSourceVersionCnt() {
        return sourceVersionCnt;
    }

    public void setSourceVersionCnt(Integer sourceVersionCnt) {
        this.sourceVersionCnt = sourceVersionCnt;
    }

    public Integer getTargetVersionCnt() {
        return targetVersionCnt;
    }

    public void setTargetVersionCnt(Integer targetVersionCnt) {
        this.targetVersionCnt = targetVersionCnt;
    }

    public Integer getInvVersionCnt() {
        return invVersionCnt;
    }

    public void setInvVersionCnt(Integer invVersionCnt) {
        this.invVersionCnt = invVersionCnt;
    }

    public String getStorageBase() {
        return storageBase;
    }

    public void setStorageBase(String storageBase) {
        this.storageBase = storageBase;
    }

    public Boolean getMatchManifestStore() {
        return matchManifestStore;
    }

    public void setMatchManifestStore(Boolean matchManifestStore) {
        this.matchManifestStore = matchManifestStore;
    }

    public Boolean getMatchManifestInv() {
        return matchManifestInv;
    }

    public void setMatchManifestInv(Boolean matchManifestInv) {
        this.matchManifestInv = matchManifestInv;
    }

    public List getStorageError() {
        return storageError;
    }

    public void setStorageError(List<String> storageError) {
        this.storageError = storageError;
    }

    public List<String> getInvError() {
        return invError;
    }

    public void setInvError(List<String> invError) {
        this.invError = invError;
    }

    public DateState getDate() {
        return date;
    }

    public void setDate(DateState date) {
        this.date = date;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
    
    public String getCombine()
    {
        StringBuffer combine = new StringBuffer();
        combine.append(objectID.getValue());
        combine.append('|');
        if (sourceNode != null) {
            combine.append("" + sourceNode);
        }
        combine.append('|');
        if (targetNode != null) {
            combine.append("" + targetNode);
        }
        combine.append('|');
        if (sourceVersionCnt != null) {
            combine.append("" + sourceVersionCnt);
        }
        combine.append('|');
        if (targetVersionCnt != null) {
            combine.append("" + targetVersionCnt);
        }
        combine.append('|');
        if (invVersionCnt != null) {
            combine.append("" + invVersionCnt);
        }
        combine.append('|');
        if (matchManifestStore != null) {
            combine.append("" + matchManifestStore);
        }
        combine.append('|');
        if (matchManifestInv != null) {
            combine.append("" + matchManifestInv);
        }
        combine.append('|');
        int errorCnt = 0;
        if (storageError != null) {
            errorCnt += storageError.size();
        }
        if (invError != null) {
            errorCnt += invError.size();
        }
        combine.append("" + errorCnt);
        combine.append('|');
        if (getDate() != null) {
            combine.append(getDate());
        }
        return combine.toString();
    }
}
