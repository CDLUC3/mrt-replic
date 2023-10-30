/*
Copyright (c) 2005-2010, Regents of the University of California
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
**********************************************************/
package org.cdlib.mrt.replic.basic.content;


import java.util.Properties;


import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.InvUtil;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.core.DateState;
import static org.cdlib.mrt.inv.content.ContentAbs.NODES_OBJECTS;
import static org.cdlib.mrt.inv.content.ContentAbs.setNum;
import static org.cdlib.mrt.inv.content.ContentAbs.setNumLong;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
/**
 * Container class for inv Object content
 * @author dloy
 */
public class ReplicNodesObjects
        extends InvNodeObject
        implements StateInf
{
    private static final String NAME = "InvNodeObject";
    private static final String MESSAGE = NAME + ": ";
    

    public long nodeNumber = 0;
    public Boolean deleteStore = null;
    public Boolean deleteInv = null;
    protected Long durationMs = null;
    protected Integer invDeleteCount = null;
    protected Integer storeDeleteCount = null;
    protected DateState deleteDate = null;
    protected Exception exception = null;
    
    public ReplicNodesObjects(LoggerInf logger)
        throws TException
    { 
        super(logger);
    }
    
    public ReplicNodesObjects(InvNodeObject invNodeObject, long nodeNumber, LoggerInf logger)
        throws TException
    { 
        super(logger);
        Properties prop = invNodeObject.retrieveProp();
        prop.setProperty("number", "" + nodeNumber);
        setProp(prop);
    }
    
    public ReplicNodesObjects(Properties prop, LoggerInf logger)
        throws TException
    {
        super(logger);
        setProp(prop);
    }

    /**
     * From a Properties container set the local values for the nodes table
     * @param prop nodes Properties
     * @throws TException 
     */
    public void setProp(Properties prop)
        throws TException
    {
        if ((prop == null) || (prop.size() == 0)) return;
        try {
            setId(prop.getProperty("id"));
            setNodesid(prop.getProperty("inv_node_id"));
            setObjectsid(prop.getProperty("inv_object_id"));
            setRole(prop.getProperty("role"));
            setCreatedDB(prop.getProperty("created"));
            setReplicatedDB(prop.getProperty("replicated"));
            setVersionNumber(prop.getProperty("version_number"));
            setNodeNumber(prop.getProperty("number"));
            setDurationMs(prop.getProperty("durationMs"));
            
        } catch (Exception ex) {
            ex.printStackTrace(); //!!!
            throw new TException(ex);
        }
    }
    
    public void setNodeNumber(String nodeNumberS) {
        this.nodeNumber = setNumLong(nodeNumberS);
    }

    public long getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(long nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public Boolean getDeleteStore() {
        return deleteStore;
    }

    public void setDeleteStore(Boolean deleteStore) {
        this.deleteStore = deleteStore;
    }

    public Boolean getDeleteInv() {
        return deleteInv;
    }

    public void setDeleteInv(Boolean deleteInv) {
        this.deleteInv = deleteInv;
    }

    public Integer getInvDeleteCount() {
        return invDeleteCount;
    }

    public void setInvDeleteCount(Integer invDeleteCount) {
        this.invDeleteCount = invDeleteCount;
    }

    public Integer getStoreDeleteCount() {
        return storeDeleteCount;
    }

    public void setStoreDeleteCount(Integer storeDeleteCount) {
        this.storeDeleteCount = storeDeleteCount;
    }

    public DateState getDeleteDate() {
        return deleteDate;
    }

    public void setDeleteDate(DateState deleteDate) {
        this.deleteDate = deleteDate;
    }

    public Long getDurationMs() {
        return durationMs;
    }
   
    public void setDurationMs(String durationMsS) {
        this.durationMs = setNumLong(durationMsS);
    }
    
    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
    
}