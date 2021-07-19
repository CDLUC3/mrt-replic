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

import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.Properties;

import org.cdlib.mrt.core.FixityStatusType;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.replic.basic.service.ReplicationScheme;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Format container class for Fixity Service
 * @author dloy
 */
public class ReplicationServiceState
        implements StateInf
{
    private static final String NAME = "ReplicationServiceState";
    private static final String MESSAGE = NAME + ": ";
    public enum StateStatus  { unknown, paused, running, shuttingdown, shutdown; }
    public static final boolean DEBUG=true;

    protected String name = null;
    protected String identifier = null;
    protected String description = null;
    protected String version = null;
    protected ReplicationScheme serviceScheme = null;
    protected int threadPool = 1;
    protected int queueCapacity = 100;
    protected ServiceStatus status = ServiceStatus.unknown;
    protected DateState lastModified = null;
    protected Long processCount = null;
    protected List<NodeCountState> nodeCounts = null;
    protected Boolean replicationSQL = null;
    protected Boolean runReplication = null;
    protected Boolean replicationProcessing = null;
    protected Long cnt = null;
    protected Integer addQueueCnt = null;
    protected String replicQualify = null;
    protected String nodePath = null;

    public ReplicationServiceState() { }
    
    public ReplicationServiceState(ReplicationServiceState instate)
    {
        this.setName(instate.getName());
        this.setIdentifier(instate.getIdentifier());
        this.setDescription(instate.getDescription());
        this.setVersion(instate.getVersion());
        this.setServiceScheme(instate.getServiceScheme());
        this.setThreadPool(instate.getThreadPool());
        this.setNodePath(instate.getNodePath());
        this.setQueueCapacity(instate.getQueueCapacity());
    }

    public ReplicationServiceState(ReplicationConfig replicationConfig)
        throws TException
    {
        JSONObject serviceJSON = replicationConfig.getServiceJSON();
        JSONObject stateJSON = replicationConfig.getStateJSON();
        setValues(serviceJSON, stateJSON);
    }

    /**
     * 
     * @return non required description of entry
     */
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public DateState getLastModified() {
        return lastModified;
    }

    public void setLastModified(DateState lastModified) {
        this.lastModified = lastModified;
    }

    /**
     * 
     * @return Name of fixity service
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }



    public void setServiceScheme(ReplicationScheme serviceScheme) {
        this.serviceScheme = serviceScheme;
    }

    /**
     * 
     * @return Fixity service scheme
     */
    public String getServiceScheme() {
        if (serviceScheme == null) return null;
        return serviceScheme.toString();
    }

    public void setServiceScheme(String schemeLine)
    {
        try {
            if (DEBUG) System.out.println("schemeLine=" + schemeLine);
            this.serviceScheme = ReplicationScheme.buildSpecScheme("replication", schemeLine);
        } catch (Exception ex) {
            System.out.println("WARNING: setServiceScheme fails:" + ex);
        }
    }

    public ReplicationScheme retrieveServiceScheme()
    {
        return serviceScheme;
    }

    /**
     * 
     * @return fixity status: running, shuttingdown, shutdown
     */
    public String getStatus() {
        return status.toString();
    }

    public void setStatus(ServiceStatus status) {
        this.status = status;
    }

    /**
     * 
     * @return Number of concurrent executing threads
     */
    public int getThreadPool() {
        return threadPool;
    }

    public void setThreadPool(int threadPool) {
        this.threadPool = threadPool;
    }

    public void setThreadPool(String threadPoolS) {
        this.threadPool = Integer.parseInt(threadPoolS);
    }
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Set all entry values based on Properties
     * @param prop 
     */
    public void setValues(JSONObject serviceJSON, JSONObject stateJSON)
         throws TException
    {
        try {
            this.setName(stateJSON.getString("name"));
            this.setIdentifier(stateJSON.getString("id"));
            this.setDescription(stateJSON.getString("description"));
            this.setVersion(stateJSON.getString("version"));
            this.setServiceScheme(stateJSON.getString("serviceScheme"));
            this.setThreadPool(serviceJSON.getString("threadPool"));
            this.setQueueCapacity(serviceJSON.getInt("queueCapacity"));
            this.setNodePath(serviceJSON.getString("nodePath"));
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }


    /**
     * 
     * @return number of entries processed since last startup
     */
    public Long getProcessCount() {
        return processCount;
    }

    public void setProcessCount(Long processCount) {
        this.processCount = processCount;
    }

    public DateState getCurrentReportDate()
    {
        return new DateState();
    }

    public List<NodeCountState> getNodeCounts() {
        return nodeCounts;
    }

    public void setNodeCounts(List<NodeCountState> nodeCounts) {
        this.nodeCounts = nodeCounts;
    }

    public Boolean isReplicationSQL() {
        return replicationSQL;
    }

    public void setReplicationSQL(Boolean replicationSQL) {
        this.replicationSQL = replicationSQL;
    }

    public Boolean isRunReplication() {
        return runReplication;
    }

    public void setRunReplication(Boolean runReplication) {
        this.runReplication = runReplication;
    }

    public Boolean isReplicationProcessing() {
        return replicationProcessing;
    }

    public void setReplicationProcessing(Boolean replicationProcessing) {
        this.replicationProcessing = replicationProcessing;
    }

    public Long getCnt() {
        return cnt;
    }

    public void setCnt(AtomicLong inCnt) {
        if (inCnt == null) {
            this.cnt = 0L;
        } else {
            this.cnt = inCnt.get();
        }
    }

    public Integer getAddQueueCnt() {
        return addQueueCnt;
    }

    public void setAddQueueCnt(Integer addQueueCnt) {
        this.addQueueCnt = addQueueCnt;
    }

    public String getReplicQualify() {
        return replicQualify;
    }

    public void setReplicQualify(String replicQualify) {
        this.replicQualify = replicQualify;
    }

    public String getNodePath() {
        return nodePath;
    }

    public void setNodePath(String nodePath) {
        this.nodePath = nodePath;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }
    
}
