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

import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Primary Fixity container used by RunFixity. This class contains the primary control
 * properties used for fixity process handling.
 * 
 * 
 *  runFixity - this flag controls whether to start or stop fixity
 *            - true=fixity should be running or starting to run
 *            - false=stop fixity and exit routine
 *  fixityProcessing - this flag determines if fixity is running
 *            - true=fixity is now running
 *            - false=fixity has stopped
 * capacity - size of fixity block size
 * threadPool - number of concurrent threads to run fixity
 * interval - interval between the last and next execution of fixity for a particular entry
 * queueSleep - sleep interval between each fixity test to be performed
 * 
 * @author dloy
 */

public class ReplicationRunInfo
{
    private static final String NAME = "ReplicationRunInfo";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");

    private static final boolean DEBUG = true;
    protected volatile boolean replicationSQL = false;
    protected volatile boolean runReplication = false;
    protected volatile boolean replicationProcessing = false;
    protected volatile boolean allowScan = true;
    protected volatile int queueCapacity = 100;
    protected volatile int threadPool = 4;
    protected volatile String replicQualify = null;
    protected AtomicLong cnt = new AtomicLong();
    protected ReplicationServiceState state = null;
    protected ReplicationConfig replicConfig = null;

    public ReplicationRunInfo() { }

    public ReplicationRunInfo(ReplicationConfig replicConfig)
        throws TException
    {
        this.state = replicConfig.getServiceState();
        this.replicConfig = replicConfig;
        set();
    }
    
    public synchronized void set()
        throws TException
    {
        try {
            threadPool = state.getThreadPool();
            queueCapacity = state.getQueueCapacity();
            replicQualify = state.getReplicQualify();
    
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public boolean isReplicationSQL() {
        return replicationSQL;
    }

    public synchronized void setReplicationSQL(boolean replicationSQL) {
        this.replicationSQL = replicationSQL;
    }


    public AtomicLong getCnt() {
        return cnt;
    }

    public void setCnt(AtomicLong cnt) {
        this.cnt = cnt;
    }
    
    public boolean isReplicationProcessing() {
        return replicationProcessing;
    }

    public synchronized void setReplicationProcessing(boolean replicationProcessing) {
        this.replicationProcessing = replicationProcessing;
    }

    public boolean isRunReplication() {
        return runReplication;
    }

    public void setRunReplication(boolean runReplication) {
        this.runReplication = runReplication;
    }

    public boolean isAllowScan() {
        return allowScan;
    }

    public void setAllowScan(boolean allowScan) {
        this.allowScan = allowScan;
    }

    public int getThreadPool() {
        return threadPool;
    }

    public void setThreadPool(int threadPool) {
        this.threadPool = threadPool;
    }

    public ReplicationScheme getReplicationScheme()
    {
        return state.retrieveServiceScheme();
    }
    
    public long bumpCnt()
    {
        return cnt.getAndAdd(1);
    }

    public String getReplicQualify() {
        return replicQualify;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public String dump(String header)
        throws TException
    {
        StringBuffer buf = new StringBuffer(100);
        buf.append("ReplicationState:" + header + NL);
        String msg = ""
                + " - replicationProcessing=" + replicationProcessing + NL
                + " - queueCapacity=" + queueCapacity + NL
                + " - threadPool=" + threadPool + NL
                + " - cnt=" + cnt + NL
                ;
        buf.append(msg);
        return buf.toString();
               
    }
    
}
