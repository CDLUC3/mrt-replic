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
package org.cdlib.mrt.replic.basic.logging;

import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.s3.tools.CloudManifestCopyVersion.Stat;
import org.cdlib.mrt.core.FixityStatusType;
import org.cdlib.mrt.log.utility.AddStateEntryGen;
import org.cdlib.mrt.replic.basic.action.ReplicationInfo;
import org.cdlib.mrt.s3.service.NodeIO;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
/**
 * Run fixity
 * @author dloy
 */
public class LogReplicAdd
{

    protected static final String NAME = "LogReplicEntry";
    protected static final String MESSAGE = NAME + ": ";
    private static final Logger log4j = LogManager.getLogger();
    
    protected String serviceProcess = null;
    protected Long durationMs = null;
    protected Stat stat = null;
    protected Long addBytes = null;
    protected Long addFiles = null;
    protected AddStateEntryGen stateEntry = null;
    protected String keyPrefix = null;
    protected ReplicationInfo.NodeObjectInfo nodeObjectInfo = null;
    
    
    public static LogReplicAdd getLogReplicAdd(
            Stat stat,
            ReplicationInfo.NodeObjectInfo nodeObjectInfo)
        throws TException
    {
        return new LogReplicAdd(stat, nodeObjectInfo);
    }
    
    public LogReplicAdd(
            Stat stat,
            ReplicationInfo.NodeObjectInfo nodeObjectInfo)
        throws TException
    {
        this.keyPrefix = "repadd";
        this.serviceProcess = "Replication";
        stateEntry = AddStateEntryGen.getAddStateEntryGen(this.keyPrefix, this.serviceProcess, "ReplicAdd");
        if (stat == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "stat missing");
        }
        this.stat = stat;
        if (nodeObjectInfo == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeObjectInfo missing");
        }
        this.nodeObjectInfo = nodeObjectInfo;
        log4j.trace("LogReplicEntry constructor");
        setEntry();
    }
    
    private void setEntry()
        throws TException
    {
        stateEntry.setDurationMs(stat.getTime + stat.metaTime + stat.putTime);
        stateEntry.setAwsVersion(ReplicationConfig.getAwsVersion());
        stateEntry.setBytes(stat.fileCopySize);
        stateEntry.setSourceNode(nodeObjectInfo.getPrimaryInvNode().getNumber());
        stateEntry.setTargetNode(nodeObjectInfo.getSecondaryInvNode().getNumber());
        stateEntry.setProcessNode(stateEntry.getTargetNode());
        stateEntry.setObjectID(nodeObjectInfo.getObjectID());
        stateEntry.setVersion(nodeObjectInfo.maxAuditVersion);
        stateEntry.setFiles(stat.fileCopyCnt);
    }
    
    
    public void addEntry()
        throws TException
    {
        stateEntry.addLogStateEntry("info", "replicJSON");
    }
}

