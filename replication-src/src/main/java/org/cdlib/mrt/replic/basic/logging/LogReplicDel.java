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
import org.cdlib.mrt.replic.basic.action.ReplicationInfo.NodeObjectInfo;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class LogReplicDel
{

    protected static final String NAME = "LogReplicDel";
    protected static final String MESSAGE = NAME + ": ";
    private static final Logger log4j = LogManager.getLogger();
    
    
    protected String service = null;
    protected String serviceProcess = null;
    protected String keyPrefix = null;
    protected Long durationMs = null;
    protected Long addBytes = null;
    protected Long addFiles = null;
    protected AddStateEntryGen stateEntry = null;
    protected ReplicationDeleteState delState = null;
    
    
    public static LogReplicDel getLogReplicDel(ReplicationDeleteState delState)
        throws TException
    {
        return new LogReplicDel(delState);
    }
    
    public LogReplicDel(ReplicationDeleteState delState)
        throws TException
    {
        this.service = "Replic";
        this.serviceProcess = "ReplicDelete";
        this.keyPrefix = "repdel";
        stateEntry = AddStateEntryGen.getAddStateEntryGen(this.keyPrefix, this.service, this.serviceProcess);
        if (delState == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "delState missing");
        }
        this.delState = delState;
        log4j.trace("getLogReplicDel constructor");
        
    }
    
    public void process()
        throws TException
    {
        stateEntry.setObjectID(delState.getObjectID());
        for (ReplicNodesObjects nodeObject: delState.getSecondaryNodes()) {
            
            processEntries(nodeObject);
        }
    }
    
    private void processEntries(ReplicNodesObjects nodeObject)
        throws TException
    {   
        stateEntry.setDurationMs(nodeObject.getDurationMs());
        stateEntry.setProcessNode(nodeObject.nodeNumber);
        if (nodeObject.getStoreDeleteCount() != null) {
            long storeDeleteCountL = nodeObject.getStoreDeleteCount();
            stateEntry.setFiles(storeDeleteCountL);
        }
        if (nodeObject.getVersionNumber() != null) {
            int versionNumberI = (Integer)nodeObject.getStoreDeleteCount();
            stateEntry.setVersion(versionNumberI);
        }
        addEntry();
    }
    
    public void addEntry()
        throws TException
    {
        stateEntry.addLogStateEntry("info", "replicJSON");
    }
}

