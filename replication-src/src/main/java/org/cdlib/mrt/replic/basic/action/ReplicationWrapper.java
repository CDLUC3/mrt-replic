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
import org.cdlib.mrt.core.DateState;

import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class ReplicationWrapper
        implements Runnable
{

    protected static final String NAME = "ReplicationWrapper";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected InvNodeObject nodeObject = null;
    protected DPRFileDB db = null;
    protected LoggerInf logger = null;
    protected ReplicationRunInfo replicationInfo = null;
    protected NodeIO nodes = null;

    public ReplicationWrapper(
            InvNodeObject nodeObject,
            ReplicationRunInfo replicationInfo,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        this.nodeObject = nodeObject;
        this.logger = logger;
        this.nodes = nodes;
        this.db = db;
        this.replicationInfo = replicationInfo;
    }

    @Override
    public void run()
    {
        Replicator replicator = null;
        try {
            if (!replicationInfo.isRunReplication()) {
                System.out.println(MESSAGE + "run cancelled:" 
                        + " - nodeseq=" + nodeObject.getNodesid()
                        + " - objectseq=" + nodeObject.getObjectsid()
                );
                resetReplicatedRetry(nodeObject);
                return;
                
            } else {
                replicationInfo.bumpCnt();
                System.out.println(MESSAGE 
                        + "RUN cnt:" + replicationInfo.getCnt()
                        + " - nodeseq=" + nodeObject.getNodesid()
                        + " - objectseq=" + nodeObject.getObjectsid()
                );
            }
            replicator = ReplicActionAbs.getReplicator(nodeObject, nodes, db, logger);
            Thread t = Thread.currentThread();
            String name = t.getName();
            log("START:" + nodeObject.getObjectsid());
            replicator.run();

            if (replicator.getException() != null) {
                log("Exception:" + replicator.getException().toString());
                resetReplicatedError(nodeObject);
                return;
            }
            //resetReplicatedCurrent(nodeObject);

        } catch(Exception e)  {
            e.printStackTrace();
            resetReplicatedError(nodeObject);

        } 
    }
    
    protected boolean resetReplicatedRetry(InvNodeObject primaryNodeObject)
    {
        return ReplicDB.resetReplicatedNull(db, primaryNodeObject, logger);
    }
    
    protected boolean resetReplicatedError(InvNodeObject primaryNodeObject)
    {
        return ReplicDB.resetReplicatedYear(db, primaryNodeObject, logger);
    }
    
    protected boolean resetReplicatedCurrent(InvNodeObject primaryNodeObject)
    {
        return ReplicDB.resetReplicatedCurrent(db, primaryNodeObject, logger);
    }

    private void log(String msg)
    {
        try {
            logger.logMessage(msg, 15);
            if (!DEBUG) return;
            Thread t = Thread.currentThread();
            String name = t.getName();
            System.out.println(MESSAGE + '[' + name + "]:" + msg);
        } catch (Exception ex) { System.out.println("log exception"); }
    }
}

