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
import java.util.ArrayList;
import java.util.Properties;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import static org.cdlib.mrt.replic.basic.action.ObjectReplication.MESSAGE;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.replic.basic.service.NodeState;
import org.cdlib.mrt.replic.basic.service.ReplicationAddState;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.utility.DateUtil;

/**
 * Run fixity
 * @author dloy
 */
public class Replicator
        extends ReplicActionAbs
{

    protected static final String NAME = "Replicator";
    protected static final String MESSAGE = NAME + ": ";

    protected ReplicationInfo info = null;
    protected ObjectReplication objectReplication = null;
    protected NodeIO nodes = null;
    protected Long startTime = null;
    protected Long stopTime = null;
    public static enum ReplicatorState {
        initial, run, ok, fail;
    }
    protected ReplicatorState repstate = ReplicatorState.initial;
    
    public static Replicator getReplicator(
            InvNodeObject nodeObject,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        return new Replicator(nodeObject, nodes, db, logger);
    }
    
    protected Replicator(
            InvNodeObject nodeObject,
            NodeIO nodes,
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        super(nodeObject, db, logger);
        repstate = ReplicatorState.run;
        startTime = DateUtil.getEpochUTCDate();
        Connection connect = null;
        this.nodes = nodes;
        try {
            connect = db.getConnection(true);
            info = ReplicationInfo.getReplicationInfo(this.nodeObject, connect, logger);
            info.buildInfo();
            
        } catch (TException tex) {
            throw tex;
            
        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
        
        objectReplication = ObjectReplication.getObjectReplication(info, db, nodes);
    }

    public void run()
    {
        
        try {
            if (info.getSecondaryCount() <= 0) {
                System.out.println(MESSAGE + "nothing to process - "
                        + PropertiesUtil.dumpProperties(NAME, nodeObject.retrieveProp())
                );
                InvNodeObject primary = info.primaryInvNodeObject;
                resetReplicationDate(info.getMaxReplication(), primary);
                return;
            }
            objectReplication.process();
            repstate = ReplicatorState.ok;

        } catch (Exception ex) {
            String msg = MESSAGE + "Exception for nodeid=" + nodeObject.getNodesid() 
                    + " - objectid=" + nodeObject.getObjectsid();
            try {
                Properties entryProp = nodeObject.retrieveProp();
                msg = msg + " - " + PropertiesUtil.dumpProperties("entry", entryProp);
            } catch (Exception xx) { 
            }
            
            System.out.println(MESSAGE + msg + " - Exception:" + ex);
            ex.printStackTrace();
            logger.logError(msg, 2);
            repstate = ReplicatorState.fail;
            setException(ex);
            
        } finally {
            stopTime = DateUtil.getEpochUTCDate();
            long processTime = stopTime - startTime;
            DateState ds = new DateState();
            String msg = "Replicator Time(" + ds.getIsoDate() + "): " 
                    + " - id=" +  info.getObjectID().getValue() 
                    + " - state=" +  repstate.toString() 
                    + " - time=" + processTime
                    ;
            logger.logMessage(msg, 2, true);
        }

    }
    
    public void resetReplicationDate(DateState maxReplicationDate, InvNodeObject primary) 
        throws TException
    {
        Connection connect = null;

        try {
            connect = db.getConnection(false);
            if (maxReplicationDate == null) {
                ReplicDB.resetReplicatedNull(connect, nodeObject, logger);
                System.out.println(MESSAGE + "resetReplicationDate: null");
            } else {
                primary.setReplicated(maxReplicationDate);
                ReplicDB.resetReplicated(connect, primary, logger);
                System.out.println(MESSAGE + "resetReplicationDate max:" + maxReplicationDate.getIsoDate());
            }
            connect.commit();
            
        } catch (Exception ex) {
 
            try {
                connect.rollback();
            } catch (Exception cex) {
                System.out.println("WARNING: rollback Exception:" + cex);
            }

            
        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
    }
    
    public ReplicationAddState getResult()
    {
        ArrayList<NodeState> nodeNumbers = info.getReplicationNodeNumbers();
        ReplicationAddState addState = new ReplicationAddState(info.getObjectID(), nodeNumbers);
        return addState;
    }
    
    public void dumpInfo(String header)
    {
        info.dump(header);
    }


}

