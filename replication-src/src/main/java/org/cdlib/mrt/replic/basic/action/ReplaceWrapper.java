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

import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.replic.basic.service.ReplicationAddState;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceInf;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class ReplaceWrapper
        implements Runnable
{

    protected static final String NAME = "AddWrapper";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected final Identifier objectID;
    protected final LoggerInf logger;
    protected final ReplicationServiceInf replicationService;
    protected TException tException = null;
    protected ReplicationAddState responseState = null;
    protected String msg = null;
    
    protected int nodeNum = 0;
    
    public static ReplaceWrapper getReplaceWrapper(
            int nodeNum,
            Identifier objectID,
            ReplicationServiceInf replicationService,
            LoggerInf logger)
        throws TException
    {
        ReplaceWrapper addWrapper = new ReplaceWrapper(nodeNum, objectID, replicationService, logger);
        return addWrapper;
    }

    protected ReplaceWrapper(
            int nodeNum,
            Identifier objectID,
            ReplicationServiceInf replicationService,
            LoggerInf logger)
        throws TException
    {
        this.nodeNum = nodeNum;
        this.objectID = objectID;
        this.replicationService = replicationService;
        this.logger = logger;
    }

    @Override
    public void run()
    {
        try {
            System.out.println(MESSAGE + "Start processing:"
                    + " - objectID=" + objectID.getValue()
            );
 
            ReplicationDeleteState deleteState = replicationService.deleteInv(nodeNum, objectID);
            responseState = replicationService.add(objectID);

        } catch(TException tex)  {
            tException = tex;
    
        } finally {
            complete();
        }
    }
    
    protected void complete()
    {
        if (tException != null) {
            msg = objectID.getValue() + " - FAIL:" + tException;
            log(msg);
            return;
        }
        if (responseState != null) {
            
            msg = objectID.getValue() + " - OK:" 
                    + " - date:" + responseState.getAddDate().getIsoDate()
                    + " - cnt:" + responseState.getReplicationCount()
                    ;
            log(msg);
        }
    }

    private void log(String msg)
    {
        try {
            logger.logMessage(msg, 0, true);
            System.out.println(msg);
        } catch (Exception ex) { System.out.println("log exception:" + ex); }
    }
}

