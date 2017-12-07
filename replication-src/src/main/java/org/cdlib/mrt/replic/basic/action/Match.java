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
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.cloud.VersionMap;

import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.MatchObjectState;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class Match
        extends ReplicActionAbs
        implements Runnable
{

    protected static final String NAME = "Match";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;

    protected MatchObjectState state = null;
    protected MatchStore matchStore = null;
    protected VersionMap sourceMap = null;
    protected VersionMap targetMap = null;
    protected VersionMap invMap = null;
    protected Exception exception = null;
    protected final NodeIO nodeIO;
    
    public static Match getMatch(
            Identifier objectID,
            NodeIO nodeIO,
            Integer sourceNode,
            Integer targetNode,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return new Match(objectID, nodeIO, sourceNode, targetNode, connection, logger);
    }
    
    protected Match(
            Identifier objectID,
            NodeIO nodeIO,
            Integer sourceNode,
            Integer targetNode,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        this.nodeIO= nodeIO;
        try {
            connection.setAutoCommit(false);
        } catch (Exception ex) {
            throw new TException.SQL_EXCEPTION(ex);
        }
        try {
            state = new MatchObjectState(objectID, nodeIO, sourceNode, targetNode);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public MatchObjectState process()
        throws TException
    {
        try {
            sourceMap = getVersionMap(nodeIO, state.sourceNode, state.objectID, logger);
            state.sourceVersionCnt = sourceMap.getCurrent();
            if (state.targetNode != null) {
                targetMap = getVersionMap(nodeIO, state.targetNode, state.objectID, logger);
                state.targetVersionCnt = targetMap.getCurrent();
            }
            if (DEBUG) {
                MatchStore.dumpMap(MESSAGE + "sourceMap", sourceMap);
                if (targetMap != null) {
                    MatchStore.dumpMap(MESSAGE + "targetMap", targetMap);
                }
            }
            try {
                BuildMapInv buildMap = BuildMapInv.getBuildMapInv(state.objectID, connection, logger);
                invMap = buildMap.process();
                if (invMap != null) {
                    MatchStore.dumpMap(MESSAGE + "invMap", invMap);
                }
                state.invVersionCnt = invMap.getCurrent();
            } catch (Exception ex) {
                invMap = null;
                System.out.println(MESSAGE + ex);
                ex.printStackTrace();
            }
            if (targetMap != null) {
                MatchStore matchStore = MatchStore.getMatchStore( logger);
                MatchResults results = matchStore.process(sourceMap, targetMap);
                if (results.getObjectMatch()) {
                    state.setMatchManifestStore(true);
                }
                List errors = results.getErrors();
                if (errors.size() > 0) {
                    state.setStorageError(errors);
                }
                
            }
            
            if (invMap != null) {
                VersionMap testMap = targetMap;
                if (testMap == null) {
                    testMap = sourceMap;
                }
                MatchStore matchStore = MatchStore.getMatchStore( logger);
                MatchResults results = matchStore.process(testMap, invMap);
                if (results.getObjectMatch()) {
                    state.setMatchManifestInv(true);
                }
                List errors = results.getErrors();
                if (errors.size() > 0) {
                    state.setInvError(errors);
                }

            }
            return state;
            

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException(ex);
            
        } 
    }
    
    public void run()
    {
        try {
            process();

        } catch (TException tex) {
            System.out.println(MESSAGE + tex);
            tex.printStackTrace();
            exception = tex;

        } catch (Exception ex) {
            System.out.println(MESSAGE + ex);
            ex.printStackTrace();
            exception = ex;
            
        }
    }

    public MatchObjectState getMatch() {
        return state;
    }
}

