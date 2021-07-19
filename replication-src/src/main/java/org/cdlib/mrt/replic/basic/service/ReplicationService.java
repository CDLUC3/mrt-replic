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


import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.action.Versions;
import org.cdlib.mrt.inv.service.VersionsState;
import org.cdlib.mrt.inv.service.Version;
import org.cdlib.mrt.inv.service.VFile;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.replic.basic.action.FileInput;
import org.cdlib.mrt.replic.basic.action.ReplaceWrapper;
import org.cdlib.mrt.replic.basic.app.ReplicationServiceInit;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Inv Service
 * @author  dloy
 */

public class ReplicationService
    implements ReplicationServiceInf
{
    private static final String NAME = "ReplicationService";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;
    private static final boolean THREADDEBUG = false;
    protected LoggerInf logger = null;
    protected Exception exception = null;
    protected ReplicationServiceHandler replicationServiceHandler = null;

    public static ReplicationService getReplicationService(ReplicationServiceHandler replicationServiceHandler)
            throws TException
    {
        return new ReplicationService(replicationServiceHandler);
    }

    public static ReplicationService getReplicationService(ReplicationConfig replicConfig)
            throws TException
    {
        return new ReplicationService(replicConfig);
    }

    protected ReplicationService(ReplicationServiceHandler replicationServiceHandler)
        throws TException
    {
        this.replicationServiceHandler = replicationServiceHandler;
        this.logger = replicationServiceHandler.getLogger();
    }

    protected ReplicationService(ReplicationConfig replicConfig)
        throws TException
    {
        this.replicationServiceHandler = new ReplicationServiceHandler(replicConfig);
        this.logger = replicationServiceHandler.getLogger();
    }
    
    @Override
    public ReplicationAddState add(
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("add entered");
        return replicationServiceHandler.addObject(objectID);
    }
    
    @Override
    public ReplicationAddState addReplicInv(
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("add entered");
        return replicationServiceHandler.addReplicInv(objectID);
    }
    
    @Override
    public ReplicationServiceState replaceBackground(
            int nodeNum,
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("addBackground");  
        ReplicationServiceHandler handler = this.getReplicationServiceHandler();

        logger = this.getLogger();
        ReplaceWrapper replaceWrapper = ReplaceWrapper.getReplaceWrapper(nodeNum, objectID, this, logger);
        System.out.println(MESSAGE + "addBackground queue:"
                + " - nodeNum=" + nodeNum
                + " - objectID=" + objectID.getValue()
        );
        handler.newAddQueue(replaceWrapper);
        return this.getReplicationServiceStatus();
    }
    
    @Override
    public ReplicationDeleteState delete(
            boolean deleteInvWhenStoreMissing,
            int nodeNum,
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("delete entered");
        return replicationServiceHandler.delete(nodeNum, objectID);
    }
    
    @Override
    public ReplicationDeleteState deleteInv(
            int nodeNum,
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("delete entered");
        return replicationServiceHandler.deleteObjectNode(false, true, nodeNum, objectID);
    }
    
    @Override
    public ReplicationDeleteState deleteSecondary(
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("delete entered");
        return replicationServiceHandler.deleteSecondary(objectID);
    }
       
    @Override
    public ReplicationPropertiesState doCleanup()
        throws TException
    {
        if (DEBUG) System.out.print("doCleanup entered");
        return replicationServiceHandler.doCleanup();
    }
    /**
     * Match duplicated objects on different nodes
     * @param sourceNode input node
     * @param targetNode copied or virtual  node
     * @param objectID object identifier
     * @return results of match
     * @throws TException 
     */
    public MatchObjectState matchObjects(
            int sourceNode,
            Integer targetNode,
            Identifier objectID)
        throws TException
    {
        if (DEBUG) System.out.print("delete entered");
        return replicationServiceHandler.matchObjects(sourceNode, targetNode, objectID);
    }
    
    @Override
    public ReplicationAddMapState addMap(
            Identifier collectionID,
            int nodeNum)
        throws TException
    {
        if (DEBUG) System.out.print("delete entered");
        return replicationServiceHandler.addMap(collectionID, nodeNum);
    }

    @Override
    public ReplicationServiceState getReplicationServiceState()
        throws TException
    {
        ReplicationServiceState invServiceState = replicationServiceHandler.getReplicationServiceState();
        return invServiceState;
    }

    @Override
    public ReplicationServiceState getReplicationServiceStatus()
        throws TException
    {
        ReplicationServiceState invServiceState = replicationServiceHandler.getReplicationServiceStatus();
        return invServiceState;
    }

    @Override
    public ReplicationServiceState pause()
        throws TException
    {
        replicationServiceHandler.pauseReplication();
        ReplicationServiceState invServiceState = replicationServiceHandler.getReplicationServiceState();
        return invServiceState;
    }

    @Override
    public ReplicationServiceState shutdown()
        throws TException
    {
        replicationServiceHandler.shutdown();
        ReplicationServiceState replicationServiceState = replicationServiceHandler.getReplicationServiceState();
        return replicationServiceState;
    }

    @Override
    public ReplicationServiceState startup()
        throws TException
    {
        if (!replicationServiceHandler.isSQL()) {
            replicationServiceHandler.startup();
        }
        replicationServiceHandler.startupReplication();
        ReplicationServiceState replicationServiceState = replicationServiceHandler.getReplicationServiceState();
        return replicationServiceState;
    }
    
    
    /**
     * Return content without node and using cloud access
     * @param objectID storage objectID
     * @param versionID version number or 0=current
     * @param fileID key file path
     * @return 
     */
    public File getFile(Identifier objectID, int versionID, String fileID)
        throws TException
    {
        Connection connection = null;
        try { 
            connection = replicationServiceHandler.getConnection(true);
            NodeIO nodeIO = replicationServiceHandler.getNodes();
            FileInput fileInput = FileInput.getFileInput(
                    connection, nodeIO, logger);
            return fileInput.getFile(objectID, versionID, fileID);
            
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) { }
            }
        }
    }
    
    
    /**
     * Return manifest content without node and using cloud access
     * @param objectID storage objectID
     * @param versionID version number or 0=current
     * @param fileID key file path
     * @return 
     */
    public File getManifest(Identifier objectID)
        throws TException
    {
        Connection connection = null;
        try { 
            connection = replicationServiceHandler.getConnection(true);
            NodeIO nodeIO = replicationServiceHandler.getNodes();
            FileInput fileInput = FileInput.getFileInput(
                    connection, nodeIO, logger);
            return fileInput.getManifest(objectID);
            
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) { }
            }
        }
    }

    /**
     * Normalize name of a file response file
     * @param fileResponseName non-normalized name
     * @return normalized name
     */
    public static  String getFileResponseFileName(String fileResponseName)
        throws TException
    {
        if (StringUtil.isEmpty(fileResponseName)) return "";
        fileResponseName = fileResponseName.replace('/', '=');
        fileResponseName = fileResponseName.replace('\\', '=');
        return fileResponseName;
    }
    
    protected void throwException(Exception ex)
        throws TException
    {
        if (ex instanceof TException) {
            throw (TException) ex;
        }
        throw new TException(ex);
    }

    public LoggerInf getLogger() {
        return logger;
    }

    public ReplicationServiceHandler getReplicationServiceHandler() {
        return replicationServiceHandler;
    }
    

}

