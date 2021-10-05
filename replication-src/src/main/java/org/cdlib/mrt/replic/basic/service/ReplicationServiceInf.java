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
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.inv.content.InvStorageMaint;
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;

/**
 * Replication Service Interface
 * @author  dloy
 */

public interface ReplicationServiceInf
{
    /**
     * Add a specific replication only if requested object qualifies based
     * on inv_collections_inv_nodes entry
     * @param objectID object identifier for add
     * @return Replication Add State
     * @throws TException 
     */
    public ReplicationAddState add(
            Identifier objectID)
        throws TException;
    
    /**
     * Add only inventory part of replication
     * on inv_collections_inv_nodes entry
     * NOTE: storage archive is not validated for content
     * @param objectID object identifier for add inventory
     * @return Replication Add State
     * @throws TException 
     */
    public ReplicationAddState addReplicInv(
            Identifier objectID)
        throws TException;
            
    /**
     * Replace a specific replication in background thread
     * @param nodeNum number of node to be deleted in inventory
     * @param objectID object identifier for add
     * @return Replication Add State
     * @throws TException 
     */
    public ReplicationServiceState replaceBackground(
            int nodeNum,
            Identifier objectID)
        throws TException;
    
     
    /**
     * Add inv_collections_inv_nodes entry to support replication
     * @param collectionID primary collection used to replicate
     * @param nodeNum target node number
     * @param collectionNode create
     * @return results of operation
     */
    public ReplicationAddMapState addMap(
            Identifier collectionID,
            int nodeNum)
        throws TException;
            
    /*
    /**
     * Delete object from storage and inv
     * @param objectID storage objectID
     * @return number of deleted rows
     * @throws TException 
     */
    public ReplicationDeleteState delete(
            boolean deleteInvWhenStoreMissing,
            int nodeNum,
            Identifier objectID)
        throws TException;
    
    /**
     * Delete replicated content
     * @param objectID storage objectID
     * @return state of all deleted secondary 
     * @throws TException 
     */
    public ReplicationDeleteState deleteSecondary(
            Identifier objectID)
        throws TException;
            
    
    public ReplicationPropertiesState doCleanup()
        throws TException;
    
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
        throws TException;
    
    /**
     * Scan this node for orphaned content
     * @param nodeNumber node to be scanned
     * @param keyList node:S3 key to cloud content OR null if next is used
     * @return working scan status
     * @throws TException 
     */
    public InvStorageScan scanStart(
            Long nodeNumber,
            String keyList)
        throws TException;
    
    /**
     * Restart scan on this previous run
     * @param scanID inv_storage_scan.id to be restarted
     * @return restarted scan
     * @throws TException 
     */
    public InvStorageScan scanRestart(
            Integer scanID)
        throws TException;
            
    /**
     * Delete object from inv only
     * @param objectID storage objectID
     * @return number of deleted rows
     * @throws TException 
     */
    public ReplicationDeleteState deleteInv(
            int nodeNum,
            Identifier objectID)
        throws TException;
    
    /**
     * Return content without node and using cloud access
     * @param objectID storage objectID
     * @param versionID version number or 0=current
     * @param fileID key file path
     * @return 
     */
    public File getFile(Identifier objectID, int versionID, String fileID)
        throws TException;
    
    /**
     * Return manifest content without node and using cloud access
     * @param objectID storage objectID
     * @return 
     */
    public File getManifest(Identifier objectID)
        throws TException;
    
    /**
     * @return pointer to merritt logger
     */
    public LoggerInf getLogger();
    
    /**
     * @return state including db stats
     * @throws TException 
     */
    public ReplicationServiceState getReplicationServiceState()
        throws TException;
    
    /**
     * @return state no db stats
     * @throws TException 
     */
    public ReplicationServiceState getReplicationServiceStatus()
        throws TException;
    
    /**
     * Pause services - stope replication looping
     * @return service state
     * @throws TException 
     */
    public ReplicationServiceState pause()
        throws TException;
    
    /**
     * Shutdown services
     * @return service state
     * @throws TException 
     */
    public ReplicationServiceState shutdown()
        throws TException;
    
    /**
     * Allow or Stop scanning
     * @param allow true=continue to let run; false=stop
     * @return service state
     * @throws TException 
     */
    public ReplicationServiceState allowScan(Boolean allow)
        throws TException;
    
    /**
     * Delete item in Inv_storage_maints table
     * @param storageMaintId id in inv_storage_maints
     * @return maints db entry
     * @throws TException 
     */
    public InvStorageMaint scanDelete(
            long storageMaintId)
        throws TException;
    
    /**
     * Startup services
     * @return service state
     * @throws TException 
     */
    public ReplicationServiceState startup()
        throws TException;
}
