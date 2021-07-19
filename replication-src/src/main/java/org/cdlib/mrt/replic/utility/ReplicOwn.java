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

package org.cdlib.mrt.replic.utility;

import org.cdlib.mrt.db.DBUtil;
import java.util.LinkedList;
import java.util.Properties;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import java.sql.Connection;


import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.utility.InvUtil;

/**
 * This class performs the overall fixity functions.
 * Fixity runs as a background thread.
 * 
 * Fixity uses a relational database (here MySQL) to process the oldest entries 
 * first. These entries are pulled in as blocks. Each block is then processed and
 * the results are then collected before the next block is started.
 * 
 * The fixity tests whether either the extracted file has changed either size or digest.
 * Any change results in error information being saved to the db entry for that test.
 * 
 * Note that FixityState contains 2 flags used for controlling fixity handling:
 * 
 *  runFixity - this flag controls whether to start or stop fixity
 *            - true=fixity should be running or starting to run
 *            - false=stop fixity and exit routine
 *  fixityProcessing - this flag determines if fixity is running
 *            - true=fixity is now running
 *            - false=fixity has stopped
 * @author dloy
 */

public class ReplicOwn 
{
    private static final String NAME = "ReplicOwn";
    private static final String MESSAGE = NAME + ": ";

    private static final boolean DEBUG = false;
    
       
    public static LinkedList<InvNodeObject> getOwnListReplic(
            Connection ownConnect, 
            String replicQualify,
            int capacity,
            LoggerInf logger)
        throws TException
    {        
        LinkedList<InvNodeObject> replicList = new LinkedList<InvNodeObject>();
        try {
            
            ownConnect.setAutoCommit(false);
            if (replicQualify == null) {
                replicQualify = "";
            }
            replicList = new LinkedList<InvNodeObject>();
            String runSQL = "select distinct inv_nodes_inv_objects.* "
                    + "from " + InvNodeObject.NODES_OBJECTS + "," 
                            + InvNodeObject.COLLECTION_NODES + "," 
                            + InvNodeObject.COLLECTIONS_OBJECTS + " "
                    + "where inv_collections_inv_objects.inv_collection_id = inv_collections_inv_nodes.inv_collection_id "
                    + "and inv_nodes_inv_objects.inv_object_id = inv_collections_inv_objects.inv_object_id "
                    + "and inv_nodes_inv_objects.role = 'primary' "
                    + "and inv_nodes_inv_objects.replicated is null "
                    + " " + replicQualify + " "
                    + "limit " + capacity + " "
                    + "for update "
                    + ";";
    

            Properties [] props = DBUtil.cmd(ownConnect, runSQL, logger);
            if ((props == null) || (props.length==0)) {
                //System.out.println("getOwnListReplic select list null");
                return replicList;
            }
            System.out.println("getOwnListReplic select size=" + props.length);
            DateState zeroDate = new DateState(28800000);
            String initialDate = InvUtil.getDBDate(zeroDate);
            String concatid = "";
            for (Properties prop : props) {
                InvNodeObject replic = new InvNodeObject(prop, logger);
                replic.setReplicated(zeroDate);
                if (concatid.length() > 0 ) {
                    concatid = concatid + ",";
                }
                concatid = concatid + replic.getId();
                replicList.add(replic);
            }
            
        
            String updateSql = "update inv_nodes_inv_objects "
                + "set replicated = '" + initialDate + "' "
                + "where id in (" + concatid + ") "
                + "and replicated is null;";
        
            int updates = DBUtil.update(ownConnect, updateSql, logger);
            
            //System.out.println("getOwnListReplic update size=" + updates);
            ownConnect.commit();
            String msg = MESSAGE + "getOwnListReplic:"
                    + " - updates=" + updates
                    + " - capacity=" + capacity 
                    + " - replicQualify='" + replicQualify +"'"
                    ;
            logger.logMessage(msg, 5, true);
            return replicList;
            
        } catch (Exception ex) {
            try {
                ownConnect.rollback();
                System.out.println("Exception rollback:" + ex);
                
            } catch (Exception exr) {
                System.out.println("WARNING rollback fails:" + ex);
            }
            return new LinkedList<InvNodeObject>();

        }
    }
}
