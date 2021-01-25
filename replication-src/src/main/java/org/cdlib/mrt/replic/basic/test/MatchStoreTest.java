
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.Properties;


import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.basic.action.MatchStore;
import static org.cdlib.mrt.replic.basic.action.ReplicActionAbs.getVersionMap;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class MatchStoreTest
{
    private static final String NAME = "MatchStoreTest";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;

    /**
     * Main method
     */
    public static void main(String args[])
    {

        try {
            ReplicationConfig replicationConfig = ReplicationConfig.useYaml();
            LoggerInf logger = replicationConfig.getLogger();
            //Identifier objectID = new Identifier("ark:/99999/fk4dr3cp1");
            Identifier objectID = new Identifier("ark:/b5072/fk2668bm6c");
            int sourceNode=9502;
            int targetNode=2002;
            
            NodeIO nodeIO = replicationConfig.getNodeIO();
            VersionMap sourceMap = getVersionMap(nodeIO, sourceNode, objectID, logger);
            VersionMap targetMap = getVersionMap(nodeIO, targetNode, objectID, logger);
            
            MatchStore matchStore = MatchStore.getMatchStore( logger);
            MatchResults results = matchStore.process(sourceMap, targetMap);
            String out = results.dump("MatchStoreTest");
            System.out.println(out);
          
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } 
    }

}

