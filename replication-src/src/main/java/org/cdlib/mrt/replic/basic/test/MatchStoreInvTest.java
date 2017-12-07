
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
import org.cdlib.mrt.replic.basic.action.BuildMapInv;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.basic.action.MatchStore;
import static org.cdlib.mrt.replic.basic.action.ReplicActionAbs.getVersionMap;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.utility.ReplicUtil;

/**
 * Load manifest.
 * @author  dloy
 */

public class MatchStoreInvTest
{
    private static final String NAME = "MatchStoreInvTest";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;

    /**
     * Main method
     */
    public static void main(String args[])
    {

        TFrame tFrame = null;
        DPRFileDB db = null;
        try {
            String propertyList[] = {
                "resources/ReplicLogger.properties",
                "resources/ReplicTest.properties",
                "resources/Replic.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            db = new DPRFileDB(logger, invProp);
            Connection connect = db.getConnection(true);
            //Identifier objectID = new Identifier("ark:/99999/fk4dr3cp1");
            //Identifier objectID = new Identifier("ark:/13030/sss001");
            Identifier objectID = new Identifier("ark:/90135/q15h7d7h");
            int sourceNode=910;
            int targetNode=8001;
            //int sourceNode=8001;
            //int targetNode=910;
            String storageBase = "http://uc3-mrt-store2-dev.cdlib.org:35121";
            
            VersionMap sourceMap = getVersionMap(storageBase, sourceNode, objectID, logger);
            VersionMap targetMap = getVersionMap(storageBase, targetNode, objectID, logger);
            
            MatchStore matchStore = MatchStore.getMatchStore( logger);
            MatchResults results = matchStore.process(sourceMap, targetMap);
            String out = results.dump("MatchStoreTest");
            System.out.println(out);
            
            BuildMapInv buildMap = BuildMapInv.getBuildMapInv(objectID, connect, logger);
            VersionMap invMap = buildMap.process();
            String mapxml = ReplicUtil.versionMap2String(invMap);
            
            MatchStore matchInvStore = MatchStore.getMatchStore( logger);
            MatchResults resultsInv = matchStore.process(sourceMap, invMap);
            if (resultsInv.getObjectMatch()) {
                resultsInv.setObjectFoundInv(true);
            }
            String outInv = resultsInv.dump("MatchStoreInvTest");
            System.out.println(outInv);
            
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            try {
                db.shutDown();
            } catch (Exception ex) {
                System.out.println("db Exception:" + ex);
            }
        }
    }

}

