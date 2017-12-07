
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestSAX;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileContent;
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
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.utility.ReplicUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Load manifest.
 * @author  dloy
 */

public class BuildMapInvTest
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

        TFrame tFrame = null;
        DPRFileDB db = null;
        try {
            String propertyList[] = {
                "resources/ReplicLogger.properties",
                "resources/ReplicTest.properties",
                "resources/Replic.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            //LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            LoggerInf logger = new TFileLogger("testFormatter", 1, 1);
            db = new DPRFileDB(logger, invProp);
            Connection connect = db.getConnection(true);
            //Identifier objectID = new Identifier("ark:/99999/fk4dr3cp1");
            //Identifier objectID = new Identifier("ark:/13030/sss001");
            //Identifier objectID = new Identifier("ark:/99999/fk4dr3bzf");
            Identifier objectID = new Identifier("ark:/99999/fk4rx9wmm");
            BuildMapInv buildMap = BuildMapInv.getBuildMapInv(objectID, connect, logger);
            VersionMap map = buildMap.process();
            String mapxml = ReplicUtil.versionMap2String(map);
            System.out.println(mapxml);
            
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

