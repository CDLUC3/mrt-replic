
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
import org.cdlib.mrt.replic.basic.action.ReplicCleanup;
import org.cdlib.mrt.replic.basic.service.ReplicationPropertiesState;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class ReplicCleanupTestArk
{
    private static final String NAME = "ReplicCleanupTest";
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
                "resources/ReplicCleanupTest.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            //LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            LoggerInf logger = new TFileLogger("testFormatter", 1, 1);
            db = new DPRFileDB(logger, invProp);
            System.out.println(PropertiesUtil.dumpProperties("Cleanup props", invProp));
            NodeIO nodeIO = new NodeIO("nodes-dev-glacier", logger);
            String[] arks = {
                "ark:/99999/fk4rx9wmm",
                "ark:/99999/fk4rx9wn3",
                "ark:/99999/fk4rx9wpk",
                "ark:/99999/fk4rx9wq2",
                "ark:/99999/fk4rx9wrj"
            };
            ReplicCleanup rc = ReplicCleanup.getReplicCleanup(arks, nodeIO, db, logger);
            ReplicationPropertiesState rps = rc.call();
            if (rps != null) {
                rps.dump("ReplicCleanupTest");
            }
            
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

