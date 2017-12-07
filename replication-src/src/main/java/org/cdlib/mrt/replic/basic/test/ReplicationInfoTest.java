
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.Properties;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.ReplicationInfo;

/**
 * Load manifest.
 * @author  dloy
 */

public class ReplicationInfoTest
{
    private static final String NAME = "ReplicationInfoTest";
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
            Identifier objectID = new Identifier("ark:/13030/c8222v72");
            long objectseq = 13400; //6195;
            InvNodeObject[] invNodeObjects = InvDBUtil.getNodeObjects(objectseq, "primary", connect, logger);
            int item=0;
            for (InvNodeObject invNodeObject : invNodeObjects) {
                System.out.println("ITEM(" + item + "):" + PropertiesUtil.dumpProperties("IN", invNodeObject.retrieveProp()));
                item++;
            }
            ReplicationInfo ri = ReplicationInfo.getReplicationInfo(invNodeObjects[0], connect, logger);
            ri.buildInfo();
            ri.dump("TEST");

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

