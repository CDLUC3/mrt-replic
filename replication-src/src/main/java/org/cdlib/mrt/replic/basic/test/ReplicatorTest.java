
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
import org.cdlib.mrt.replic.basic.action.Replicator;
import org.cdlib.mrt.replic.basic.content.CopyNodes;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class ReplicatorTest
{
    private static final String NAME = "ReplicatorTest";
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
                "resources/ReplicatorTest.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            db = new DPRFileDB(logger, invProp);
            Connection connect = db.getConnection(true);
            Identifier objectID = new Identifier("ark:/99999/fk49k4w12");
            long objectseq = 2426;
            InvNodeObject[] invNodeObjects = InvDBUtil.getNodeObjects(objectseq, "primary", connect, logger);
            InvNodeObject nodeObject = invNodeObjects[0];
            
            System.out.println(PropertiesUtil.dumpProperties("IN", invNodeObjects[0].retrieveProp()));
            
            String nodeName = invProp.getProperty("nodeName)");
            NodeIO nodes = new NodeIO(nodeName, logger);
            Replicator replicator = Replicator.getReplicator(nodeObject, nodes, db, logger);
            replicator.run();
            replicator.dumpInfo("TEST");

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

