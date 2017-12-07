
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.Properties;


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
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class DeletorTest
{
    private static final String NAME = "DeletorTest";
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
            long objectseq = 6195;
            long nodeseq = 21;
            long nodeObjectSeq = 13116;
            InvNodeObject invNodeObject = InvDBUtil.getNodeObject(nodeseq, objectseq, connect, logger);
            
            System.out.println(PropertiesUtil.dumpProperties("DELETE IN", invNodeObject.retrieveProp()));
            NodeIO nodeIO = new NodeIO("nodes-dev-glacier", logger);
            nodeIO.printNodes("main dump");
            ReplicNodesObjects replicNodesObject = new ReplicNodesObjects(invNodeObject, 6001, logger);
            Deletor deletor = Deletor.getDeletor(replicNodesObject, false, connect, nodeIO, logger);
            deletor.run();
            Exception se = deletor.getSaveException();
            if (deletor.getSaveException() != null) {
                System.out.println("DeletorTest exception:" + se);
                se.printStackTrace();
                return;
            }
            ReplicationDeleteState returnState = deletor.getReturnState();

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

