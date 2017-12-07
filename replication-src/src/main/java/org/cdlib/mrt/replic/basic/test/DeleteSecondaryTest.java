
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.Properties;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.DeleteSecondary;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceStateManager;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class DeleteSecondaryTest
{
    private static final String NAME = "DeleteSecondaryTest";
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
            Identifier objectID = new Identifier("ark:/99999/fk4cv4s85d");
            NodeIO nodeIO = new NodeIO("nodes-dev-glacier", logger);
            nodeIO.printNodes("main dump");
            DeleteSecondary deleteSecondary = DeleteSecondary.getDeleteSecondary(
                    objectID,
                    db,
                    nodeIO,
                    logger);
            ReplicationDeleteState response = deleteSecondary.process();
            
            FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
            String format = ReplicationServiceStateManager.formatIt(xml, response);
            System.out.println("OUTPUT:\n" + format);

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

