
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
import org.cdlib.mrt.replic.basic.action.RestoreObject;
import org.cdlib.mrt.replic.basic.content.CopyNodes;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.NodeService;

/**
 * Load manifest.
 * @author  dloy
 */

public class RestoreObjectTest
{
    private static final String NAME = "RestoreObjectTest";
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
                "resources/RestoreObjectTest.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            db = new DPRFileDB(logger, invProp);
            Connection connect = db.getConnection(true);
            String nodeName = "nodes-dev";
            long node = 6001;
            Identifier objectID = new Identifier("ark:/b5072/fk2cn7513m");
            NodeService service = NodeService.getNodeService(nodeName, node, logger);
            RestoreObject restoreObject = RestoreObject.getRestore(service,
                objectID,
                connect,
                logger);
            boolean objectComplete = restoreObject.process();
            System.out.println("***objectComplete:" + objectComplete + "\n"
                    + " - nodeName:" + nodeName + "\n"
                    + " - node:" + node + "\n"
                    + " - objectID:" + objectID.getValue() + "\n"
                    );

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

