
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.Properties;
import org.cdlib.mrt.cloud.VersionMap;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.ANVLFormatter;
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
import org.cdlib.mrt.replic.basic.action.Match;
import org.cdlib.mrt.replic.basic.action.MatchStore;
import static org.cdlib.mrt.replic.basic.action.ReplicActionAbs.getVersionMap;
import org.cdlib.mrt.replic.basic.service.MatchObjectState;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.utility.ReplicUtil;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class MatchTest
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
            Connection connection = db.getConnection(true);
            //Identifier objectID = new Identifier("ark:/99999/fk4dr3cp1");
            //Identifier objectID = new Identifier("ark:/13030/sss001");
            Identifier objectID = new Identifier("ark:/28722/k2qb9v531");
            String nodeName = "nodes-stg";
            int sourceNode=2111;
            int targetNode=5001;
            //int sourceNode=8001;
            //int targetNode=910;
            NodeIO nodeIO = NodeIO.getNodeIO(nodeName, logger);
            Match match = Match.getMatch(objectID, nodeIO, sourceNode,targetNode, connection, logger);
            
            MatchObjectState state = match.process();
            
            String results = state.dump("MatchTest");
            System.out.println("RESULTS:\n" + results);
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(baos, true, "utf-8");
            //XMLFormatter formatter = XMLFormatter.getXMLFormatter(logger, outFileStream);
            ANVLFormatter formatter = ANVLFormatter.getANVLFormatter(logger);
            formatter.format(state, printStream);
            System.out.println("ANVL:\n" + baos.toString());
            
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

