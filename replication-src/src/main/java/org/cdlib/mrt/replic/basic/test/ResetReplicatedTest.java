
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
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.utility.ReplicDB;

/**
 * Load manifest.
 * @author  dloy
 */

public class ResetReplicatedTest
{
    private static final String NAME = "ResetReplicatedTest";
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
                "resources/Replic.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            db = new DPRFileDB(logger, invProp);
            Connection connect = db.getConnection(true);
            long objectseq = 598;
            long nodeseq = 7;
            InvNodeObject invNodeObject = get("start", connect, logger, nodeseq, objectseq);
            
            System.out.println(PropertiesUtil.dumpProperties(MESSAGE, invNodeObject.retrieveProp()));
            ReplicDB.resetReplicatedNull(connect, invNodeObject, logger);
            InvNodeObject noAfter = get("null", connect, logger, nodeseq, objectseq);
            
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
    
    public static InvNodeObject get(String header, 
            Connection connect,
            LoggerInf logger,
            long nodeseq, long objectseq )
        throws TException
    {
        try {
            InvNodeObject invNodeObject = InvDBUtil.getNodeObject(nodeseq, objectseq, connect, logger);
            
            System.out.println(PropertiesUtil.dumpProperties(header, invNodeObject.retrieveProp()));
            return invNodeObject;
            
        } catch(Exception e) {
                e.printStackTrace();
                throw new TException(e);
        }
    }

}

