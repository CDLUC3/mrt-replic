
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.Properties;


import org.cdlib.mrt.core.DateState;
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

public class ReplicatedDates
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
            DateState zeroDate = new DateState(28800000);
            System.out.println("zeroDate:" + zeroDate.getIsoDate());
            DateState yearDate = new DateState(31507200000L);
            System.out.println("yearDate:" + yearDate.getIsoDate());
            
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {}
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

