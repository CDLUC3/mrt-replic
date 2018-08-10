
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Properties;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.replic.basic.action.ReplicCleanup;
import org.cdlib.mrt.replic.basic.service.ReplicationPropertiesState;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.StateInf;

/**
 * Load manifest.
 * @author  dloy
 */

public class ReplicCleanupTest
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
            ReplicCleanup rc = ReplicCleanup.getReplicCleanup((Properties)null, nodeIO, db, logger);
            ReplicationPropertiesState rps = rc.call();
            if (rps != null) {
                rps.dump("ReplicCleanupTest");
            }
            
            String report = formatIt(logger, rps);
            System.out.println("report\n" + report);
            
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
    


    public static String formatIt(
            LoggerInf logger,
            StateInf responseState)
    {
        try {
           //FormatterInf anvl = FormatterAbs.getJSONFormatter(logger);
           FormatterInf anvl = FormatterAbs.getXMLFormatter(logger);
           ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
           PrintStream  stream = new PrintStream(outStream, true, "utf-8");
           anvl.format(responseState, stream);
           stream.close();
           outStream.close();
           byte [] bytes = outStream.toByteArray();
           String retString = new String(bytes, "UTF-8");
           return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        } finally {

        }
    }
}

