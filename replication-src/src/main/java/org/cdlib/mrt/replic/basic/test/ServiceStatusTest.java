/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.io.File;
import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.Replicator;
import org.cdlib.mrt.replic.basic.service.RunReplication;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.replic.basic.service.ReplicationService;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceHandler;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceState;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceStateManager;
import static org.cdlib.mrt.replic.basic.service.ReplicationServiceStateManager.formatIt;

/**
 * Load manifest.
 * @author  dloy
 */

public class ServiceStatusTest
{
    private static final String NAME = "ServiceStateTest";
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
            tFrame = new TFrame(propertyList, "ReplicLoad");

            // Create an instance of this object
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            Properties props = tFrame.getAllProperties();
            System.out.println(PropertiesUtil.dumpProperties("RUN", props));
            ReplicationServiceHandler serviceHandler = ReplicationServiceHandler.getReplicationServiceHandler(props);
            ReplicationService service = ReplicationService.getReplicationService(serviceHandler);
            
            ReplicationServiceState serviceState = service.getReplicationServiceStatus();
            
            FormatterInf anvl = FormatterAbs.getXMLFormatter(logger);
            String format = ReplicationServiceStateManager.formatIt(anvl, serviceState);
            System.out.println("OUTPUT:\n" + format);

        } catch(Exception e) {
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            if (db != null) {
                try {
                    db.shutDown();
                } catch (Exception ex) { }
            }
        }
    }

}


