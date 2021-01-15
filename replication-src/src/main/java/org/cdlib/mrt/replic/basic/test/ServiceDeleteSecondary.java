/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.replic.basic.service.NodesObjectsState;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.basic.service.ReplicationService;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceHandler;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceState;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceStateManager;

/**
 * Load manifest.
 * @author  dloy
 */

public class ServiceDeleteSecondary
{
    private static final String NAME = "ServiceDeleteSecondary";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;

    /**
     * Main method
     */
 
    
    public static void main(String args[])
    {

        ReplicationServiceHandler serviceHandler = null;
        try {
            ReplicationConfig replicConfig = ReplicationConfig.useYaml();
            LoggerInf logger = replicConfig.getLogger();
            serviceHandler = ReplicationServiceHandler.getReplicationServiceHandler(replicConfig);
            ReplicationService service = ReplicationService.getReplicationService(serviceHandler);
            
            ReplicationServiceState serviceState = service.pause();
            print("START", serviceState, logger);
            Thread.sleep(30000);
            
            //Identifier objectID = new Identifier("ark:/99999/fk48342g2z");
            //Identifier objectID = new Identifier("ark:/99999/fk4g44z76r");
            //Identifier objectID = new Identifier("ark:/99999/fk46q27106");
            Identifier objectID = new Identifier("ark:/b5072/fk2668bm6c");
            
            ReplicationDeleteState deleteState = service.deleteSecondary(objectID);
            print("Delete", deleteState, logger);
            
            serviceState = service.shutdown();
            print("SHUTDOWN", serviceState, logger);

        } catch(Exception e) {
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } 
    }
    
    public static void print(String header, StateInf response, LoggerInf logger)
        throws TException
    {
        FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
        String format = ReplicationServiceStateManager.formatIt(xml, response);
        System.out.println("***" + header + "***\n" + "\nOUTPUT:\n" + format);
    }

}


