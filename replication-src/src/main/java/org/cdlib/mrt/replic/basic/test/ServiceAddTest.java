
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.basic.service.NodeState;
import org.cdlib.mrt.replic.basic.service.ReplicationAddState;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.basic.service.ReplicationService;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceHandler;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceState;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceStateManager;

/**
 * Load manifest.
 * @author  dloy
 */

public class ServiceAddTest
{
    private static final String NAME = "ServiceAddTest";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;

    /**
     * Main method
     */
    
    
    public static void main(String args[])
        throws TException
    {


        Identifier objectID = new Identifier("ark:/b5072/fk2668bm6c");
        ReplicationServiceHandler serviceHandler = null;
        try {
            ReplicationConfig replicConfig = ReplicationConfig.useYaml();
            LoggerInf logger = replicConfig.getLogger();
            serviceHandler = ReplicationServiceHandler.getReplicationServiceHandler(replicConfig);
            ReplicationService service = ReplicationService.getReplicationService(serviceHandler);
            ReplicationAddState addState = service.add(objectID);
            
            FormatterInf xml = FormatterAbs.getXMLFormatter(logger);
            String format = ReplicationServiceStateManager.formatIt(xml, addState);
            System.out.println("OUTPUT:\n" + format);

        } catch(Exception e) {
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            if (serviceHandler != null) {
                try {
                    serviceHandler.shutdown();
                } catch (Exception ex) { }
            }
        }
    }

}

