
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.sql.Connection;
import java.util.Properties;
import org.cdlib.mrt.core.DateState;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.inv.utility.InvUtil;
import org.cdlib.mrt.replic.basic.action.Deletor;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.s3.service.NodeIO;

/**
 * Load manifest.
 * @author  dloy
 */

public class ReplicatorSetTest
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
                "resources/Mysql.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            System.out.println(PropertiesUtil.dumpProperties("ReplicatorSetTest", invProp));
            LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            db = new DPRFileDB(logger, invProp);
            Connection connect = db.getConnection(true);
            ReplicatorSetTest rst = new ReplicatorSetTest();
            long id = 4870;
            InvNodeObject nodeObj = new InvNodeObject(logger);
            nodeObj.setId(id);
            Boolean test = rst.resetReplicated(nodeObj, connect, logger);
            System.out.println("test:" + test);

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

    
    public boolean entryFree(long id, Connection connection, LoggerInf logger)
    {
        Properties [] rows = null;
        
        String sql = "SELECT id,replicated "
            + "FROM inv_nodes_inv_objects "
            + "WHERE id=" + id;
        
        try {
            Properties[] propArray = DBUtil.cmd(connection, sql, logger);
        
            if ((propArray == null)) {
                System.out.println("propArray null");
                return false;

            } 
            System.out.println(PropertiesUtil.dumpProperties("ReplicatorSetTest", propArray[0]));
            if (propArray[0].getProperty("id") == null) {
                return false;
            } 
            if (propArray[0].getProperty("replicated") == null) {
                return true;
            }
            return false;

        } catch (Exception ex) {
            return false;
        }
    }
    

    public boolean resetReplicated(InvNodeObject nodeObj, Connection connection, LoggerInf logger)
    {
        DateState zeroDate = new DateState(28800000);
        long id = nodeObj.getId();
        String initialDate = InvUtil.getDBDate(zeroDate);
        System.out.println("initialDate:" + initialDate);
        nodeObj.setReplicated(zeroDate);
        
        String sql = "update inv_nodes_inv_objects "
            + "set replicated = '" + initialDate + "' "
            + "where id=" + id + " "
            + "and replicated is null ";
        
        try {
            System.out.println("sql:" + sql);
            int updates = DBUtil.update(connection, sql, logger);
            System.out.println("***updates:" + updates);
        
            if (updates == 1) {
                System.out.println("update worked--" + nodeObj.dump("ADD"));
                return true;

            } 
            return false;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            return false;
        }
    }
    public boolean resetReplicated(long id, Connection connection, LoggerInf logger)
    {
        Properties [] rows = null;
        
        String sql = "update inv_nodes_inv_objects "
            + "set replicated = '1970-01-01 00:00:00' "
            + "where id=" + id + " "
            + "and replicated is null ";
        
        try {
            int updates = DBUtil.update(connection, sql, logger);
            System.out.println("***updates:" + updates);
        
            if (updates == 1) {
                System.out.println("update worked");
                return true;

            } 
            return false;

        } catch (Exception ex) {
            return false;
        }
    }
}

