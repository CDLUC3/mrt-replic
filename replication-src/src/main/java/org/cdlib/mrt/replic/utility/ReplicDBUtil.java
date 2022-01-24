/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.replic.utility;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvStorageMaint;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;

/**
 *
 * @author loy
 */
public class ReplicDBUtil 
{
    
    protected static final String NAME = "ReplicDBUtil";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
    public static ArrayList<String> getNodeKeys(
            Identifier objectID,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        log("getKeys entered");
        
        ArrayList<String> list = new ArrayList<>();
        String sql = "SELECT n.number nnum, v.ark, v.number vnum, f.pathname "
               + "FROM inv_versions v, "
               + "inv_files f, "
               + "inv_nodes_inv_objects NO, "
               + "inv_nodes n  "
               + "WHERE v.ark='" + objectID.getValue() + "' "
               + "AND f.inv_version_id=v.id "
               + "AND f.billable_size = f.full_size "
               + "AND NO.inv_object_id=v.inv_object_id "
               + "AND n.id = NO.inv_node_id "
               + ";";
        log("sql:" + sql);
        if (DEBUG) System.out.println("sql:" + sql);
        Properties[] propArray = DBUtil.cmd(connection, sql, logger);
        if ((propArray == null)) {
            log("InvDBUtil - prop null");
            return null;
        } else if (propArray.length == 0) {
            log("InvDBUtil - length == 0");
            return null;
        }
        
        for (Properties prop : propArray) {
            String key = buildNodeKey(prop);
            if (key == null) continue;
            list.add(key);
        }
        return list;
    }
    
    public static ArrayList<InvStorageMaint> getMaintsStatus(
            InvStorageMaint.MaintStatus status,
            long node,
            int limit,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        log("getDeleteMaints entered");
        
        ArrayList<InvStorageMaint> list = new ArrayList<>();
        String sql = "SELECT sm.* "
             + "FROM inv_storage_maints sm, "
             + "inv_nodes n "
             + "WHERE sm.maint_status='" + status + "' "
             + "AND n.id=sm.inv_node_id "
             + "AND n.number=" + node + " "
             + "LIMIT " + limit + " ;" ;
        log("sql:" + sql);
        if (DEBUG) System.out.println("sql:" + sql);
        Properties[] propArray = DBUtil.cmd(connection, sql, logger);
        if ((propArray == null)) {
            log("InvDBUtil - prop null");
            return null;
        } else if (propArray.length == 0) {
            log("InvDBUtil - length == 0");
            return null;
        }
        
        for (Properties prop : propArray) {
            InvStorageMaint addMaint = new InvStorageMaint(prop, logger);
            list.add(addMaint);
        }
        return list;
    }

    public static HashMap<String,String> getHashNode(
            long nodeNum,
            Identifier objectID,
            ArrayList<String> nodeKeys,
            LoggerInf logger)
        throws TException
    {
        String found = "found";
        try {
            if (nodeKeys == null) return null;
            HashMap<String, String> map = new HashMap<>();
            nodeKeys.add("" + nodeNum + ";" + objectID.getValue() + "|manifest");
            nodeKeys.add("" + nodeNum + ";" + objectID.getValue() + "|manifest.save");
            for (String nodeKey : nodeKeys) {
                String [] parts = nodeKey.split("\\;",2);
                long keyNode = Long.parseLong(parts[0]);
                if (keyNode != nodeNum) continue;
                String key = parts[1];
                map.put(key, found);
            }
            
            if (map.size() == 0) return null;
            return map;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public static HashMap<String,String> getHashNoNode(
            Identifier objectID,
            ArrayList<String> nodeKeys,
            LoggerInf logger)
        throws TException
    {
        String found = "found";
        try {
            if (nodeKeys == null) return null;
            HashMap<String, String> map = new HashMap<>();
            nodeKeys.add("00000;" + objectID.getValue() + "|manifest");
            nodeKeys.add("00000;" + objectID.getValue() + "|manifest.save");
            for (String nodeKey : nodeKeys) {
                String [] parts = nodeKey.split("\\;",2);
                String key = parts[1];
                map.put(key, found);
            }
            if (map.size() == 0) return null;
            return map;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    

    public static HashMap<String,String> getKeyHash(
            Identifier objectID,
            long nodeNum,
            Connection connect,
            LoggerInf logger)
        throws TException
    {
        String found = "found";
        try {
            ArrayList<String> keyList = ReplicDBUtil.getNodeKeys(objectID, connect,logger);
            if (keyList == null) {
                if (DEBUG) System.out.println("empty list");
                return null;
            }
            return  ReplicDBUtil.getHashNode(nodeNum, objectID, keyList, logger);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected static String buildNodeKey(Properties prop)
    {
        String node = prop.getProperty("nnum");
        String ark = prop.getProperty("ark");
        String version  = prop.getProperty("vnum");
        String pathname  = prop.getProperty("pathname");
        
        if ((ark != null) && (version != null) && (pathname != null)) {
            return node + ";" + ark + "|" + version + "|" + pathname;
        }
        return null;
    }
    
    public static Long[] getVersionSize(
            Long objectseq,
            Long lastVersion,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        log("getKeys entered");
        
        ArrayList<String> list = new ArrayList<>();
        String sql = "SELECT SUM(f.billable_size) bsize, MAX(v.number) maxver "
               + "FROM inv_versions v, "
               + "inv_files f "
               + "WHERE v.id = f.inv_version_id "
               + "AND v.inv_object_id = " + objectseq + " "
               + "AND v.number > " + lastVersion + " "
               + ";"; 
        
        log("sql:" + sql);
        if (DEBUG) System.out.println("sql:" + sql);
        Properties[] propArray = DBUtil.cmd(connection, sql, logger);
        if ((propArray == null)) {
            log("InvDBUtil - prop null");
            return null;
        } else if (propArray.length == 0) {
            log("InvDBUtil - length == 0");
            return null;
        }
        
        Long[] ret = new Long[2];
        String sizeS = propArray[0].getProperty("bsize");
        if (sizeS == null) return null;
        ret[0] = Long.parseLong(sizeS);
        
        String maxverS = propArray[0].getProperty("maxver");
        if (maxverS == null) return null;
        ret[1] = Long.parseLong(maxverS);
        return ret;
    }
    
    protected static void log(String msg)
    {
        if (!DEBUG) return;
        System.out.println(MESSAGE + msg);
    }
}
