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
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.inv.content.InvNodeObject;
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

    public static ArrayList<String> getKeys(
            Identifier objectID,
            long nodeNumber,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        log("getKeys entered");
        
        ArrayList<String> list = new ArrayList<>();
        String sql = "SELECT v.ark, v.number, f.pathname "
               + "FROM inv_versions v, "
               + "inv_files f, "
               + "inv_nodes_inv_objects NO, "
               + "inv_nodes n  "
               + "WHERE v.ark='" + objectID.getValue() + "' "
               + "AND n.number=" + nodeNumber + " "
               + "AND f.inv_version_id=v.id "
               + "AND f.billable_size > 0 "
               + "AND NO.inv_object_id=v.inv_object_id "
               + "AND n.id = NO.inv_node_id "
               + ";";
        log("sql:" + sql);
        Properties[] propArray = DBUtil.cmd(connection, sql, logger);
        if ((propArray == null)) {
            log("InvDBUtil - prop null");
            return null;
        } else if (propArray.length == 0) {
            log("InvDBUtil - length == 0");
            return null;
        }
        
        for (Properties prop : propArray) {
            String key = buildKey(prop);
            if (key == null) continue;
            list.add(key);
        }
        return list;
    }

    public static HashMap<String,String> getKeyHash(
            Identifier objectID,
            long nodeNumber,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        try {
            HashMap<String, String> map = new HashMap<>();
            ArrayList<String> keyList = getKeys(objectID, nodeNumber, connection, logger);
            String found = "found";
            if (keyList == null) return null;
            keyList.add(objectID.getValue() + "|manifest");
            keyList.add(objectID.getValue() + "|manifest.save");
            for (String key: keyList) {
                map.put(key, found);
            }
            return map;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected static String buildKey(Properties prop)
    {
        String ark = prop.getProperty("ark");
        String version  = prop.getProperty("number");
        String pathname  = prop.getProperty("pathname");
        
        if ((ark != null) && (version != null) && (pathname != null)) {
            return ark + "|" + version + "|" + pathname;
        }
        return null;
    }
    
    
    protected static void log(String msg)
    {
        if (!DEBUG) return;
        System.out.println(MESSAGE + msg);
    }
}
