
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.basic.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestSAX;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileContent;
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
import org.cdlib.mrt.replic.basic.action.MatchStore;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.basic.service.ReplicationDeleteState;
import org.cdlib.mrt.replic.utility.ReplicDBUtil;
import org.cdlib.mrt.replic.utility.ReplicUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Load manifest.
 * @author  dloy
 */

public class KeyTest
{
    private static final String NAME = "KeyTest";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;

    /**
     * Main method
     */
    public static void main(String args[])
    {

        
        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
        DPRFileDB db = null;
        try {
            //Identifier objectID = new Identifier("ark:/28722/bk0006w8m0c");
            Identifier objectID = new Identifier("ark:/13030/hb0w1005w7");
            ReplicationConfig config =  ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connect = db.getConnection(true);
            ArrayList<String> keyList = ReplicDBUtil.getNodeKeys(objectID, connect,logger);
            if (keyList == null) {
                System.out.println("empty list");
                return;
            }
            int cnt = 0;
            for (String key : keyList) {
                System.out.println("key=" + key);
                cnt++;
            }
            System.out.println("******");
            System.out.println("count=" + cnt);
            System.out.println("******");
            HashMap<String,String> hash = ReplicDBUtil.getHashNode(nodeNumber, objectID, keyList, logger);
            Set<String> keys = hash.keySet();
            for (String key : keys) {
                System.out.println("HashNode key:" + key);
            }
            System.out.println("hash:" + hash.size());
            hash = ReplicDBUtil.getHashNoNode(objectID, keyList, logger);
            keys = hash.keySet();
            for (String key : keys) {
                System.out.println("HashNoNode key:" + key);
            }
            System.out.println("hashNoNode:" + hash.size());
            
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

