/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/
package org.cdlib.mrt.replic.basic.action;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.inv.content.InvFile;
import org.cdlib.mrt.inv.content.InvStorageMaint;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.replic.utility.ReplicDBUtil;
        ;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Run fixity
 * @author dloy
 */
public class DoScan
        extends ReplicActionAbs
{

    protected static final String NAME = "DoScan";
    protected static final String MESSAGE = NAME + ": ";

    
    protected CloudStoreInf service = null;
    protected String bucket = null;
    protected ArrayList<CloudList.CloudEntry> entryList = null;
    protected ArrayList<CloudList.CloudEntry> failList = new ArrayList<>();
    protected String lastKey = null;
    protected Long inNode = null;
    protected Long nodeid = null;
    protected String dbArk = null;
    protected HashMap<String,String> dbHash = null;
    protected DBAdd dbAdd = null;
    protected ScanInfo scanInfo = null;
    public static void main(String args[])
    {

        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("DoScan", 20, 20);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            DoScan doScan = DoScan.getScan(nodeNumber, connection, logger);
            //String afterKey = "ark:/28722/k2xw47w3g|1|system/mrt-mom.txt";
            String afterKey = "ark:/99999/fk40303r7f|1|producer/nuxeo.cdlib.org/Merritt/2ced9aed-9724-44ff-8d74-52a0f9dad298.xml";
            int maxKeys=15000;
            DoScan.ScanInfo info = doScan.process(afterKey, maxKeys);
            System.out.println(info.dump("Count Dump 1"));
            afterKey = info.getLastProcessKey();
            info = doScan.process(
            );
            System.out.println(info.dump("Count Dump 2"));
            
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
    
    public static void main_prime(String args[])
    {

        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
                
            DoScan doScan = DoScan.getScan(nodeNumber, connection, logger);
            String afterKey = " ";
            int maxKeys=5000;
            doScan.process(afterKey, maxKeys);
            DoScan.ScanInfo cnt = doScan.getScanCnt();
            System.out.println(cnt.dump("Count Dump"));
            
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
    
    public static void main_original(String args[])
    {

        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            DoScan doScan = DoScan.getScan(nodeNumber, connection, logger);
            String afterKey = " ";
            int maxKeys=2000;
            ArrayList<CloudList.CloudEntry> entryList = doScan.getList(afterKey, maxKeys);
            for (CloudList.CloudEntry entry: entryList) {
                System.out.println(entry.key);
            }
            String s3Ark = "ark:/13030/hb0w1005w7";
            HashMap<String,String> scanHash = doScan.getHash(s3Ark);
            Set<String> keys = scanHash.keySet();
            for (String key : keys) {
                System.out.println("keys:" + key);
            }
            doScan.process(afterKey, maxKeys);
            DoScan.ScanInfo cnt = doScan.getScanCnt();
            System.out.println(cnt.dump("Count Dump"));
            
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
    public static DoScan getScan(
            long inNode,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        NodeIO nodeIO = ReplicationConfig.getNodeIO();
        if (nodeIO == null) {
            throw new TException.INVALID_CONFIGURATION("NodeIO not set");
        }
        NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
        if (inAccessNode == null) {
            throw new TException.INVALID_CONFIGURATION("AccessNode not found for given node:" + inNode);
        }
        DoScan doScan = new DoScan(inNode, inAccessNode.service, inAccessNode.container, connection, logger);
        return doScan;
    }
    
    protected DoScan(
            Long inNode,
            CloudStoreInf service,
            String bucket,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        this.inNode = inNode;
        this.service = service;
        this.bucket = bucket;
        
        setConnectionAuto();
        validate();
    }
    
    protected void validate()
        throws TException
    {
        try {
            if (service == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Service not supplied");
            }
            if (service == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "Bucket not supplied");
            }
            if (connection == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "connection not supplied");
            }
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger not supplied");
            }

            nodeid = InvDBUtil.getNodeSeq(inNode,connection, logger);
            if (nodeid == null) {
                throw new TException.INVALID_OR_MISSING_PARM("nodeid not found for:" + inNode);
            }
            if (DEBUG) System.out.println("nodeid:" + nodeid);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public ScanInfo process()
        throws TException
    {
        log(15, "process");
        if (scanInfo == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "scanInfo missing");
        }
        int maxKeys = scanInfo.getMaxkey();
        String afterKey = scanInfo.getLastProcessKey();
        return process(afterKey, maxKeys);
    }
    
    public ScanInfo process(int maxKeys)
        throws TException
    {
        return process(" ", maxKeys);
    }

    public ScanInfo process(String afterKey, int maxKeys)
        throws TException
    {
        if (scanInfo == null) {
            scanInfo = new ScanInfo(afterKey, maxKeys);
        }
        try {
            if (connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            dbAdd = new DBAdd(connection, logger);
            String lastProcessKey = null;
            dbArk = afterKey;
            List<CloudList.CloudEntry> entryList = getList(afterKey, maxKeys);
            log(15, "entryList=" + entryList.size());
            if ((entryList == null) || (entryList.size() < maxKeys)) {
                System.out.println("EOF set");
                scanInfo.setEof(true);
            }
            for (CloudList.CloudEntry entry: entryList) {
                test(entry);
                lastProcessKey = entry.getKey();
            }
            scanInfo.setLastProcessKey(lastProcessKey);
            return scanInfo;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public void test(CloudList.CloudEntry entry)
        throws TException
    {
        
        try {
            String key = entry.getKey();
            String parts[] = key.split("\\|", 3);
            String dataArk = parts[0];
            if (!dataArk.startsWith("ark:")) {
                add(InvStorageMaint.MaintType.mtNonArk, entry);
                return;
            }
            if (!dbArk.equals(dataArk)) {
                dbHash = getHash(dataArk);
                if (dbHash == null) {
                    add(InvStorageMaint.MaintType.mtMissArk, entry);
                    return;
                }
                dbArk = dataArk;
            }
            String response = dbHash.get(key);
            if (response == null) {
                add(InvStorageMaint.MaintType.mrtMissFile, entry);
                if (false) {
                    dumpHash("***test dataArk:" + dataArk);
                    throw new TException.GENERAL_EXCEPTION("test");
                }
                return;
            }
            scanInfo.bump(InvStorageMaint.MaintType.mtOK);
            if (DEBUG) System.out.println(">>MATCH" + entry.getKey());
            return;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public void add(InvStorageMaint.MaintType addType, CloudList.CloudEntry entry)
        throws TException
    {
        
        try {
            
            log(15, ">>ADD(" + addType.name() + "):" + entry.key);
            scanInfo.bump(addType);
            if (false) return;
            
            setConnectionAuto();
            InvStorageMaint dbStorageMaint = InvDBUtil.getStorageMaint(nodeid, entry.key, connection, logger);
            if (dbStorageMaint != null) {
                if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("saved", dbStorageMaint.retrieveProp()));
                scanInfo.bumpMatch();
                return;
            }
            String entryKey = entry.getKey();
            
            InvStorageMaint invStorageMaint = new InvStorageMaint(nodeid, 1, addType, entry, logger);
            invStorageMaint.setMaintStatus(InvStorageMaint.MaintStatus.review);
            long ismseq = dbAdd.insert(invStorageMaint);
            log(15, ">>INSERT(" + addType.name() + "):" + entry.key);
            
            if (DEBUG) System.out.println("ismseq=" + ismseq + " " 
                    + PropertiesUtil.dumpProperties("invStorageMaint", invStorageMaint.retrieveProp()));
            
            //throw new TException.GENERAL_EXCEPTION("test");
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    protected void setConnectionAuto()
        throws TException
    {
        try {
            connection.setAutoCommit(true);
        } catch (Exception ex) { 
            throw new TException(ex);
        }
    }
    
    public HashMap<String,String> getHash(String s3Ark)
        throws TException
    {
        try {
            Identifier ark = new Identifier(s3Ark);
            if (DEBUG) System.out.println("getHash:"
                    + " - ark:" + ark.getValue()
                    + " - inNode:" + inNode
                    + " - connection:" + connection.getAutoCommit()
            );
            ArrayList<String> nodeKeys = ReplicDBUtil.getNodeKeys(ark, connection, logger);
            if (nodeKeys == null) return null;
            HashMap<String,String> hash = ReplicDBUtil.getHashNode(inNode, ark, nodeKeys, logger);
            
            scanInfo.bumpDB();
            return hash;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public ArrayList<CloudList.CloudEntry> getList(String afterKey, int maxKeys)
        throws TException
    {
        
        try {
            
            CloudResponse response = service.getObjectListAfter(bucket, afterKey, maxKeys);
            if (response.getException() != null) {
                throw response.getException();
            }
            CloudList cloudList = response.getCloudList();
            ArrayList<CloudList.CloudEntry> entryList = cloudList.getList();
            return entryList;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void dumpHash(String header)
    {
        System.out.println("dumpHash <" + header + ">");
        if (dbHash == null) {
            System.out.println("dbHash null");
            return;
        }
        Set<String> keys = dbHash.keySet();
        for (String key : keys) {
            System.out.println("dbHash Key:" + key);
        }
    }

    public ScanInfo getScanCnt() {
        return scanInfo;
    }
    
    public static class ScanInfo  
    {         
        protected HashMap<InvStorageMaint.MaintType, Long> scanCnt = new HashMap<>();
        protected String lastProcessKey = " ";
        protected int maxkey = 0;
        protected long dbCnt = 0;
        protected long matchCnt = 0;
        protected boolean eof = false;
        
        public ScanInfo(String lastProcessKey, int maxkey)
        {
            this.lastProcessKey = lastProcessKey;
            this.maxkey = maxkey;
        }

        public void bump(InvStorageMaint.MaintType addType)
        {   
            Long cnt = scanCnt.get(addType);
            if (cnt == null) {
                cnt = 0L;
            }
            cnt++;
            scanCnt.put(addType, cnt);
        }
        public void bumpDB()
        {   
            dbCnt++;
        }
        
        public void bumpMatch()
        {   
            matchCnt++;
        }

        public long get(InvStorageMaint.MaintType addType)
        {   
            Long cnt = scanCnt.get(addType);
            if (cnt == null) {
                return 0L;
            }
            return cnt;
        }

        public String dump(String header)
        {   
            StringBuffer buf = new StringBuffer();
            buf.append(header + "\n");
            for (InvStorageMaint.MaintType type : InvStorageMaint.MaintType.values()) { 
                Long cnt = scanCnt.get(type);
                if (cnt != null) {
                    buf.append(" - " + type.toString() + "=" + cnt + "\n");
                }
            }
            buf.append(" - dbCnt=" + dbCnt + "\n");
            buf.append(" - matchCnt=" + matchCnt + "\n");
            buf.append(" - LastProcessKey:" + lastProcessKey + "\n");
            buf.append(" - eof:" + eof + "\n");
            return buf.toString();
        }

        public String getLastProcessKey() {
            return lastProcessKey;
        }

        public void setLastProcessKey(String lastProcessKey) {
            this.lastProcessKey = lastProcessKey;
        }

        public int getMaxkey() {
            return maxkey;
        }

        public void setMaxkey(int maxkey) {
            this.maxkey = maxkey;
        }

        public boolean isEof() {
            return eof;
        }

        public void setEof(boolean eof) {
            this.eof = eof;
        }
        
    }
    
}

