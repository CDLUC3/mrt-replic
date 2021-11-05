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

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.MessageDigest;
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
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.s3.service.CloudResponse;
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
public abstract class DoScanBase
        extends ReplicActionAbs
{

    private static final String NAME = "DoScanAbs";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    private static final long CURRENT_DATE_DELTA = 1000*60*60*24;

    
    protected CloudStoreInf service = null;
    protected String bucket = null;
    //protected ArrayList<CloudList.CloudEntry> entryList = null;
    //protected ArrayList<CloudList.CloudEntry> failList = new ArrayList<>();
    protected Long inNode = null;
    protected Long nodeid = null;
    protected Long storageScanId = null;
    protected String dbArk = null;
    protected HashMap<String,String> dbHash = null;
    protected HashMap<String,String> dbHashNoNode = null;
    protected DBAdd dbAdd = null;
    protected ScanInfo scanInfo = null;
    protected String lastKey = null;
    protected long lastPos = 0;
    protected long currentPos = 0;
    
    public DoScanBase(
            Long inNode,
            Long storageScanId,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        this.inNode = inNode;
        this.storageScanId = storageScanId;
        this.scanInfo = new ScanInfo();
        setServiceBucket(inNode, connection, logger);
        setDB(inNode, connection, logger);
        validate();
    }

    protected void setServiceBucket(
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
        service = inAccessNode.service;
        bucket = inAccessNode.container;
    }

    protected void setDB(
            long inNode,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        
        try {
            
            this.dbAdd = new DBAdd(connection, logger);
            this.nodeid = InvDBUtil.getNodeSeq(inNode,connection, logger);
            if (nodeid == null) {
                throw new TException.INVALID_OR_MISSING_PARM("nodeid not found for:" + inNode);
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    private void validate()
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
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected void test(String key)
        throws TException
    {
        
        try {
            scanInfo.lastProcessKey = key;
            scanInfo.bumpLastScanCnt();
            String parts[] = key.split("\\|", 3);
            String dataArk = parts[0];
            if (!dataArk.startsWith("ark:")) {
                add(InvStorageMaint.MaintType.mtNonArk, key);
                return;
            }
            if (!dbArk.equals(dataArk)) {
                setHash(dataArk);
                dbArk = dataArk;
            }
            if (dbHashNoNode == null) {
                add(InvStorageMaint.MaintType.mtMissArk, key);
                return;
            }
            
            //matches - no action
            String responseNode = dbHash.get(key);
            if (responseNode != null) {
                return;
            }
            
            String responseNoNode = dbHashNoNode.get(key);
            if (responseNoNode != null) {
                add(InvStorageMaint.MaintType.mtOrphanCopy, key);
                return;
            }
            add(InvStorageMaint.MaintType.mrtMissFile, key);
            return;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    protected void add(InvStorageMaint.MaintType addType, String key)
        throws TException
    {
        if (DEBUG) {
            System.out.println("add(" + scanInfo.getLastScanCnt() + "):"
                    + " - addType:" + addType.toString()
                    + " - key:" + key
            );
        }
        log(10, "add(" + storageScanId + "," + scanInfo.getLastScanCnt() + "):"
                + " - addType:" + addType.toString()
                + " - key:" + key
        );
            
        try {
            CloudList.CloudEntry entry = getCloudEntry(key);
            if (entry == null) {
                log(2, "Add fail 404:" + key);
                return;
            }
            if (isCurrentEntry(entry, CURRENT_DATE_DELTA)) {
                log(5,"Current skip"
                            + " - node:" + inNode
                            + " - date:" + entry.lastModified
                            + " - key:" + entry.key
                            + " - size:" + entry.size
                );
                scanInfo.bumpCurrentCnt();
                return;
            }
            scanInfo.bump(addType);
            if (false) return;
            
            setConnectionAuto();
            InvStorageMaint dbStorageMaint = InvDBUtil.getStorageMaint(nodeid, entry.key, connection, logger);
            if (dbStorageMaint != null) {
                if (DEBUG) System.out.println(PropertiesUtil.dumpProperties("saved", dbStorageMaint.retrieveProp()));
                scanInfo.bumpMatch();
                return;
            }
            
            InvStorageMaint invStorageMaint = new InvStorageMaint(nodeid, storageScanId, addType, entry, logger);
            if ((entry.storageClass != null) && entry.storageClass.contains("REDUCED_REDUNDANCY")) {
                invStorageMaint.setNote("REDUCED_REDUNDANCY");
            }
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

    public static boolean isCurrentEntry(CloudList.CloudEntry entry, long currentSkip)
        throws TException
    {
        try {
            //System.out.println("ISO:" + DateTimeFormatter.ISO_DATE_TIME.toString());
            String lastModified = entry.getLastModified();
            int lpos = lastModified.lastIndexOf(":");
            if (lpos >= 0) {
                lastModified = lastModified.substring(0,lpos) + lastModified.substring(lpos+1);
                //System.out.println("lastModified:" + lastModified);
            }
            Date modifiedDate = DateUtil.getDateFromString(lastModified, "yyyy-MM-dd'T'HH:mm:ssZ");
            Date currentDate = DateUtil.getCurrentDate();
            long modifiedTime = modifiedDate.getTime();
            long currentTime = currentDate.getTime();
            if ((modifiedTime + currentSkip) > currentTime) return true;
            return false;
            
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
    
    protected void setHash(String s3Ark)
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
            if (nodeKeys == null) {
                dbHash = null;
                dbHashNoNode = null;
                return;
            }
            dbHashNoNode =  ReplicDBUtil.getHashNoNode(ark, nodeKeys, logger);
            dbHash = ReplicDBUtil.getHashNode(inNode, ark, nodeKeys, logger);
            
            scanInfo.bumpDB();
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    abstract public ScanInfo process(
            int maxKeys,
            Connection connection)
        throws TException;
    
    abstract protected ArrayList<String> getKeys(int maxKeys)
        throws TException;
    
    /**
     * 
     * @return last skipped key
     * @throws TException 
     */
    abstract protected String skip()
        throws TException;

    
    public CloudList.CloudEntry getCloudEntry(String key)
        throws TException
    {
       
        try {
            Properties objectProp = service.getObjectMeta(bucket, key);
            CloudList.CloudEntry entry = CloudResponse.getCloudEntry(objectProp);
            return entry;
            
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

    public String getLastKey() {
        return lastKey;
    }

    public long getLastPos() {
        return lastPos;
    }

    public ScanInfo getScanCnt() {
        return scanInfo;
    }

    public long getCurrentPos() {
        return currentPos;
    }
    
    public static class ScanInfo  
    {         
        protected HashMap<InvStorageMaint.MaintType, Long> scanCnt = new HashMap<>();
        protected String lastProcessKey = " ";
        protected int maxkey = 0;
        protected long lastScanCnt = 0;
        protected long dbCnt = 0;
        protected long matchCnt = 0;
        protected long currentCnt = 0;
        protected boolean eof = false;
        
        public ScanInfo()
        {
        }
        
        public void set(String lastProcessKey, int maxkey)
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
        
        public void bumpLastScanCnt()
        {   
            lastScanCnt++;
        }
        
        public void bumpCurrentCnt()
        {   
           currentCnt++;
        }
        
        public long getLastScanCnt()
        {   
            return lastScanCnt;
        }

        public void setLastScanCnt(long lastScanCnt) {
            this.lastScanCnt = lastScanCnt;
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
            buf.append(" - currentCnt=" + matchCnt + "\n");
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

        public long getDbCnt() {
            return dbCnt;
        }

        public void setDbCnt(long dbCnt) {
            this.dbCnt = dbCnt;
        }

        public long getMatchCnt() {
            return matchCnt;
        }

        public void setMatchCnt(long matchCnt) {
            this.matchCnt = matchCnt;
        }

        public long getCurrentCnt() {
            return currentCnt;
        }

        public void setCurrentCnt(long currentCnt) {
            this.currentCnt = currentCnt;
        }
        
    }
    
}

