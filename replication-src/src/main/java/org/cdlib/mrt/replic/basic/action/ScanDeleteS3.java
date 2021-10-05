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
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import static org.cdlib.mrt.replic.basic.action.ReplicActionAbs.DEBUG;
import static org.cdlib.mrt.replic.basic.action.ScanWrapper.MESSAGE;
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
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Run fixity
 * @author dloy
 */
public class ScanDeleteS3
        extends ReplicActionAbs
{

    protected static final String NAME = "ScanDeleteS3";
    protected static final String MESSAGE = NAME + ": ";

    protected NodeService nodeService = null;
    protected Long inNode = null;
    protected Long inNodeSeq = null;
    protected DBAdd dbAdd = null;
    protected InvStorageMaint storageMaint = null;
    public static void main(String args[])
    {
        main_s3c(args);
    }
    public static void main_id(String args[])
    {

        long storageMaintId=895; //OK
        String existsKey = "ark:/99999/stg01n5001a|1|system/mrt-membership.txt";
        String existsKey2 = "ark:/99999/stg01n5001a|manifest";
        String existsKey3 = "ark:/99999/stg01n5001a|1|system/mrt-membership2.txt";
        // long maintID=894; //Fail
        LoggerInf logger = new TFileLogger("DoScan", 20, 20);
        DPRFileDB db = null;
        long inNode = 5001;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            
            
            ScanDeleteS3 scanDeleteS3 = ScanDeleteS3.getScanDeleteS3(storageMaintId, connection, logger);
            scanDeleteS3.process();
            
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
    public static void main_storageMaint(String args[])
    {

        long maintID=894; //OK
        String existsKey = "ark:/99999/stg01n5001a|1|system/mrt-membership.txt";
        String existsKey2 = "ark:/99999/stg01n5001a|manifest";
        String existsKey3 = "ark:/99999/stg01n5001a|1|system/mrt-membership2.txt";
        // long maintID=894; //Fail
        LoggerInf logger = new TFileLogger("DoScan", 20, 20);
        DPRFileDB db = null;
        long inNode = 5001;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            InvStorageMaint storageMaint = InvDBUtil.getStorageMaintsFromId(maintID,connection, logger);
            
            ScanDeleteS3 scanDeleteS3 = ScanDeleteS3.getScanDeleteS3(storageMaint, connection, logger);
            System.out.println(PropertiesUtil.dumpProperties("test", storageMaint.retrieveProp()));
            if (false) {
                String key = scanDeleteS3.getStorageMaint().getKey();
                key = existsKey3;
                System.out.println("scan=" + scanDeleteS3.reconfirmCanDelete(key));
            }
            scanDeleteS3.process();
            
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
    
    public static void main_s3c(String args[])
    {

        //long maintID=894; //OK
        String key = "ark:/99999/test|1|prod/test1526587205349";
        //String key = "ark:/99999/test|1|prod/test1526586905288";
        
        // long maintID=894; //Fail
        LoggerInf logger = new TFileLogger("DoScan", 20, 20);
        DPRFileDB db = null;
        long inNode = 5001;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();

            NodeIO nodeIO = ReplicationConfig.getNodeIO();
            if (nodeIO == null) {
                throw new TException.INVALID_CONFIGURATION("NodeIO not set");
            }
            NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
            if (inAccessNode == null) {
                throw new TException.INVALID_CONFIGURATION("AccessNode not found for given node:" + inNode);
            }

            NodeService nodeService =  NodeService.getNodeService(nodeIO, inNode, logger);
            
            Properties tprop = nodeService.getObjectMeta(key);
            if ((tprop == null) || (tprop.size() == 0)) {
                System.out.println("before cloud key missing:" + key);
            } else {
                System.out.println(PropertiesUtil.dumpProperties("before cloud key found", tprop));
            }
            
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        }
    }
    
    public static ScanDeleteS3 getScanDeleteS3(
            long storageScanId,
            Connection connection,
            LoggerInf logger)
        throws TException
    {    
        try {
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger not supplied");
            }
            if (storageScanId < 1) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "storageMaint id required");
            }
            if ((connection == null) || connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            
            InvStorageMaint storageMaint = InvDBUtil.getStorageMaintsFromId(storageScanId, connection, logger);
            return getScanDeleteS3(storageMaint, connection, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static ScanDeleteS3 getScanDeleteS3(
            InvStorageMaint storageMaint,
            Connection connection,
            LoggerInf logger)
        throws TException
    {    
        if (storageMaint == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "storageMaint required");
        }

        if (storageMaint.getMaintStatus() != InvStorageMaint.MaintStatus.delete) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "storageMaint status not equal delete(" 
                    + storageMaint.getId() + "):" + storageMaint.getMaintStatus());
        }
        Long inNode = InvDBUtil.getNodeNumber(storageMaint.getNodeid(), connection, logger);
        if (inNode == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "inNode not found");
        }
        NodeIO nodeIO = ReplicationConfig.getNodeIO();
        if (nodeIO == null) {
            throw new TException.INVALID_CONFIGURATION("NodeIO not set");
        }
        NodeIO.AccessNode inAccessNode = nodeIO.getAccessNode(inNode);
        if (inAccessNode == null) {
            throw new TException.INVALID_CONFIGURATION("AccessNode not found for given node:" + inNode);
        }
        
        NodeService nodeService =  NodeService.getNodeService(nodeIO, inNode, logger);
        ScanDeleteS3 scanDelete = new ScanDeleteS3(inNode, nodeService, storageMaint, connection, logger);
        return scanDelete;
    }
    
    protected ScanDeleteS3(
            Long inNode,
            NodeService nodeService,
            InvStorageMaint storageMaint,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        this.inNode = inNode;
        this.nodeService = nodeService;
        this.storageMaint = storageMaint;
        
        setConnectionAuto();
        validate();
    }
    
    private void validate()
        throws TException
    {
        try {
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger not supplied");
            }

            if (inNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "inNode required");
            }
            
            inNodeSeq = InvDBUtil.getNodeSeq(inNode,connection, logger);
            if (inNodeSeq == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeid not found for:" + inNode);
            }
            
            if (inNodeSeq != storageMaint.getNodeid()) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "storageMaint nodeid not = inNode nodeseq:"
                        + " - inNode.nodeid=" + inNodeSeq
                        + " - StorageMaint.nodeid=" + storageMaint.getNodeid()
                );
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public InvStorageMaint process()
        throws TException
    {
        log(15, "process");
        try {
            if (connection == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "connection not supplied");
            }
            
            String key = storageMaint.getKey();
            boolean delete = reconfirmCanDelete(key);
            if (DEBUG) System.out.println("delete:" + delete);
            if (reconfirmCanDelete(key)) {
                System.out.println("doDelete");
                deleteS3(key);
                updateDB();
            }
            
            log(2, PropertiesUtil.dumpProperties((MESSAGE + "delete"), storageMaint.retrieveProp())
            );
            return storageMaint;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public boolean deleteS3(String key)
        throws TException
    {
        
        try {
            testMeta("before", key);
            CloudResponse response = nodeService.deleteObject(key);
            Exception ex = response.getException();
            if (ex != null) {
                if (ex instanceof TException) {
                    throw (TException) ex;
                }
                throw ex;
            }
            testMeta("after", key);
            log(5, "deleteS3: Delete:"
                    + " - node=" + inNode
                    + " - key=" + key
            );
            return true;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void testMeta(String header, String key)
        throws TException
    {
        
        try {
            System.out.print(header + " - key:" + key);
            Properties tprop = nodeService.getObjectMeta(key);
            if ((tprop == null) || (tprop.size() == 0)) {
                System.out.println(" - missing");
            } else {
                System.out.println(" - found");
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public void updateDB()
        throws TException
    {
        long dbCnt = 0;
        try {
            if (connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            storageMaint.setMaintStatus(InvStorageMaint.MaintStatus.removed);
            storageMaint.setRemovedDB(null);
            dbAdd = new DBAdd(connection, logger);
            connection.setAutoCommit(true);
            dbCnt = dbAdd.update(storageMaint);
            if (DEBUG) System.out.println("After connection:" + connection.getAutoCommit());
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public boolean reconfirmCanDelete(String key)
        throws TException
    {
        
        try {
            if (connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            String [] parts = key.split("\\|",3);
                if (DEBUG) {
                for (String part : parts) {
                    System.out.println("part=" + part);
                }
            }
            if (parts.length == 1) return true;
            if (!parts[0].startsWith("ark")) return true;
            if (parts.length == 2) {
                if (!parts[1].contains("manifest")) return true;
            }
            String s3Ark = parts[0];
            Identifier ark = new Identifier(s3Ark);
            if (DEBUG) System.out.println("getHash:"
                    + " - ark:" + ark.getValue()
                    + " - inNode:" + inNode
                    + " - connection:" + connection.getAutoCommit()
            );
            HashMap<String,String> hash = ReplicDBUtil.getKeyHash(ark, inNode, connection, logger);
            if (hash == null) return true;
            if (!hash.containsKey(key)) {
                return true;
            }
            return false;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public Long getInNode() {
        return inNode;
    }

    public Long getInNodeSeq() {
        return inNodeSeq;
    }

    public InvStorageMaint getStorageMaint() {
        return storageMaint;
    }


    private void setConnectionAuto()
        throws TException
    {
        try {
            connection.setAutoCommit(true);
        } catch (Exception ex) { 
            throw new TException(ex);
        }
    }
    
}

