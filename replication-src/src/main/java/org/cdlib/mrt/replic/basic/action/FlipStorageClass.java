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

import com.amazonaws.services.s3.model.StorageClass;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Date;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.db.DBUtil;

import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.replic.utility.ReplicProp;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
import org.cdlib.mrt.s3.aws.AWSObjectStorageClassConvert;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.s3.tools.CloudManifestCopy;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.TFrame;

/**
 * Run fixity
 * @author dloy
 */
public class FlipStorageClass
        extends ReplicActionAbs
{

    protected static final String NAME = "CopyRestore";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;

    protected DPRFileDB db = null;
    protected StorageClass targetStorageClass = null;
    protected String bucket = null;
    protected AWSS3Cloud awsCloud = null;
    protected AWSObjectStorageClassConvert objectConvert = null;
    protected int nodeseq = 0;
    protected long nextNodeObjectSeq = 0;
    protected String nextArk = null;
    //protected Boolean objectComplete = null;//protected Boolean objectComplete = null;
    protected Long maxCnt = null;
    protected long inCnt = 0;
    protected long matchCnt = 0;
    protected long errorCnt = 0;
    protected long bigCnt = 0;
    protected long flipCnt = 0;
    protected long fileCnt = 0;
    protected long totalValidate= 0;
    protected long totalSize = 0;
    protected long totalTime = 0;
    protected boolean endList = false;
    protected boolean testBig = false;
    protected long modDump = 10;
    
    public enum ResetAction {convert, match, error, end};
    
    
    public static void main(String args[])
    {

        TFrame tFrame = null;
        DPRFileDB db = null;
        try {
            String propertyList[] = {
                "resources/FlipStorageClass.properties"};
            tFrame = new TFrame(propertyList, "FlipStorageClass");
            Properties flipProp  = tFrame.getProperties();
            FlipStorageClass flipStorageClass =  FlipStorageClass.getFlipStorageClass(flipProp);
            flipStorageClass.propDump();
            flipStorageClass.process();

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
    
    public static void main_test1(String args[])
    {

        TFrame tFrame = null;
        DPRFileDB db = null;
        try {
            String propertyList[] = {
                "resources/ReplicLogger.properties",
                "resources/RestoreObjectTest.properties"};
            tFrame = new TFrame(propertyList, "FlipStorageClass");
            Properties invProp  = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("testFormatter", 5, 10);
            db = new DPRFileDB(logger, invProp);
            String bucket     = "uc3-s3mrt5001-dev";
            //StorageClass outStorageClass = StorageClass.ReducedRedundancy;
            StorageClass outStorageClass = StorageClass.Standard;
            FlipStorageClass flipStorageClass =  FlipStorageClass.getFlipStorageClass(db, outStorageClass, 22, bucket, logger);
            //flipStorageClass.setMaxCnt(10L).startNodeObjectSeq(40000);
            flipStorageClass.propDump();
            flipStorageClass.process();

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
    
    public static  FlipStorageClass getFlipStorageClass(Properties flipProp)
        throws TException
    {
        DPRFileDB db = null;
        try {
            System.out.println(PropertiesUtil.dumpProperties(NAME, flipProp));
            LoggerInf logger = new TFileLogger("flip", "logs", flipProp);
            StorageClass targetStorageClass = null;
            String targetStorageClassS = ReplicProp.getEx(flipProp, "targetStorageClass");
            if (targetStorageClassS == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "targetStorageClass invalid:" + targetStorageClassS);
            }
            
            targetStorageClassS = targetStorageClassS.toLowerCase();
            if (targetStorageClassS.equals("standard")) 
                targetStorageClass = StorageClass.Standard;
            else if (targetStorageClassS.equals("reducedredundancy")) 
                targetStorageClass = StorageClass.ReducedRedundancy;
            else if (targetStorageClassS.equals("standardinfrequentaccess")) 
                targetStorageClass = StorageClass.StandardInfrequentAccess;
            if (targetStorageClass == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "targetStorageClass invalid:" + targetStorageClassS);
            }
            if (DEBUG) System.out.println(PropertiesUtil.dumpProperties(MESSAGE + "main", flipProp));
           
            int nodeseq = ReplicProp.getExNumInt(flipProp, "nodeseq");
            
            String bucket = ReplicProp.getEx(flipProp, "bucket");
            
            Long maxCnt = ReplicProp.getNumLong(flipProp, "maxCnt");
            Long startNodeObjectSeq = ReplicProp.getNumLong(flipProp, "startNodeObjectSeq");
            Long modDump = ReplicProp.getNumLong(flipProp, "modDump");
            Boolean testBigB = ReplicProp.getBool(flipProp, "testBig");
            
            db = new DPRFileDB(logger, flipProp);
            FlipStorageClass flip 
                    =  FlipStorageClass.getFlipStorageClass(db, targetStorageClass,nodeseq, bucket, logger);
            
            if (maxCnt != null) {
                flip.setMaxCnt(maxCnt);
            }
            
            if (startNodeObjectSeq != null) {
                flip.startNodeObjectSeq(startNodeObjectSeq);
            }
            
            if (modDump != null) {
                flip.setModDump(modDump);
            }
            
            if (testBigB != null) {
                flip.setTestBig(testBigB);
            }
            return flip;
            
            //service.shutdown();
        } catch(TException tex) {
                tex.printStackTrace();
                throw tex;

        } catch(Exception ex) {
                ex.printStackTrace();
                throw new TException(ex);
        }
    }
    
    public static FlipStorageClass getFlipStorageClass(
            DPRFileDB db,
            StorageClass targetStorageClass,
            int nodeseq,
            String bucket,
            LoggerInf logger)
        throws TException
    {
        return new FlipStorageClass(db, targetStorageClass, nodeseq, bucket, logger);
    }
    
    protected FlipStorageClass(
            DPRFileDB db,
            StorageClass targetStorageClass,
            int nodeseq,
            String bucket,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.db = db;
        this.bucket = bucket;
        this.awsCloud = AWSS3Cloud.getAWSS3(logger);
        this.objectConvert = new AWSObjectStorageClassConvert(awsCloud, bucket, targetStorageClass, logger);
        this.nodeseq = nodeseq;
        this.targetStorageClass = targetStorageClass;
        System.out.println(">>>FlipStorageClass:"
            + " - nodeseq:" + this.nodeseq
            + " - targetStorageClass:" + this.targetStorageClass.toString()
            + " - nextNodeObjectSeq:" + this.nextNodeObjectSeq
            + " - maxCnt:" + this.maxCnt
        );
        validate();
    }
    
    protected void validate()
        throws TException
    {
        if (db == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "db not supplied");
        }
    }
 

    public boolean process()
        throws TException
    {
        try {
            while (!endList) {
                ObjectAction objectAction = null;
                try {
                    objectAction = resetObject();
                    
                } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                    String msg = MESSAGE + "NOT FOUND " + rinf
                        + " - nodeseq:" + nodeseq
                        + " - bucket:" + bucket
                        + " - nextArk:" + nextArk
                            ;
                    System.out.println(msg);
                    errorCnt++;
                    continue;
                }
                if (endList) break;
                if (maxCnt != null) {
                    if (inCnt >= maxCnt) break;
                }
                inCnt++;
                ResetAction action = objectAction.action;
                switch (action) {
                    case convert: 
                        flipCnt++;
                        break;
                    case match: 
                        matchCnt++;
                        break;
                    case error: 
                        errorCnt++;
                        break;
                }
                fileCnt += objectAction.fileCnt;
                bigCnt += objectAction.bigCnt;
                totalValidate += objectAction.validateCnt;
                totalSize += objectAction.size;
                totalTime += objectAction.time;
                if (DEBUG) objectAction.dump("object");
                if ((inCnt % modDump) == 0) {
                    dump("Partial(" + inCnt + "):" + objectAction.ark + " - " + DateUtil.getCurrentIsoDate());
                }
            }
            dump("Final");
            return true;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                db.shutDown();
            } catch (Exception ex) { 
                System.out.println("db Exception:" + ex);
            }
        }
    }
    
    protected ObjectAction resetObject()
        throws TException
    {
        Connection connect = null;
        try {
            connect = db.getConnection(true);
            setNextArk(connect);
            if (endList) {
                return null;
            }
            
            ObjectAction objectAction = new ObjectAction();
            objectAction.ark = nextArk;
            objectAction.nodeObjectSeq = nextNodeObjectSeq;
            
            ResetAction matchAction = matchStorageClass();
            if (matchAction != ResetAction.convert) {
                if (testBig) {
                    AWSObjectStorageClassConvert.ObjectStats stats = objectConvert.testObject(nextArk);
                    objectAction.set(stats);
                }
                objectAction.action = matchAction;
                return objectAction;
            } else {
                AWSObjectStorageClassConvert.ObjectStats stats = objectConvert.convertObject(nextArk);
                objectAction.set(stats);
                objectAction.action = ResetAction.convert;
                return objectAction;
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            try {
                connect.close();
            } catch (Exception ex) { }
        }
    }
    
    
    protected Properties nextArk(Connection connect, int nodeseq, long lastId,  LoggerInf logger)
        throws TException
    {
        log("getNodeObjects entered");
        String sql = 
            "select inv_nodes_inv_objects.id, inv_objects.ark "
                + "from inv_nodes_inv_objects,inv_objects "
                + "where inv_nodes_inv_objects.id > " + lastId + " "
                + "and inv_nodes_inv_objects.inv_node_id=" + nodeseq + " "
                //+ "and inv_nodes_inv_objects.role='primary' "
                + "and inv_nodes_inv_objects.inv_object_id=inv_objects.id "
                + "limit 1; ";
        Properties[] propArray = DBUtil.cmd(connect, sql, logger);
        
        if ((propArray == null)) {
            log("InvDBUtil - prop null");
            return null;
        } else if (propArray.length == 0) {
            log("InvDBUtil - length == 0");
            return null;
        }
        if (DEBUG) log("DUMP" + PropertiesUtil.dumpProperties("prop", propArray[0]));
        return propArray[0];
    }

    protected boolean setNextArk(Connection connect)
        throws TException
    {
        long lastNodeObjectSeq = nextNodeObjectSeq;
        try {
            Properties arkProp = nextArk(connect, nodeseq, lastNodeObjectSeq, logger);
            if (arkProp == null) return setEnd();
            String ark = arkProp.getProperty("ark");
            String nodeseqS = arkProp.getProperty("id");
            if ((ark == null) || (nodeseqS == null)) {
                return setEnd();
            }
            
            try {
                nextNodeObjectSeq = Long.parseLong(nodeseqS);
            } catch (Exception ex) {
                return setEnd();
            }
            nextArk = ark;
            if (DEBUG) System.out.println("setNextArk:"
                    + " - nextNodeObjectSeq:" + nextNodeObjectSeq
                    + " - nextArk:" + nextArk
            );
            return true;
            
        } catch (TException tex) {
            tex.printStackTrace();
            return setEnd();
            
        } catch (Exception ex) {
            ex.printStackTrace();
            return setEnd();
            
        }
    }
    

    protected ResetAction matchStorageClass()
        throws TException
    {
        try {
            String keyManifest = nextArk + "|manifest";
            StorageClass manifestClass = awsCloud.getStorageClass(objectConvert.getBucket(), keyManifest);
            if (manifestClass == null) {
                return ResetAction.error;
            }
            
            if (DEBUG) System.out.println("matchStorageClass:"
                + " - manifestClass:" + manifestClass
                + " - targetStorageClass:" + targetStorageClass.toString()
            );
            if (manifestClass == targetStorageClass) {
                return ResetAction.match;
            }
            return ResetAction.convert;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
    protected boolean setEnd()
    {
        nextNodeObjectSeq = 0;
        nextArk = null;
        endList = true;
        return false;
    }

    public FlipStorageClass setMaxCnt(Long maxCnt) {
        this.maxCnt = maxCnt;
        System.out.println("***setMaxCnt:" + this.maxCnt);
        return this;
    }

    public FlipStorageClass startNodeObjectSeq(long startNodeObjectSeq) {
        this.nextNodeObjectSeq = startNodeObjectSeq;
        System.out.println("***startNodeObjectSeq:" + this.nextNodeObjectSeq);
        return this;
    }

    public FlipStorageClass  setModDump(long modDump) {
        this.modDump = modDump;
        System.out.println("***setModDump:" + this.modDump);
        return this;
    }

    public boolean isTestBig() {
        return testBig;
    }

    public void setTestBig(boolean testBig) {
        this.testBig = testBig;
    }
    
    public void dump(String header)
    {
        System.out.println("***" + NAME + ":" + header + '\n'
                + " - restart nextNodeObjectSeq:" + nextNodeObjectSeq + '\n'
                + " - object inCnt:" + inCnt + '\n'
                + " - object matchCnt:" + matchCnt + '\n'
                + " - object flipCnt:" + flipCnt + '\n'
                + " - object errorCnt:" + errorCnt + '\n'
                + " - file bigCnt:" + bigCnt + '\n'
                + " - fileCnt:" + fileCnt + '\n'
                + " - file totalValidate:" + totalValidate + '\n'
                + " - file totalSize:" + totalSize + '\n'
                + " - totalTime:" + totalTime + '\n'
        );
    }
    
    public void propDump()
    {
        System.out.println("***" + NAME + ": Properties" + '\n'
                + " - targetStorageClass:" + targetStorageClass + '\n'
                + " - nodeseq:" + nodeseq + '\n'
                + " - bucket:" + bucket + '\n'
                + " - maxCnt:" + maxCnt + '\n'
                + " - startNodeObjectSeq:" + nextNodeObjectSeq + '\n'
                + " - modDump:" + modDump + '\n'
                + " - testBig:" + testBig + '\n'
        );
    }
    
    public static class ObjectAction
    {
        public String ark = null;
        public long nodeObjectSeq = 0;
        public ResetAction action = ResetAction.end;
        public Boolean actionTaken = null;
        public long fileCnt = 0;
        public long validateCnt = 0;
        public long bigCnt = 0;
        public long size = 0;
        public long time = 0;
        public void set(AWSObjectStorageClassConvert.ObjectStats stats)
        {
            fileCnt = stats.fileCnt;
            validateCnt = stats.validateCnt;
            bigCnt += stats.bigCnt;
            size = stats.size;
            time = stats.time;
        }
        public void dump(String header)
        {
            System.out.println("***" +  header + ':'
                    + " - id:" + ark
                    + " - nodeObjectSeq:" + nodeObjectSeq
                    + " - action:" + action
                    + " - fileCnt:" + fileCnt
                    + " - validateCnt:" + validateCnt
                    + " - size:" + size
                    + " - time:" + time
            );
        }
    }
}

