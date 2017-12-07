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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.s3.tools.CloudManifestCopy;
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
public class CopyRestore
        extends ReplicActionAbs
{

    protected static final String NAME = "CopyRestore";
    protected static final String MESSAGE = NAME + ": ";

    protected DPRFileDB db = null;
    protected String nodeName = null;
    protected NodeService inService = null;
    protected NodeService outService = null;
    protected File objectList = null;
    protected BufferedReader br = null;
    protected CloudManifestCopy copy = null;
    protected int inCnt = 0;
    protected int completeCnt = 0;
    protected int alreadyRestoredCnt = 0;
    protected int copyCnt = 0;
    protected int notFoundCnt = 0;
    protected int restoreInProcessCnt = 0;
    protected int restoredCnt = 0;
    protected boolean runComplete = true;
    //protected Boolean objectComplete = null;//protected Boolean objectComplete = null;
    
    public static void main(String args[])
    {

        TFrame tFrame = null;
        DPRFileDB db = null;
        try {
            String propertyList[] = {
                "resources/ReplicLogger.properties",
                "resources/RestoreObjectTest.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties invProp  = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            db = new DPRFileDB(logger, invProp);
            String arks ="/apps/replic/test/replic/170614-fix-jais/ark10.txt";
            File listArks = new File(arks);
            String nodeName = "nodes-dev";
            long inNode = 6001;
            long outNode = 5001;
            CopyRestore copyRestore = getCopyRestore(db, listArks, nodeName, inNode, outNode, logger);
            copyRestore.dump("Start");
            boolean runComplete = copyRestore.process();
            System.out.println("**************************************Complete:" + runComplete);
            copyRestore.dump("Final");

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
    
    public static CopyRestore getCopyRestore(
            DPRFileDB db,
            File objectList,
            String nodeName,
            long inNode,
            long outNode,
            LoggerInf logger)
        throws TException
    {
        return new CopyRestore(db, objectList, nodeName, inNode, outNode, logger);
    }
    
    protected CopyRestore(
            DPRFileDB db,
            File objectList,
            String nodeName,
            long inNode,
            long outNode,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.db = db;
        setBufferedReader(objectList);
        this.nodeName = nodeName;
        inService = setNodeService(inNode);
        outService = setNodeService(outNode);
        setCopy();
        validate();
    }
    
    protected void validate()
        throws TException
    {
        if (db == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "db not supplied");
        }
    }
    
    protected void setBufferedReader(File fileList)
        throws TException
    {

        try {
            if (fileList == null) {
                throw new TException.INVALID_OR_MISSING_PARM("fileList missing");
            }
            if ((fileList == null) || !fileList.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("fileList not found:" + fileList.getCanonicalPath());
            }
            InputStream inStream = new FileInputStream(fileList);
            DataInputStream in = new DataInputStream(inStream);
            br = new BufferedReader(new InputStreamReader(in, "utf-8"));

        } catch (Exception ex) {
            logger.logError(MESSAGE + "ProcessList - Exception:" + ex, 0);
            logger.logMessage("trace:" + StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "setInputStream - Exception:" + ex);

        }
    }
    
    protected NodeService setNodeService(long node)
        throws TException
    {
        return NodeService.getNodeService(nodeName, node, logger);
    }
    
    protected void setCopy()
        throws TException
    {
        try {
            if (inService == null) {
                throw new TException.INVALID_OR_MISSING_PARM("inService missing");
            }
            if (outService == null) {
                throw new TException.INVALID_OR_MISSING_PARM("outService missing");
            }
            CloudStoreInf in = inService.getCloudService();
            CloudStoreInf out = outService.getCloudService();
            String inContainer = inService.getBucket();
            String outContainer = outService.getBucket();
            copy = new CloudManifestCopy(in, inContainer, out, outContainer, logger);
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public boolean process()
        throws TException
    {
        boolean runComplete = true;
        String line = null;
        try {
            while (true) {
                line = br.readLine();
                //System.out.println("!!!!: line=" + line);
                //System.out.println("!!!!: prevLine=" + prevLine);
                if (line == null) {
                    br.close();
                    return runComplete;
                }
                if (line.substring(0,1).equals('#')) {
                    continue;
                }
                inCnt++;
                Identifier objectID = new Identifier(line);
                boolean complete = processObject(objectID);
                runComplete |= complete;
            }
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        } finally {
            try {
                br.close();
            } catch (Exception ex) { }
        }
    }
    
    protected boolean processObject(Identifier objectID)
        throws TException
    {
        Connection connect = null;
        try {
            connect = db.getConnection(true);
            String manifestKey = objectID.getValue()+ "|manifest";
            Properties objectProp = outService.getObjectMeta(manifestKey);
            System.out.println(PropertiesUtil.dumpProperties("processObject entered:" + objectID.getValue(), objectProp));
            // if copied then no further process
            if ((objectProp != null) && (objectProp.size() > 0) ) {
                alreadyRestoredCnt++;
                return true;
            }
            Properties inObjectProp = inService.getObjectMeta(manifestKey);
            System.out.println(PropertiesUtil.dumpProperties("processObject entered:" + objectID.getValue(), inObjectProp));
            // if copied then no further process
            if ((inObjectProp != null) && (inObjectProp.size() > 0) ) {
                String ongoingRestore = inObjectProp.getProperty("ongoingRestore");
                if (ongoingRestore != null) {
                    restoreInProcessCnt++;
                    return true;
                }
            }
            RestoreObject restoreObject = RestoreObject.getRestore(inService,
                objectID,
                connect,
                logger);
            boolean objectRestored = false;
            try {
                objectRestored = restoreObject.process();
            } catch (TException.REQUESTED_ITEM_NOT_FOUND rinf) {
                notFoundCnt++;
                return false;
            }
            if (!objectRestored) {
                return false;
            }
            restoredCnt++;
            copy.copyObject(objectID.getValue());
            copyCnt++;
            return true;
            
            
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
    
    public void dump(String header)
    {
        System.out.println("***" + NAME + ":" + header + '\n'
                + " - runComplete:" + runComplete + '\n'
                + " - inCnt:" + inCnt + '\n'
                + " - restoreInProcessCnt:" + restoreInProcessCnt + '\n'
                + " - notFoundCnt:" + notFoundCnt + '\n'
                + " - completeCnt:" + completeCnt + '\n'
                + " - alreadyRestoredCnt:" + alreadyRestoredCnt + '\n'
                + " - copyCnt:" + copyCnt + '\n'
                + " - restoredCnt:" + restoredCnt + '\n'
        );
    }
}

