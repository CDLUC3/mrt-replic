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
import java.util.List;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Run fixity
 * @author dloy
 */
public class DoScanNext
        extends DoScanBase
{

    private static final String NAME = "DoScanNext";
    private static final String MESSAGE = NAME + ": ";
    
    public static void main(String args[])
    {

        long nodeNumber = 5001;
        String lastKey = " ";
        
        LoggerInf logger = new TFileLogger("DoScan", 5, 20);
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            DoScanNext scan = getScanNext(5001, 2L, lastKey, connection, logger);
            
            connection = db.getConnection(true);
            ScanInfo info = scan.process(10, connection);
            connection.close();
            System.out.println("*************************");
            long currentPos = scan.getCurrentPos();
            System.out.println("START:"
                    + " - currentPos=" + currentPos
            );
            connection = db.getConnection(true);
            ScanInfo info2 = scan.process(10000, connection);
            connection.close();
            
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
    
    public static DoScanNext getScanNext(
            long inNode,
            Long storageScanId,
            String lastKey,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        DoScanNext doScanNext = new DoScanNext(inNode, storageScanId, lastKey, connection, logger);
        return doScanNext;
    }
    
    protected DoScanNext(
            Long inNode,
            Long storageScanId,
            String lastKey,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(inNode, storageScanId, connection, logger);
        this.lastKey = lastKey;
        try {
            connection.close();
        } catch (Exception xx) { }
    } 
    
    
    
    public ScanInfo process(
            int maxKeys,
            Connection connection)
        throws TException
    {
        try {
            if (connection.isClosed()) {
                throw new TException.GENERAL_EXCEPTION("connection closed");
            }
            this.connection = connection;
            setConnectionAuto();
            dbAdd = new DBAdd(this.connection, logger);
            String lastProcessKey = null;
            dbArk = lastKey;
            List<String> keys = getKeys(maxKeys);
            log(15, "entryList=" + keys.size());
            if ((keys == null) || (keys.size() < maxKeys)) {
                System.out.println("EOF set");
                
                scanInfo.setEof(true);
            }
            for (String key: keys) {
                test(key);
                lastProcessKey = key;
                lastKey = key;
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
    
    
    public ArrayList<String> getKeys(int maxKeys)
        throws TException
    {
        ArrayList<String> keys = new ArrayList<>();
        try {
            CloudResponse response = service.getObjectListAfter(bucket, lastKey, maxKeys);
            if (response.getException() != null) {
                throw response.getException();
            }
            CloudList cloudList = response.getCloudList();
            ArrayList<CloudList.CloudEntry> entryList = cloudList.getList();
            String key = null;
            for (CloudList.CloudEntry entry : entryList) {
                key = entry.getKey();
                keys.add(key);
            }
            lastKey = key;
            return keys;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }   
    
    public String skip() 
        throws TException
    {
        return lastKey;
    }
    
    
}

