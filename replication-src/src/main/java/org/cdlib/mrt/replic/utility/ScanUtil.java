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
package org.cdlib.mrt.replic.utility;

import org.cdlib.mrt.replic.basic.action.*;
import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import org.cdlib.mrt.core.DateState;

import org.cdlib.mrt.inv.content.InvStorageMaint;
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import static org.cdlib.mrt.replic.basic.action.ScanWrapper.getLastScan;
import static org.cdlib.mrt.replic.basic.action.ScanWrapper.writeStorageScan;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Run fixity
 * @author dloy
 */
public class ScanUtil
{
    
    private static final String NAME = "ScanUtil";
    private static final String MESSAGE = NAME + ": ";


    public static List<InvStorageScan> getStorageScanStatus(
            long nodeNum,
            String scanStatus,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return InvDBUtil.getStorageScanStatus(nodeNum, scanStatus, connection, logger);
    }
    
    public static void resetStorageScanStatus(
            String scanStatus,
            InvStorageScan storageScan,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        storageScan.setScanStatusDB(scanStatus);
        writeStorageScan(storageScan, connection, logger);
    }
    
    public static void writeStorageScan(InvStorageScan storageScan, Connection connection, LoggerInf logger)
        throws TException
    {
        try {
            DBAdd dbAdd = new DBAdd(connection, logger);
            if (!connection.isValid(10)) {
                throw new TException.EXTERNAL_SERVICE_UNAVAILABLE(MESSAGE + "rewriteAdminMaint connection not valid");
            }
            connection.setAutoCommit(true);
            long ismseq = dbAdd.insert(storageScan);
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
    }
    
    public static boolean testConnect(String header, Connection connection)
        throws Exception
    {
        if (connection == null) {
            System.out.println("CONNECT - " + header + ": connection null");
            return false;
        }
        if (connection.isClosed()) {
            System.out.println(header + ": connection closed");
            return false;
        }
        
        System.out.println(header + ": connection open");
        return true;
    }

    public static InvStorageScan getLastScan(
            long nodeNum,
            String scanStatus,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        List<InvStorageScan> scanList = ScanUtil.getStorageScanStatus(nodeNum, scanStatus, connection, logger);
        if (scanList == null) {
            System.out.println("scanList null");
            return null;
        }
        int last = (scanList.size() - 1);
        System.out.println("**getLastScan:" + last);
        InvStorageScan scan = scanList.get(last);
        return scan;
    }
    
    public static InvStorageScan getStorageScan(int scanNum, Connection connection, LoggerInf logger)
        throws TException
    {
        try {
            InvStorageScan activeScan = InvDBUtil.getStorageScanId(scanNum, connection, logger);
            if (activeScan == null) {
               throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "getRestartScan Scan Id not found:" + scanNum);
            }
            return activeScan;
                
        } catch (TException tex) {
            throw tex ;
            
        } catch (Exception ex) {
            throw new TException(ex) ;
            
        }
        
    }
}

