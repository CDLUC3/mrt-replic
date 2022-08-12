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
package org.cdlib.mrt.replic.basic.test;

import org.cdlib.mrt.replic.basic.action.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
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
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.replic.utility.ReplicDBUtil;

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
public class DoEmoji2
        extends ReplicActionAbs
{

    protected static final String NAME = "DoScanTest";
    protected static final String MESSAGE = NAME + ": ";
    
    public static void main(String args[])
    {

        long nodeNumber = 9502;
        LoggerInf logger = new TFileLogger("DoScan", 9, 10);
        String insert = "insert into inv_storage_maints set "
                + "maint_status='review',maint_admin='none',file_created='2021-02-22 12:54:21',keymd5='d8c20019367f90fb90e56b31c2da3-38',inv_node_id='11',created='2021-08-16 13:43:06',maint_type='missing-file',s3key='ark:/99999/fk42v4011j|1|producer/images🙃emo-38.jpeg',note=null,size='13043';";
        DPRFileDB db = null;
        try {
          
            ReplicationConfig config = ReplicationConfig.useYaml();
            db = config.startDB();
            Connection connection = db.getConnection(true);
            //String select = "select * from inv_storage_maints;";
            //checkText(connection, select);
            System.out.println("(*****<version>5.1.49</version>*****");
            exec(connection, insert, logger);
            
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
    
    public static long exec(
            Connection connection,
            String replaceCmd,
            LoggerInf logger)
        throws TException
    {
        if (StringUtil.isEmpty(replaceCmd)) {
            throw new TException.INVALID_OR_MISSING_PARM("replaceCmd not supplied");
        }
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM("connection not supplied");
        }
        long autoID = 0;
        Statement statement = null;
        try {

            statement = connection.createStatement();
            boolean works = statement.execute(replaceCmd, Statement.RETURN_GENERATED_KEYS);
            ResultSet rs = statement.getGeneratedKeys();
            if (rs.next()){
                autoID=rs.getInt(1);
            }
            System.out.println("****autoID=" + autoID);
            return autoID;

        } catch(Exception e) {
            String msg = "Exception"
                + " - sql=" + replaceCmd
                + " - exception:" + e;

            logger.logError(MESSAGE + "exec - " + msg, 0);
            System.out.println(msg);
            throw new TException.SQL_EXCEPTION(msg, e);
            
        } finally {
            try {
		statement.close();
            } catch (Exception e) { }
	}
    }
    
    
    public static void execUpdate(
            Connection connection,
            String replaceCmd,
            LoggerInf logger)
        throws TException
    {
        if (StringUtil.isEmpty(replaceCmd)) {
            throw new TException.INVALID_OR_MISSING_PARM("replaceCmd not supplied");
        }
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM("connection not supplied");
        }
        long autoID = 0;
        Statement statement = null;
        try {

            statement = connection.createStatement();
            statement.executeUpdate(replaceCmd);
            java.sql.ResultSet rs = statement.getResultSet();
            while (rs.next())
            {
                String k = rs.getString(1);
                String v = rs.getString(2);
                String toOut = ">>> " + k + " - " + v;
                System.out.println(toOut);
            }

        } catch(Exception e) {
            String msg = "Exception"
                + " - sql=" + replaceCmd
                + " - exception:" + e;
            e.printStackTrace();

            logger.logError(MESSAGE + "exec - " + msg, 0);
            System.out.println(msg);
            throw new TException.SQL_EXCEPTION(msg, e);
            
        } finally {
            try {
		statement.close();
            } catch (Exception e) { }
	}
    }
    
    public static void checkText(
            Connection connection,
            String query
            ) throws Exception
{
    java.sql.Statement stmt = connection.createStatement();
    stmt.execute(query);
    java.sql.ResultSet rs = stmt.getResultSet();
    while (rs.next())
    {
        String k = rs.getString(1);
        String v = rs.getString(2);
        String toOut = ">>> " + k + " - " + v;
        System.out.println(toOut);
    }
}
}

