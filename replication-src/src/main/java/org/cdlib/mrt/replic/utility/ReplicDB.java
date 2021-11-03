/*
Copyright (c) 2005-2010, Regents of the University of California
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
**********************************************************/

package org.cdlib.mrt.replic.utility;

import java.sql.Connection;
import java.util.Properties;
import org.cdlib.mrt.core.DateState;
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
public class ReplicDB 
{
    
    
    public static boolean resetReplicatedRetry(
            DPRFileDB db, 
            InvNodeObject primaryNodeObject,
            LoggerInf logger)
    {
        Connection connection = null;
        try {
            for (int i=0; i<3; i++) {
                try {
                    connection = db.getConnection(true);
                    boolean ok = resetReplicated(connection, primaryNodeObject, logger);
                    if (ok) return true;
                    
                } finally {
                    try {
                        connection.close();
                    } catch (Exception ex) { }
                }
                System.out.println("resetBackupRetry:" + i);
                Thread.sleep(15000);
            }
            return false;
            
        } catch (Exception ex) {
            System.out.println("resetBackupRetry exception:" + ex);
            return false;
            
        } 
    }

    /**
     * Reset node object to allow later replication attempt
     * @param primaryNodeObject reset node object
     * @return true=ok, false=fail
     */
    public static boolean resetReplicated(
            Connection connection, 
            InvNodeObject primaryNodeObject,
            LoggerInf logger)
    {
        try {
            if (primaryNodeObject == null) return true;
            if (!connection.isValid(10)) {
                System.out.println(PropertiesUtil.dumpProperties(
                        "INFO: - connection invalid:", primaryNodeObject.retrieveProp()));
                return false;
            }
            String tableName = primaryNodeObject.getDBName();
            Properties prop = primaryNodeObject.retrieveProp();
            Properties newProp = new Properties();
            newProp.setProperty("id", prop.getProperty("id"));
            newProp.setProperty("replicated", prop.getProperty("replicated"));
            DBAdd dbAdd = new DBAdd(connection, logger);
            dbAdd.update(tableName, newProp);
           
            System.out.println(PropertiesUtil.dumpProperties("resetReplicated: replicated reset", newProp)
                    + " - id=" + primaryNodeObject.getId()
                    + " - nodeseq=" + primaryNodeObject.getNodesid()
                    + " - objectseq=" + primaryNodeObject.getObjectsid()
                    + " - replicated=" + primaryNodeObject.getReplicatedDB()
            );
            return true;
            
        } catch (Exception ex) {
            System.out.println("WARNING resetBackup fails:" + ex);
            ex.printStackTrace();
            String exS = ex.toString().toLowerCase();
            if (exS.contains("lock")) {
                System.out.println("Lock failure");
                ex.printStackTrace();
                return false;
            }
            throw new RuntimeException(ex);
            
        }
        
    }
    
    public static InvNodeObject getReplicatedCurrent(
            InvNodeObject primaryNodeObject)
    {
        primaryNodeObject.setReplicatedCurrent();
        return primaryNodeObject;
    }
    
    public static InvNodeObject getReplicatedDayOne(
            InvNodeObject primaryNodeObject)
    {
        //replicated=1970-01-01 00:00:00
        DateState zeroDate = new DateState(28800000);
        DateState inDate = primaryNodeObject.getReplicated();
        if ((inDate != null) && (inDate.getIsoDate().equals(zeroDate.getIsoDate()))) {
            System.out.println("***Date already set:" + primaryNodeObject.getId());
            return null;
        }
        primaryNodeObject.setReplicated(zeroDate);
        return primaryNodeObject;
    }
    
    public static InvNodeObject getReplicatedYear(
            InvNodeObject primaryNodeObject)
    {
        //replicated=1970-12-31T08:00:00-08:00
        DateState yearDate = new DateState(31507200000L);
        DateState inDate = primaryNodeObject.getReplicated();
        if ((inDate != null) && (inDate.getIsoDate().equals(yearDate.getIsoDate()))) {
            System.out.println("***Date already set:" + primaryNodeObject.getId());
            return null;
        }
        primaryNodeObject.setReplicated(yearDate);
        return primaryNodeObject;
    }
    
    public static InvNodeObject getReplicatedNull(
            InvNodeObject primaryNodeObject)
    {
        if (primaryNodeObject.getReplicated() == null) {
            System.out.println("***Date already set:" + primaryNodeObject.getId());
            return null;
        }
        primaryNodeObject.setReplicated((DateState)null);
        return primaryNodeObject;
    }
    
    public static boolean resetReplicatedCurrent(
            Connection connection, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        InvNodeObject invNodeObject = getReplicatedCurrent(primaryNodeObject);
        return resetReplicated(connection, invNodeObject, logger);
    }
    
    public static boolean resetReplicatedDayOne(
            Connection connection, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        InvNodeObject invNodeObject = getReplicatedDayOne(primaryNodeObject);
        return resetReplicated(connection, invNodeObject, logger);
    }
    
    public static boolean resetReplicatedYear(
            Connection connection, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        InvNodeObject invNodeObject = getReplicatedYear(primaryNodeObject);
        return resetReplicated(connection, invNodeObject, logger);
    }
    
    public static boolean resetReplicatedNull(
            Connection connection, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        InvNodeObject invNodeObject = getReplicatedNull(primaryNodeObject);
        return resetReplicated(connection, invNodeObject, logger);
    }
    
    public static boolean resetReplicatedCurrent(
            DPRFileDB db, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        primaryNodeObject.setReplicatedCurrent();
        return resetReplicatedRetry(db, primaryNodeObject, logger);
    }
    
    public static boolean resetReplicatedDayOne(
            DPRFileDB db, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        //replicated=1970-01-01 00:00:00
        DateState zeroDate = new DateState(28800000);
        DateState inDate = primaryNodeObject.getReplicated();
        if ((inDate != null) && (inDate.getIsoDate().equals(zeroDate.getIsoDate()))) {
            System.out.println("***Date already set:" + primaryNodeObject.getId());
            return true;
        }
        primaryNodeObject.setReplicated(zeroDate);
        return resetReplicatedRetry(db, primaryNodeObject, logger);
    }
    
    public static boolean resetReplicatedYear(
            DPRFileDB db, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        //replicated=1970-01-01 00:00:00
        DateState yearDate = new DateState(31507200000L);
        DateState inDate = primaryNodeObject.getReplicated();
        if ((inDate != null) && (inDate.getIsoDate().equals(yearDate.getIsoDate()))) {
            System.out.println("***Date already set:" + primaryNodeObject.getId());
            return true;
        }
        primaryNodeObject.setReplicated(yearDate);
        return resetReplicatedRetry(db, primaryNodeObject, logger);
    }
    
    public static boolean resetReplicatedNull(
            DPRFileDB db, InvNodeObject primaryNodeObject, LoggerInf logger)
    {
        if (primaryNodeObject.getReplicated() == null) {
            System.out.println("***Date already set:" + primaryNodeObject.getId());
            return true;
        }
        primaryNodeObject.setReplicated((DateState)null);
        return resetReplicatedRetry(db, primaryNodeObject, logger);
    }
    
    public static void closeConnect(Connection connection)
    {
        try {
            if (connection == null) return;
            connection.close();
            
        } catch (Exception ex) {  
        
        }
    }
}
