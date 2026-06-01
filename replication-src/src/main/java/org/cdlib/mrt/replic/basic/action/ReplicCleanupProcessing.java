/******************************************************************************
Copyright (c) 2005-2026, Regents of the University of California
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
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FixityStatusType;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.log.utility.AddStateEntryGen;
import org.cdlib.mrt.replic.basic.service.ReplicationPropertiesState;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.replic.utility.ReplicOwn;
import org.cdlib.mrt.utility.TException;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Run fixity
 * @author dloy
 */
public class ReplicCleanupProcessing
        extends ReplicActionAbs
        implements Runnable
{

    protected static final String NAME = "FixityCleanupProcessing";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;

    
    protected enum CleanupStatus {missing, exception, error, keyerror, tested, notset};
    protected DPRFileDB db = null;
    

    protected Properties [] rows = null;
    protected Properties cleanupProperties = null;
    protected String msg = null;
    protected int processCnt = 0;
    private static final Logger log4j = LogManager.getLogger();
    
    public static ReplicCleanupProcessing getReplicCleanupProcessing(
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        return new ReplicCleanupProcessing(db, logger);
    }
    
    protected ReplicCleanupProcessing(
            DPRFileDB db,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.db = db;
    }


    @Override
    public void run()
    {
        process();

    }
    
    public JSONObject process()
    {
        Connection ownConnect = null;
        LinkedList<Long> replicList = null;
        try {
                replicList = doCleanup(ownConnect);
                log4j.debug("process:" 
                        + " - ids.size:" + replicList.size()
                        + " - auditList.size:" + replicList.size()
                );
                
            
        } catch (TException tex) {
            log4j.debug(MESSAGE, tex);
            setException(tex);
            

        } catch (Exception ex) {
            log4j.debug(MESSAGE, ex);
            setException(ex);

        } finally {
            try {
                ownConnect.close();
            } catch (Exception ex) { }
            return addLog4j(replicList);
        }

    }
    
    public LinkedList<Long> doCleanup(Connection ownConnect)
        throws Exception
    {
        Properties [] rows = null;
        LinkedList<Long> cleanupIds = new LinkedList<>();
        try {
            ownConnect = testConnection(ownConnect); 
            cleanupIds = ReplicOwn.doCleanupReplic(
                ownConnect, 
                logger);
            log4j.debug("run entered:" + cleanupIds.size());
            return cleanupIds;
            
        } catch (Exception ex) {
            log4j.debug(MESSAGE, ex);
            throw ex;

        }

    }

    protected Connection testConnection(Connection inConnection)
        throws Exception
    {
        Connection outConnection = inConnection;
        try {
            if (inConnection == null) {
                outConnection = db.getConnection(false);
            } else if (!inConnection.isValid(1)) {
                inConnection.close();
                outConnection = db.getConnection(false);
            }
            return outConnection;
            
        } catch (Exception ex) {
            throw ex;
        }
    }
    

    protected JSONObject addLog4j(LinkedList<Long> replicList)
    {
        JSONObject cleanupJson = new JSONObject();
        try {
            JSONArray jsonArr = new JSONArray();
            for (Long auditid : replicList) {
                jsonArr.put(auditid);
            }
            cleanupJson.put("count", replicList.size());
            cleanupJson.put("ids", jsonArr);
            if (exception != null) {
                cleanupJson.put("status", "exception:" + exception.toString());
            } else {
                cleanupJson.put("status", "ok");
            }
            log4j.trace("addLog4j:" + cleanupJson.toString(2));
            AddStateEntryGen.addLogStateEntry("info", "replicCleanupStat", cleanupJson);
            
            log4j.trace("cleanupJson--" + cleanupJson.toString(2));
            
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception addLog4j:" + ex);
            log4j.debug(MESSAGE + "Exception addLog4j:" + ex.toString(), ex);
            cleanupJson.put("addlo4jEx", ex.toString());
            
        } finally {
            return cleanupJson;
        }
    }
    
    protected void log(String msg)
    {
        if (!DEBUG) return;
        System.out.println(MESSAGE + msg);
    }
}


