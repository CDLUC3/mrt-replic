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

package org.cdlib.mrt.replic.basic.service;
import java.util.Properties;

import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

import java.util.concurrent.atomic.AtomicLong;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.List;

import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.ServiceStatus;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.content.ContentAbs;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.inv.utility.InvUtil;
import org.cdlib.mrt.replic.basic.service.NodeCountStates;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.inv.utility.DPRFileDB;

/**
 * Fixity build Service State
 * @author  dloy
 */

public class ReplicationServiceStateManager
{
    private static final String NAME = "ReplicationServiceStateManager";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;

    
    //protected File replicationInfoF = null;
    protected LoggerInf logger = null;
    protected ReplicationServiceState serviceState = null;


    public static ReplicationServiceStateManager getReplicationServiceStateManager(
            ReplicationConfig replicConfig)
        throws TException
    {
        return new ReplicationServiceStateManager(replicConfig);
    }
    
    protected ReplicationServiceStateManager(ReplicationConfig replicConfig)
        throws TException
    {
        try {
            if (replicConfig == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "service properties do not exist:");
            }
            this.logger = replicConfig.getLogger();
            serviceState = replicConfig.getServiceState();


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }


    public ReplicationServiceState getReplicationServiceState(Connection connection, ServiceStatus inStatus)
        throws TException
    {
        try {
            ReplicationServiceState returnState = new ReplicationServiceState(serviceState);
            returnState.setStatus(inStatus);
            if (DEBUG) {
                boolean dbRunning = false;
                if (connection != null) dbRunning = true;
                System.out.println(MESSAGE + "getFixityServiceState"
                    + " - dbRunning:" + dbRunning
                    );
            }
            if (connection != null) addDBContent(connection, returnState);
            return returnState;

        } catch (Exception ex) {
            throw new TException(ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) { }
            }
        }
    }

    public ReplicationServiceState getReplicationServiceStatus(ServiceStatus inStatus)
        throws TException
    {
        try {
            ReplicationServiceState returnState = new ReplicationServiceState(serviceState);
            returnState.setStatus(inStatus);
            return returnState;

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public ReplicationServiceState getReplicationServiceStatus(
            ServiceStatus inStatus,
            ReplicationRunInfo runInfo)
        throws TException
    {
        try {
            ReplicationServiceState returnState = new ReplicationServiceState(serviceState);
            setRunStatus(returnState, inStatus, runInfo);
            return returnState;

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public void addDBContent(Connection connection, ReplicationServiceState state)
        throws TException
    {
        try {
            addDates(connection, state);
            addNodeCounts(connection, state);

            return;
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public void addDates(
            Connection connection,
            ReplicationServiceState state)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "addDates");
        try {
            
            String sql = "select max(replicated) from inv_nodes_inv_objects;";
            DateState recent = getDateState(sql,"max(replicated)",connection);
            state.setLastModified(recent);
            return;

        } catch (Exception ex) {
            System.out.println("WARNING: addLastIteration exception:" + ex);
            return;
        }
    }



    public void addNodeCounts(
            Connection connection,
            ReplicationServiceState state)
        throws TException
    {
        try {
            NodeCountStates nodeCountStates = new NodeCountStates(connection, logger);
            List<NodeCountState> nodeCounts = nodeCountStates.build();
            state.setNodeCounts(nodeCounts);

        } catch (Exception ex) {
            System.out.println("WARNING: addLastIteration exception:" + ex);
            return;
        }
    }

    protected long getNum(
            String sql,
            String key,
            Connection connection)
        throws TException
    {
        try {
            if (connection == null) return 0;
            Properties [] props = DBUtil.cmd(connection, sql, logger);
            if ((props == null) || (props.length != 1)) {
                System.out.println("WARNING: getNum empty");
                return 0;
            }
            //System.out.println(PropertiesUtil.dumpProperties("addCount", props[0]));
            String countS = props[0].getProperty(key);
            if (StringUtil.isEmpty(countS)) {
                System.out.println("WARNING: " + key + " not found");
                return 0;
            }

            return Long.parseLong(countS);

        } catch (Exception ex) {
            System.out.println("WARNING: getNum exception:" + ex);
            return 0;
        }
    }

    protected DateState getDateState(
            String sql,
            String key,
            Connection connection)
        throws TException
    {
        try {
            if (connection == null) return null;
            Properties [] props = DBUtil.cmd(connection, sql, logger);
            if ((props == null) || (props.length != 1)) {
                System.out.println("WARNING: getNum empty");
                return null;
            }
            //System.out.println(PropertiesUtil.dumpProperties("addCount", props[0]));
            String dateS = props[0].getProperty(key);
            if (StringUtil.isEmpty(dateS)) {
                System.out.println("WARNING: " + key + " not found");
                return  null;
            }

            return InvUtil.setDBDate(dateS);

        } catch (Exception ex) {
            System.out.println("WARNING: getNum exception:" + ex);
            return  null;
        }
    }
    
    public String getNodePath()
    {
        return serviceState.getNodePath();
    }
    
    public ReplicationServiceState retrieveBasicServiceState()
    {
        return serviceState;
    }


    public static void main(String args[])
    {

        TFrame tFrame = null;
        DPRFileDB db = null;
        try {
            ReplicationConfig replicConfig = ReplicationConfig.useYaml();
            //db = replicConfig.startDB();
            ReplicationServiceStateManager manager = getReplicationServiceStateManager
                    (replicConfig);
            LoggerInf logger = replicConfig.getLogger();
            db = replicConfig.startDB();
            Connection connect = db.getConnection(true);
            ReplicationServiceState state = manager.getReplicationServiceState(connect, ServiceStatus.running);
            
            FormatterInf anvl = FormatterAbs.getANVLFormatter(logger);
            String format = formatIt(anvl, state);
            System.out.println("OUTPUT:" + format);

        } catch(Exception e) {
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            try {
                db.shutDown();
            } catch (Exception ex) { }
        }
    }

    public static String formatIt(
            FormatterInf formatter,
            StateInf responseState)
    {
        try {
           ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
           PrintStream  stream = new PrintStream(outStream, true, "utf-8");
           formatter.format(responseState, stream);
           stream.close();
           byte [] bytes = outStream.toByteArray();
           String retString = new String(bytes, "UTF-8");
           return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        }
    }

    
    protected void setRunStatus(
            ReplicationServiceState returnState,
            ServiceStatus inStatus,
            ReplicationRunInfo runInfo)
        throws TException
    {
        try {
            returnState.setReplicationProcessing(runInfo.isReplicationProcessing());
            returnState.setRunReplication(runInfo.isRunReplication());
            returnState.setReplicationSQL(runInfo.isReplicationSQL());
            returnState.setStatus(inStatus);
            returnState.setCnt((AtomicLong)runInfo.getCnt());
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return;
        }
        
    }

}
