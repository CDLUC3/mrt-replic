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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Properties;

import org.cdlib.mrt.core.FixityStatusType;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.replic.basic.service.NodeCountState;

/**
 * Format container class for Fixity Service
 * @author dloy
 */
public class NodeCountStates
        implements StateInf
{
    private static final String NAME = "NodeCountStates";
    private static final String MESSAGE = NAME + ": ";
    
    protected HashMap<Long, NodeCountState> nodeCountMap = new HashMap<>();
    
    protected Connection connection = null;
    protected LoggerInf logger = null;

    public NodeCountStates(Connection connection, LoggerInf logger) 
    { 
        this.connection = connection;
        this.logger = logger;
    }
    
    public ArrayList<NodeCountState> build()
        throws TException
    {
        try {
            String sql = sqlObjectCnt();
            Properties [] props = DBUtil.cmd(connection, sql, logger);
            if ((props == null) || (props.length != 1)) {
                System.out.println("WARNING: objects empty");
                return null;
            }
            setObject(props);
            sql = sqlComponentCnt();
            props = DBUtil.cmd(connection, sql, logger);
            if (props == null) {
                System.out.println("WARNING: components empty");
                return null;
            }
            setComponent(props);
            return getNodeStates();

        } catch (Exception ex) {
            System.out.println("WARNING: build exception:" + ex);
            return null;
        }
    }

    protected void setObject(Properties [] props)
        throws TException
    {
        for (Properties prop : props) {
            Long nodeNum = getNode(prop);
            NodeCountState nodeCountState = nodeCountMap.get(nodeNum);
            if (nodeCountState == null) {
                nodeCountState = new NodeCountState(nodeNum);
                nodeCountMap.put(nodeNum, nodeCountState);
            }
            nodeCountState.setFromProp(prop);
        }
    }

    protected void setComponent(Properties [] props)
        throws TException
    {
        for (Properties prop : props) {
            Long nodeNum = getNode(prop);
            NodeCountState nodeCountState = nodeCountMap.get(nodeNum);
            if (nodeCountState == null) {
                continue;
            }
            nodeCountState.setFromProp(prop);
        }
    }
    
    protected ArrayList<NodeCountState> getNodeStates()
    {
        ArrayList<NodeCountState> nodeCounts = new ArrayList<>();
        nodeCounts.addAll(nodeCountMap.values());
        return nodeCounts;
    }
    
    public static Long getNode(Properties prop)
        throws TException
    {
        Long nodeNumber = getLong(prop, "number");
        if (nodeNumber == null)  {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "node number missing");
        }
        return nodeNumber;
    }
    
    public static Long getLong(Properties prop, String key)
    {
        if (StringUtil.isAllBlank(key)) return null;
        String valueS = prop.getProperty(key);
        if (StringUtil.isAllBlank(valueS)) return null;
        return Long.parseLong(valueS);
    }
    
    public static String sqlObjectCnt()
    {
        return
            "select inv_nodes.number, count(inv_nodes_inv_objects.inv_node_id) "
            + "from inv_nodes, inv_nodes_inv_objects "
            + "where inv_nodes.id = inv_nodes_inv_objects.inv_node_id "
            + "and inv_nodes_inv_objects.role = 'secondary' "
            + "group by inv_nodes_inv_objects.inv_node_id"
            + ";";
    }
    
    public static String sqlComponentCnt()
    {
        return
            "select inv_nodes.number, count(inv_audits.inv_node_id)  "
            + "from inv_nodes, inv_audits "
            + "where inv_audits.inv_node_id = inv_nodes.id "
            + "group by inv_audits.inv_node_id"
            + ";";
    }
}
