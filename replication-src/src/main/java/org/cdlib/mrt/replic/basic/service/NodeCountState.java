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

import org.cdlib.mrt.core.FixityStatusType;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.replic.basic.service.ReplicationScheme;

/**
 * Format container class for Fixity Service
 * @author dloy
 */
public class NodeCountState
        implements StateInf
{
    private static final String NAME = "NodeCountState";
    private static final String MESSAGE = NAME + ": ";
    
    protected Long nodeNumber = null;
    protected Long objectCount = null;
    protected Long componentCount = null;

    public NodeCountState() { }

    public NodeCountState(Long nodeNumber)
        throws TException
    {
        this.nodeNumber = nodeNumber;
    }

    public Long getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(Long nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public void setNodeNumber(String nodeNumberS) {
        if (StringUtil.isAllBlank(nodeNumberS)) this.nodeNumber = null;
        else this.nodeNumber = Long.parseLong(nodeNumberS);
    }

    public Long getObjectCount() {
        return objectCount;
    }

    public void setObjectCount(Long objectCount) {
        this.objectCount = objectCount;
    }

    public void setObjectCount(String objectCountS) {
        if (StringUtil.isAllBlank(objectCountS)) this.objectCount = null;
        else this.objectCount = Long.parseLong(objectCountS);
    }

    public Long getComponentCount() {
        return componentCount;
    }

    public void setComponentCount(Long componentCount) {
        this.componentCount = componentCount;
    }

    public void setComponentCount(String componentCountS) {
        if (StringUtil.isAllBlank(componentCountS)) this.componentCount = null;
        else this.componentCount = Long.parseLong(componentCountS);
    }

    
    
    public void setFromProp(Properties prop)
        throws TException
    {
        //System.out.println(PropertiesUtil.dumpProperties("setFromProp", prop));
        Long objectCnt = getLong(prop, "count(inv_nodes_inv_objects.inv_node_id)");
        Long componentCnt = getLong(prop, "count(inv_audits.inv_node_id)");
        if (objectCnt != null) {
            setObjectCount(objectCnt);
        }
        if (componentCnt != null) {
            setComponentCount(componentCnt);
        }
    }
    
    
    
    public static Long getLong(Properties prop, String key)
    {
        if (StringUtil.isAllBlank(key)) return null;
        String valueS = prop.getProperty(key);
        if (StringUtil.isAllBlank(valueS)) return null;
        return Long.parseLong(valueS);
    }
}
