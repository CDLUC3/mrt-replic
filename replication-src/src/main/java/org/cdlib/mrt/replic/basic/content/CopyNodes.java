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
*********************************************************************/

package org.cdlib.mrt.replic.basic.content;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author DLoy
 */
public class CopyNodes {
    
    protected static final String NAME = "CopyNodes";
    protected static final String MESSAGE = NAME + ": ";
    
    protected HashMap<Long,CopyNode> map = new HashMap<>();
    protected LoggerInf logger = null;
    
    public CopyNodes(LoggerInf logger) 
        throws TException
    {
        try {
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "log missing");
            }
            this.logger = logger;
            CopyNode test = new CopyNode();
            InputStream propStream =  test.getClass().getClassLoader().
                    getResourceAsStream("resources/SDSC-S3.properties");
            if (propStream == null) {
                System.out.println("Unable to find resource");
                return;
            }
            Properties cloudProp = new Properties();
            cloudProp.load(propStream);
            addMap(cloudProp);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM ("Unable to locate:esources/SDSC-S3.properties:" + ex);
        }
    }
    
    public CopyNodes(Properties cloudProp, LoggerInf logger) 
        throws TException
    {
        try {
            if (logger == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "log missing");
            }
            this.logger = logger;
            addMap(cloudProp);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM ("Unable to locate:esources/SDSC-S3.properties:" + ex);
        }
    }
    
    private void addMap(Properties cloudProp)
        throws TException
    {
        try {
            for(int i=1; true; i++) {
                String line = cloudProp.getProperty("node." + i);
                if (line == null) break;
                addMapEntry(cloudProp, line);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM ("Unable to locate:esources/SDSC-S3.properties:" + ex);
        }
        
    }
    
    protected void addMapEntry(Properties cloudProp, String line) 
        throws TException
    {
        try {
            String[] parts = line.split("\\s*\\|\\s*");
            if (parts.length != 2) {
                throw new TException.INVALID_OR_MISSING_PARM("addMapENtry requires 2 parts:" + line);
            }
            Long nodeNumber = Long.parseLong(parts[0]);
            CloudStoreInf service = null;
            if (parts[1].contains("/")) {
                service = PairtreeCloud.getPairtreeCloud(true, logger);
            } else {
                service = OpenstackCloud.getOpenstackCloud(cloudProp, logger);
            }
            CopyNode copyNode = new CopyNode(service, nodeNumber, parts[1]);
            map.put(nodeNumber, copyNode);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM ("Unable to locate:esources/SDSC-S3.properties:" + ex);
        }
        
    }
    
    public CopyNode getCopyNode(long nodeNumber) 
    {
        return map.get(nodeNumber);
    }
    
    public static class CopyNode {
        public CloudStoreInf service = null;
        public Long nodeNumber = null;
        public String container = null;
        public CopyNode() { }
        public CopyNode(
            CloudStoreInf service,
            Long nodeNumber,
            String container
        ) {
            this.service = service;
            this.nodeNumber = nodeNumber;
            this.container = container;
        }
    }
    
    
}
