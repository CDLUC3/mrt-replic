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

import java.sql.Connection;
import org.cdlib.mrt.inv.action.*;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.http.HttpResponse;

import org.cdlib.mrt.inv.content.InvFile;
import org.cdlib.mrt.inv.content.InvAudit;
import org.cdlib.mrt.inv.content.InvNode;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.service.Version;
import org.cdlib.mrt.inv.service.VersionsState;
import org.cdlib.mrt.inv.service.VFile;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;

/**
 * Run fixity
 * @author dloy
 */
public class DeleteStore
        extends ReplicActionAbs
{

    protected static final String NAME = "DeleteStore";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean DUMPTALLY = false;
    protected static final boolean EACHTALLY = true;
    
    protected ReplicNodesObjects deleteInvNodeObject = null;
    protected InvNode deleteInvNode = null;
    protected InvObject deleteInvObject = null;
    protected String storeURL = null;
    protected Properties storeResponse = null;
    protected NodeIO nodes = null;
    protected NodeService service = null;
    
    public static DeleteStore getDeleteStore(
            ReplicNodesObjects deleteInvNodeObject,
            Connection connection,
            NodeIO nodes,
            LoggerInf logger)
        throws TException
    {
        return new DeleteStore(deleteInvNodeObject, connection, nodes, logger);
    }
    
    protected DeleteStore(
            ReplicNodesObjects deleteInvNodeObject,
            Connection connection,
            NodeIO nodes,
            LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        try {
            if (deleteInvNodeObject.getRole() != Role.secondary) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                        + "Only secondary nodes supported for delete:" + deleteInvNodeObject.getRole());
            }
            this.deleteInvNodeObject = deleteInvNodeObject;
            this.deleteInvNode = InvDBUtil.getNodeFromId(deleteInvNodeObject.getNodesid(), connection, logger);
            this.deleteInvObject = InvDBUtil.getObject(deleteInvNodeObject.getObjectsid(), connection, logger);
            this.nodes = nodes;
            this.service = NodeService.getNodeService(nodes, this.deleteInvNode.getNumber(), logger);
            
            deleteInvNodeObject.setDeleteStore(false);
            
        } catch (Exception ex) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ex2) { }
            
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            else throw new TException(ex);
        }
    }
        
    protected void buildStoreURL()
        throws TException
    {
        try {
            String baseURL = deleteInvNode.getBaseURL();
            long nodeS = deleteInvNode.getNumber();
            String encObjectID = URLEncoder.encode(deleteInvObject.getArk().getValue(), "utf-8");
            storeURL = baseURL + "/content/" + nodeS  + '/' + encObjectID;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
   
    public void process()
        throws TException
    {
        try {
            deleteContent();
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
    
    protected void deleteContent() 
        throws TException
    {
        try {
            deleteInvNodeObject.setDeleteStore(false);
            if (service == null) {
                deleteContentUrl();
            } else {
                deleteContentInv();
            }
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }
    
    protected void deleteContentUrl() 
        throws TException
    {
        try {
            buildStoreURL();
            log( 10,"deleteContent url=" + storeURL);
            //HttpResponse HTTPUtil.postHttpResponse(String requestURL, Properties prop, int timeout)
            Properties httpProp = new Properties();
            httpProp.setProperty("t", "xml");
            HttpResponse response = HTTPUtil.deleteHttpResponse(storeURL, 3600000);
            storeResponse = HTTPUtil.response2Property(response);
            String statusS = storeResponse.getProperty("response.status");
            int status = Integer.parseInt(statusS);
            if (status == 404) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("Missing:" + storeURL);
            }
            if ((status < 200) || (status >= 300)) {
                throw new TException.REMOTE_IO_SERVICE_EXCEPTION("Storage copy fails" 
                        + " - status=" + status
                        + " - url=" + storeURL
                        + " - response.line=" + storeResponse.getProperty("response.line")
                        );
            }
            deleteInvNodeObject.setDeleteStore(true);
            log( 10,PropertiesUtil.dumpProperties("copyContent",storeResponse));
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }
    
    protected void deleteContentInv() 
        throws TException
    {
        try {
            int deleteCntStore = 0;
            Versions runVersions = Versions.getVersions(deleteInvObject.getArk(), null, connection, logger);
            VersionsState versionState = runVersions.process();
            Set<String> keys = versionState.retrieveKeys();
            if (keys.size() > 0) {
                deleteManifest(deleteInvObject.getArk());
                deleteCntStore++;
            }
            for (String key : keys) {
                deleteKey(key);
                deleteCntStore++;
            }
            deleteInvNodeObject.setStoreDeleteCount(deleteCntStore);
            deleteInvNodeObject.setDeleteStore(true);
            
            log( 10,PropertiesUtil.dumpProperties("deleteContentInv",storeResponse));
                    
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }
    
    protected void deleteKey(String key) 
        throws TException
    {
        try {
            CloudResponse response = service.deleteObject(key);
            if (response.getException() != null) {
                throw response.getException();
            }
            log( 10,"Key deleted:" + key);
           
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }
    
    protected void deleteManifest(Identifier objectID) 
        throws TException
    {
        try {
            CloudResponse response = service.deleteManifest(objectID);
            if (response.getException() != null) {
                throw response.getException();
            }
            log( 10,"Manifest delete:" + objectID.getValue());
           
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
        
    }


    public Properties getStoreResponse() {
        return storeResponse;
    }

    public void setStoreResponse(Properties storeResponse) {
        this.storeResponse = storeResponse;
    }
    
}