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
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.inv.action.Versions;
import org.cdlib.mrt.inv.content.InvFile;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.DeleteOnCloseFileInputStream;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class FileInput
        extends ReplicActionAbs
{

    protected static final String NAME = "FileInput";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;

 
    protected InvObject invObject = null;
    protected List<InvFile> fileList = null;
    protected HashMap<String,String> deltaMap = new HashMap();
    protected ArrayList<InvFile>  files = null;
    protected VersionMap versionMap = null;
    protected Connection connection = null;
    protected NodeIO nodeIO = null;
    
    public static FileInput getFileInput(
            Connection connection,
            NodeIO nodeIO,
            LoggerInf logger)
        throws TException
    {
        return new FileInput(connection, nodeIO, logger);
    }
    
    protected FileInput(
            Connection connection,
            NodeIO nodeIO,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.connection = connection;
        this.nodeIO = nodeIO;
        validate();
    }
    
    protected void validate()
        throws TException
    {
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "connection not supplied");
        }
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger not supplied");
        }
        if (nodeIO == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeIO not supplied");
        }
    }

    public File getFile(Identifier objectID, int versionID, String fileID)
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID not supplied");
        }
        if (versionID < 0) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionID not valid:" + versionID);
        }
        if (StringUtil.isAllBlank(fileID)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileID not supplied");
        }
        if (DEBUG) {
            System.out.println(MESSAGE
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + versionID
                        + " - fileID:" + fileID
            );
        }
        CloudStoreInf service = null;
        String nodeName = null;
        String container = null;
        try {
            File tempFile = FileUtil.getTempFile("temp", ".txt");
            String nodeKey = InvDBUtil.getAccessNodeVersionKey(objectID, versionID, fileID, connection, logger);
            if (nodeKey == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("FileInput: Unable to find:"
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + versionID
                        + " - fileID:" + fileID
                );
            }
            String [] parts = nodeKey.split("#");
            long node = Long.parseLong(parts[0]);
            String key = parts[1];
            if (DEBUG) {
                System.out.println(MESSAGE
                        + " - node:" + node
                        + " - key:" + key
                );
            }
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(node);
            service = accessNode.service;
            container = accessNode.container;
            CloudResponse response = new CloudResponse(container, key);
            service.getObject(container, key, tempFile, response);
            throwException(response.getException(), objectID, versionID, fileID);
            return tempFile;
                
        } catch (TException tex) {
            System.out.println(tex);
            logger.logError(tex.toString(), 1);
            throw tex ;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
            
        } 
    }
    

    public File getManifest(Identifier objectID)
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID not supplied");
        }
        if (DEBUG) {
            System.out.println(MESSAGE
                        + " - objectID:" + objectID.getValue()
            );
        }
        CloudStoreInf service = null;
        String container = null;
        try {
            File tempFile = FileUtil.getTempFile("temp", ".txt");
            long node = InvDBUtil.getAccessNodeNumber(objectID, connection, logger);
            if (node == 0) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("FileInput: Unable to find:"
                        + " - objectID:" + objectID.getValue()
                );
            }
            String key = objectID.getValue() + "|manifest";
            if (DEBUG) {
                System.out.println(MESSAGE
                        + " - node:" + node
                        + " - key:" + key
                );
            }
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(node);
            service = accessNode.service;
            container = accessNode.container;
            CloudResponse response = new CloudResponse(container, key);
            service.getObject(container, key, tempFile, response);
            throwException(response.getException(), objectID);
            return tempFile;
                
        } catch (TException tex) {
            System.out.println(tex);
            logger.logError(tex.toString(), 1);
            throw tex ;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex) ;
            
        } 
    }
    protected void throwException(Exception ex,Identifier objectID, int versionID, String fileID)
        throws TException
    {
        if (ex == null) return;
        if (ex instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("FileInput: no storage found for :"
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + versionID
                        + " - fileID:" + fileID
                );
        } else if (ex instanceof TException) {
            throw (TException)ex;
            
        } else {
            throw new TException(ex);
        }
    }
    protected void throwException(Exception ex,Identifier objectID)
        throws TException
    {
        if (ex == null) return;
        if (ex instanceof TException.REQUESTED_ITEM_NOT_FOUND) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("FileInput: no storage found for :"
                        + " - objectID:" + objectID.getValue()
                );
        } else if (ex instanceof TException) {
            throw (TException)ex;
            
        } else {
            throw new TException(ex);
        }
    }
}