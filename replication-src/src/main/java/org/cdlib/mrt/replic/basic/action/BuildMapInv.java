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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.inv.content.InvFile;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.MatchResults;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;

/**
 * Run fixity
 * @author dloy
 */
public class BuildMapInv
        extends ReplicActionAbs
{

    protected static final String NAME = "BuildMapInv";
    protected static final String MESSAGE = NAME + ": ";

    protected Identifier objectID = null;
    protected InvObject invObject = null;
    protected List<InvFile> fileList = null;
    protected HashMap<String,String> deltaMap = new HashMap();
    protected ArrayList<InvFile>  files = null;
    protected VersionMap versionMap = null;
    protected Connection connection = null;
    
    public static BuildMapInv getBuildMapInv(
            Identifier objectID,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return new BuildMapInv(objectID, connection, logger);
    }
    
    protected BuildMapInv(
            Identifier objectID,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        super(logger);
        this.objectID = objectID;
        this.connection = connection;
        validate();
    }
    
    protected void validate()
        throws TException
    {
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "objectID not supplied");
        }
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "connection not supplied");
        }
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger not supplied");
        }
        invObject = InvDBUtil.getObject(objectID, connection, logger);
        if (invObject == null) {
            throw new TException.INVALID_OR_MISSING_PARM("Object not found"
                    + " - objectID=" + objectID.getValue()
            );
        }
    }

    public VersionMap process()
        throws TException
    {
        try {
            versionMap = new VersionMap(objectID, logger);
            for (int iver=1; true; iver++) {
                if (!addMap(iver)) break;
            }
            return versionMap;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public boolean addMap(long versionID)
        throws TException
    {
        try {
            InvVersion invVersion = InvDBUtil.getVersion(invObject.getId(), versionID, connection, logger);
            if (invVersion == null) return false;
                
            ArrayList<InvFile>  invFiles = InvDBUtil.getVersionFiles(objectID, versionID, connection, logger);
            if (invFiles == null) {
                throw new TException.INVALID_OR_MISSING_PARM("Files not found"
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + versionID
                );
            }
            
            List<FileComponent> components = getComponents(versionID, invFiles);
            versionMap.addVersion(components);
            return true;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    protected ArrayList<FileComponent> getComponents(long versionID, List<InvFile> invFiles)
        throws TException
    {
        ArrayList<FileComponent> components = new ArrayList(invFiles.size());
        try {
            for (InvFile invFile: invFiles) {
                FileComponent component = invFile.getFileComponent();
                String key = getDeltaKey(versionID, component);
                component.setLocalID(key);
                components.add(component);
            }
            return components;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public VersionMap getVersionMap() {
        return versionMap;
    }
    
    protected String getDeltaKey(long versionID, FileComponent component)
    {
        String manKey = objectID + "|" + versionID + "|" + component.getIdentifier();
        String deltaKey = component.getIdentifier() + "|" + component.getSize() + "|" + component.getMessageDigest();
        //System.out.println("deltaKey=" + deltaKey);
        String testKey = deltaMap.get(deltaKey);
        if (StringUtil.isAllBlank(testKey)) {
            testKey = manKey;
            deltaMap.put(deltaKey, manKey);
        }
        return testKey;
    }
}

