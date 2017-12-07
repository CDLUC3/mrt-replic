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
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.inv.content.InvObject;
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
public class MatchStore
        extends ReplicActionAbs
{

    protected static final String NAME = "MatchStore";
    protected static final String MESSAGE = NAME + ": ";

    protected MatchResults results = null;
    //protected int sourceNode = 0;
    //protected int targetNode = 0;
    //protected Identifier objectID = null;
    //protected String storageBase = null;
    protected VersionMap sourceMap = null;
    protected VersionMap targetMap = null;
    protected int sourceCurrent = 0;
    protected int targetCurrent = 0;
    
    public static MatchStore getMatchStore(
            LoggerInf logger)
        throws TException
    {
        return new MatchStore( logger);
    }
    
    protected MatchStore(
            LoggerInf logger)
        throws TException
    {
        super(logger);
        results = new MatchResults();
        validate();
    }
    
    protected void validate()
        throws TException
    {
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger not supplied");
        }
    }

    public MatchResults process(VersionMap sourceMap, VersionMap targetMap)
        throws TException
    {
        try {
            this.sourceMap = sourceMap;
            this.targetMap = targetMap;
            sourceCurrent = sourceMap.getCurrent();
            targetCurrent = targetMap.getCurrent();
            validateManifestVersions();
            if (results.errorsSize() > 0) return results;
            validateManifestFields();
            if (results.errorsSize() > 0) {
                results.setObjectMatch(false);
            } else {
                results.setObjectMatch(true);
            }
            return results;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    protected void validateManifestVersions()
        throws TException
    {
        
        results.setValidateManifestVersions(false);
        if (sourceCurrent == 0) {
            results.setVersionCountException(true);
            results.setObjectMatch(false);
            setError(MESSAGE + "sourceCurrent zero"
                    + " - sourceCurrent=" + sourceCurrent
                    + " - targetCurrent=" + targetCurrent
            );
        }
        if (targetCurrent == 0) {
            results.setVersionCountException(true);
            results.setObjectMatch(false);
            setError(MESSAGE + "targetCurrent zero"
                    + " - sourceCurrent=" + sourceCurrent
                    + " - targetCurrent=" + targetCurrent
            );
        }
        if (sourceCurrent > targetCurrent) {
            results.setVersionCountException(true);
            results.setObjectMatch(false);
            setError(MESSAGE + "sourceCurrent > targetCurrent"
                    + " - sourceCurrent=" + sourceCurrent
                    + " - targetCurrent=" + targetCurrent
            );
        }
        results.setValidateManifestVersions(true);
    }
    
    protected void setError(String msg)
        throws TException
    {
        
        results.addError(msg);
    }
    
    protected void validateManifestFields()
        throws TException
    {
        results.setValidateManifestFields(false);
        for (int versionID=1; versionID <= sourceCurrent; versionID++) { 
            List<FileComponent> components = getComponents(sourceMap, versionID);
            List<FileComponent> tcomponents = getComponents(targetMap, versionID);
            //List<FileComponent> components = sourceMap.getVersionComponents(versionID);
            //List<FileComponent> tcomponents = targetMap.getVersionComponents(versionID);
            if (!sourceMap.isMatchComponents(components, tcomponents)) {
                //dumpMap("source", sourceMap);
                //dumpMap("target", targetMap);
                System.out.println("ERROR VERSION:" + versionID);
                dumpComponents("source", components);
                dumpComponents("target", tcomponents);
                results.setManifestVersionFormException(true);
                setError(MESSAGE + "invalid match manifest version content"
                        + " - version=" + versionID
                );
                return;
            }
            System.out.println("test version:" + versionID);
        }
        results.setValidateManifestFields(true);
    }
    
    protected List<FileComponent> getComponents(VersionMap map, int versionID)
        throws TException
    {
        try {
            
            ManInfo verManInfo = map.getVersionInfo(versionID);
            ComponentContent componentContent = verManInfo.components;
            return componentContent.getFileComponents();
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static void dumpMap(String header, VersionMap map)
        throws TException
    {
        try {
            System.out.println("********************DUMP " + header);
            if (map == null) {
                System.out.println("Map null");
                return;
            }
            System.out.println(map.dump("***Map header:" + header));
            for (int ver=1; ver <= map.getCurrent(); ver++) {
                ManInfo verManInfo = map.getVersionInfo(ver);
                ComponentContent componentContent = verManInfo.components;
                List<FileComponent> components = componentContent.getFileComponents();
                dumpComponents("Dump Map version=" + ver, components);
            }
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static  void dumpComponents(String header, List<FileComponent> components)
        throws TException
    {
        try {
            System.out.println("***" + header + "***");
            if (components == null) {
                System.out.println("Components null");
                return;
            }
            for (int i=0; i<components.size(); i++) {
                FileComponent component = components.get(i);
                String dumpComponent = component.dump("component(" + i + ")");
                System.out.println(dumpComponent);
            }
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    public MatchResults getResults() {
        return results;
    }
    
}

