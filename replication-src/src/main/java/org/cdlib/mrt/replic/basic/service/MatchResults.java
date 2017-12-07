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
import java.util.Properties;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.replic.basic.service.MatchObjectState;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.StateInf;

/**
 * Run fixity
 * @author dloy
 */
public class MatchResults
        implements StateInf
{

    protected static final String NAME = "MatchResults";
    protected static final String MESSAGE = NAME + ": ";

    protected Boolean versionCountException = null;
    protected Boolean manifestVersionFormException = null;
    protected Boolean objectMatch = null;
    protected Boolean objectFoundInv = null;
    protected Boolean fieldSizeMatch = null;
    protected Boolean fieldContentMatch = null;
    protected Boolean validateManifestVersions = null;
    protected Boolean validateManifestFields = null;
    protected List<String> errors = new ArrayList();
    
    
    
    public static MatchResults MatchResults()
    {
        return new MatchResults();
    }
    
    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer();
        buf.append(MESSAGE + header);
        buf.append("Tests:\n"
                + " - fieldContentMatch:" + fieldContentMatch + "\n"
                + " - validateManifestVersions:" + validateManifestVersions + "\n"
                + " - validateManifestFields:" + validateManifestFields + "\n"
                + "--------------------------------------------\n"
        );
        buf.append("Results:\n"
                + " - objectMatch:" + objectMatch + "\n"
                + " - objectFoundInv:" + objectFoundInv + "\n"
                + " - versionCountException:" + versionCountException + "\n"
                + " - manifestVersionFormException:" + manifestVersionFormException + "\n"
                + "--------------------------------------------\n"
        );
        buf.append("Error:\n");
        for (int i=0; i<errors.size(); i++) {
            String err = errors.get(i);
            buf.append("Err[" + i + "]:" + errors.get(i));
        }
        buf.append("--------------------------------------------\n");
        
        return buf.toString();
        
    }
          

    public Boolean getVersionCountException() {
        return versionCountException;
    }

    public void setVersionCountException(Boolean versionCountException) {
        this.versionCountException = versionCountException;
    }

    public Boolean getManifestVersionFormException() {
        return manifestVersionFormException;
    }

    public void setManifestVersionFormException(Boolean manifestVersionFormException) {
        this.manifestVersionFormException = manifestVersionFormException;
    }

    public Boolean getObjectMatch() {
        return objectMatch;
    }

    public void setObjectMatch(Boolean objectMatch) {
        this.objectMatch = objectMatch;
    }

    public Boolean getObjectFoundInv() {
        return objectFoundInv;
    }

    public void setObjectFoundInv(Boolean objectFoundInv) {
        this.objectFoundInv = objectFoundInv;
    }

    public Boolean getFieldSizeMatch() {
        return fieldSizeMatch;
    }

    public void setFieldSizeMatch(Boolean fieldSizeMatch) {
        this.fieldSizeMatch = fieldSizeMatch;
    }

    public Boolean getFieldContentMatch() {
        return fieldContentMatch;
    }

    public void setFieldContentMatch(Boolean fieldContentMatch) {
        this.fieldContentMatch = fieldContentMatch;
    }
    
    public void addError(String msg)
    {
        if (StringUtil.isAllBlank(msg)) return;
        errors.add(msg);
    }
    
    public List<String> getErrors()
    {
        return errors;
    }
    
    public int errorsSize()
    {
        return errors.size();
    }

    public Boolean getValidateManifestVersions() {
        return validateManifestVersions;
    }

    public void setValidateManifestVersions(Boolean validateManifestVersions) {
        this.validateManifestVersions = validateManifestVersions;
    }

    public Boolean getValidateManifestFields() {
        return validateManifestFields;
    }

    public void setValidateManifestFields(Boolean validateManifestFields) {
        this.validateManifestFields = validateManifestFields;
    }
    
}

