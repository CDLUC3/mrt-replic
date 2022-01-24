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
package org.cdlib.mrt.replic.basic.test;

import org.cdlib.mrt.replic.basic.action.*;
import java.sql.Connection;
import org.cdlib.mrt.inv.action.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.http.HttpResponse;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FixityStatusType;

import org.cdlib.mrt.replic.basic.action.ReplicActionAbs;
import org.cdlib.mrt.inv.content.InvFile;
import org.cdlib.mrt.inv.content.InvAudit;
import org.cdlib.mrt.inv.content.InvNode;
import org.cdlib.mrt.inv.content.InvNode.AccessMode;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.content.InvObject;
import org.cdlib.mrt.inv.content.InvVersion;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.InvDBUtil;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.inv.content.InvStorageScan;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.replic.basic.service.MatchObjectState;
import org.cdlib.mrt.replic.basic.service.ReplicationConfig;
import org.cdlib.mrt.replic.basic.service.ScanManager;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.replic.utility.ReplicDB;
import org.cdlib.mrt.replic.utility.ReplicDBUtil;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.tools.CloudManifestCopyVersion;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONObject;

/**
 * Run fixity
 * @author dloy
 */
public class GetNodeObject
        extends ReplicActionAbs
{

    protected static final String NAME = "NodeObjectMaintTest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
 
    
    protected ReplicationInfo info = null;
    
    protected InvNodeObject primaryNodeObject = null;
    
    public static void main(String args[])
    {
        main_get(args);
    }
    
    
    public static void main_get(String args[])
    {

        int scanNum = 11;
        DPRFileDB db = null;
        try {
            ReplicationConfig config = ReplicationConfig.useYaml();
            config.startDB();
            db = config.getDB();
            LoggerInf logger = new TFileLogger("DoScan", 9, 10);
            Connection connection = db.getConnection(true);
            InvNodeObject nodeObject = InvDBUtil.getNodeObject(15L, 58178L, connection, logger);
            
        } catch(Exception e) {
                e.printStackTrace();
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            try {
                db.shutDown();
            } catch (Exception ex) {
                //System.out.println("db Exception:" + ex);
            }
        }
    }
    
}