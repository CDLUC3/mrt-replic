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



import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.Properties;

import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.replic.basic.action.ReplicCleanup;
import org.cdlib.mrt.replic.basic.action.ReplicEmailWrapper;
import org.cdlib.mrt.replic.basic.service.ReplicationPropertiesState;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.replic.basic.service.ReplicationRunInfo;
import org.cdlib.mrt.utility.LoggerAbs;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;

/**
 * Fixity Service
 * @author  dloy
 */

public class ReplicCleanupExecutor
{
    private static final String NAME = "ReplicCleanupTestMail";
    private static final String MESSAGE = NAME + ": ";
    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = true;
    private static final boolean THREADDEBUG = false;

 

    public static void main(String args[])
    {

        TFrame tFrame = null;
        DPRFileDB db = null;
        
        Connection connection = null;
        try {
            String propertyList[] = {
                "resources/ReplicLogger.properties",
                "resources/ReplicCleanupTest.properties"};
            tFrame = new TFrame(propertyList, "InvLoad");
            Properties setupProp  = tFrame.getProperties();
            //LoggerInf logger = new TFileLogger("testFormatter", 10, 10);
            LoggerInf logger = new TFileLogger("testFormatter", 1, 1);
            String infos = setupProp.getProperty("ReplicationService");
            if (infos == null) {
                throw new TException.INVALID_OR_MISSING_PARM("missing ReplicatinService");
            }
            File replicationInfoF = new File(infos + "/replic-info.txt");
            if (!replicationInfoF.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("missing properties:" + replicationInfoF.getCanonicalFile());
            }
            FileInputStream fis = new FileInputStream(replicationInfoF);
            Properties serviceProperties = new Properties();
            serviceProperties.load(fis);
            
            setupProp.putAll(serviceProperties);
            db = new DPRFileDB(logger, setupProp);
            //db = null;
            System.out.println(PropertiesUtil.dumpProperties("Cleanup props", setupProp));
            NodeIO nodeIO = new NodeIO("nodes-dev-glacier", logger);
            
            System.out.println(PropertiesUtil.dumpProperties("ReplicCleanupTestMail", setupProp));
            ReplicCleanup rc = ReplicCleanup.getReplicCleanup(setupProp, nodeIO, db, logger);
            System.out.println("from:" + rc.getEmailFrom());
            System.out.println("to:" + rc.getEmailTo());
            System.out.println("subject:" + rc.getEmailSubject());
            System.out.println("msg:" + rc.getEmailMsg());
            if (false) return;
            ReplicEmailWrapper cleanuptWrapper = new ReplicEmailWrapper(
                rc,
                true,
                rc.getEmailTo(),
                rc.getEmailFrom(),
                rc.getEmailSubject(),
                rc.getEmailMsg(),
                "xml",
                db,
                setupProp,
                logger);
            ExecutorService threadExecutor = Executors.newFixedThreadPool( 1 );
            threadExecutor.execute(cleanuptWrapper ); // start task1
            threadExecutor.shutdown();
            Thread.sleep(3000);
            //sqlReportWrapper.run();

        } catch(Exception e) {
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        } finally {
            try {
                connection.close();
            } catch (Exception ex) { }
        }
    }

    public static String formatIt(
            LoggerInf logger,
            StateInf responseState)
    {
        try {
           FormatterInf anvl = FormatterAbs.getJSONFormatter(logger);
           ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
           PrintStream  stream = new PrintStream(outStream, true, "utf-8");
           anvl.format(responseState, stream);
           stream.close();
           outStream.close();
           byte [] bytes = outStream.toByteArray();
           String retString = new String(bytes, "UTF-8");
           return retString;

        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            System.out.println("Trace:" + StringUtil.stackTrace(ex));
            return null;
        } finally {

        }
    }

    
    

}
