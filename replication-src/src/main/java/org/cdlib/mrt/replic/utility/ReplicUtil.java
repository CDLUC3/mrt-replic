/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.replic.utility;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.util.Properties;
import org.cdlib.mrt.cloud.ManifestStr;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.DPRFileDB;

/**
 *
 * @author loy
 */
public class ReplicUtil 
{
    
    
    
    public static String versionMap2String(VersionMap map)
        throws TException
    {
        
        try {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream(5000);
            ManifestStr.buildManifest(map, outStream);
            String outS = outStream.toString("utf-8");
            return outS;
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
        
    }
}
