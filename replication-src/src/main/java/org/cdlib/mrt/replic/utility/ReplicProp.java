
/*********************************************************************
    Copyright 2003 Regents of the University of California
    All rights reserved
*********************************************************************/

package org.cdlib.mrt.replic.utility;

import java.util.Properties;
import org.cdlib.mrt.utility.StringUtil;

import org.cdlib.mrt.utility.TException;

/**
 * Properties utility methods
 * @author  dloy
 */

public class ReplicProp
{
    private static final String NAME = "ReplicProp";
    private static final String MESSAGE = NAME + ": ";


    public static String get(Properties prop, String key)
        throws TException
    {
        String retVal = prop.getProperty(key);
        if (StringUtil.isEmpty(retVal)) return null;
        return retVal;
    } 
    
    public static String getEx(Properties prop, String key)
        throws TException
    {
        String retVal = prop.getProperty(key);
        if (StringUtil.isEmpty(retVal)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "prop missing:" + key);
        }
        return retVal;
    }  
    
    public static int getExNumInt(Properties prop, String key)
        throws TException
    {
        String retVal = prop.getProperty(key);
        if (StringUtil.isEmpty(retVal)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "number prop missing:" + key);
        }
        int numVal = 0;
        try {
            numVal = Integer.parseInt(retVal);
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "number prop invalid:" + key);
        }
        return numVal;
    }   
    
    public static Long getNumLong(Properties prop, String key)
        throws TException
    {
        String retVal = prop.getProperty(key);
        if (StringUtil.isEmpty(retVal)) {
            return null;
        }
        long numVal = 0;
        try {
            numVal = Long.parseLong(retVal);
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "number prop invalid:" + key);
        }
        return numVal;
    } 
    
    public static Boolean getBool(Properties prop, String key)
            throws TException
    {
        String retVal = prop.getProperty(key);
        if (StringUtil.isEmpty(retVal)) {
            return null;
        }
        key = key.toLowerCase();
        if (retVal.equals("true") || retVal.equals("yes") || retVal.equals("y")) {
            return true;
        } else if (retVal.equals("false") || retVal.equals("no") || retVal.equals("n")) {
            return false;
        } else {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "boolean prop invalid:" + key);
        }
    } 
}
