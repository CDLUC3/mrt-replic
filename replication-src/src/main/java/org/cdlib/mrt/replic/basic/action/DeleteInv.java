/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.replic.basic.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;


import org.cdlib.mrt.db.DBUtil;
import org.cdlib.mrt.inv.service.Role;
import org.cdlib.mrt.inv.content.InvNodeObject;
import org.cdlib.mrt.replic.basic.content.ReplicNodesObjects;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
/**
 * Container class for inv Object content
 * @author dloy
 */
public class DeleteInv
        extends ReplicActionAbs
{
    private static final String NAME = "DeleteInv";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    

    protected ReplicNodesObjects deleteNodeObject = null;
    protected long objectseq = 0;
    protected long nodeseq = 0;
    protected Connection connection = null;
    protected boolean deleteOK = false;
    
    
    
    public static DeleteInv getDeleteInv(
            ReplicNodesObjects deleteInvNodeObject,
            Connection connection,
            LoggerInf logger)
        throws TException
    {
        return new DeleteInv(deleteInvNodeObject, connection, logger);
    }
    
    protected DeleteInv(
            ReplicNodesObjects deleteNodeObject, 
            Connection connection, 
            LoggerInf logger)
        throws TException
    { 
        if (deleteNodeObject == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "deleteNodeObject - missing");
        }
        this.deleteNodeObject = deleteNodeObject;
        this.objectseq = deleteNodeObject.getObjectsid();
        this.nodeseq = deleteNodeObject.getNodesid();
        this.connection = connection;
        this.logger = logger;
        if (deleteNodeObject.getRole() != Role.secondary) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE 
                    + " requires Role secondary:" + deleteNodeObject.getRole());
        }
        validate();
        if (DEBUG) System.out.println(MESSAGE 
                + " - nodeseq=" + nodeseq
                + " - objectseq=" + objectseq
        );
    }
    
    private void validate()
        throws TException
    {
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "DBDelete - logger empty");
        }
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "DBDelete - connection empty");
        }
        if (objectseq <= 0) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "delete - objectseq empty");
        }
        if (nodeseq <= 0) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "delete - nodeseq empty");
        }
        boolean isValid = true;
        try {
            isValid = connection.isValid(10);
            connection.setAutoCommit(false);
            
        } catch (Exception ex) {
            throw new TException.SQL_EXCEPTION(MESSAGE + "connection error:" + ex);
        }
        
    }
    

    public int delete()
        throws TException
    {
        deleteNodeObject.setDeleteInv(false);
        try {
            if (DEBUG) System.out.println(MESSAGE + "process entered");
            int delCnt=0;
            delCnt += deleteAudits();
            delCnt += deleteNodesObjects();
            deleteNodeObject.setInvDeleteCount(delCnt);
            deleteNodeObject.setDeleteInv(true);
            connection.commit();
            return delCnt;

        } catch (Exception ex) {
            String msg = MESSAGE + "Exception for objectseq=" + objectseq
                    + " - Exception:" + ex
                    ;
            System.out.println("EXception:" + msg);
            logger.logError(msg, 2);
            try {
                connection.rollback();
            } catch (Exception cex) {
                System.out.println("WARNING: rollback Exception:" + cex);
            }
            if (ex instanceof TException) {
                throw (TException) ex;
            } else {
                throw new TException (ex);
            }

        } finally {
            try {
                connection.close();
            } catch (Exception ex) { }
        }

    }
    
    public int deleteAudits()
        throws TException
    {
        int delNum = delete(
                "inv_audits",
                "inv_node_id",
                nodeseq,
                "inv_object_id",
                objectseq,
                connection,
                logger
                );
        return delNum;
    }
    
    public int deleteNodesObjects()
        throws TException
    {
        int delNum = delete(
                "inv_nodes_inv_objects",
                "inv_node_id",
                nodeseq,
                "inv_object_id",
                objectseq,
                connection,
                logger
                );
        return delNum;
    }
    
    protected static int delete(
            String tableName, 
            String nodeRowName, 
            long nodeseq, 
            String objectRowName, 
            long objectseq, 
            Connection connection, 
            LoggerInf logger)
        throws TException
    {
        try {
            if (objectseq <= 0) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "delete - objectseq not valid:" + objectseq);
            }
            String sql =
                    "DELETE FROM " + tableName 
                    + " WHERE " + nodeRowName + "=" + nodeseq
                    + " AND " + objectRowName + "=" + objectseq
                    + ";";
            
            int delCnt= delete(connection, sql, logger);
            if (DEBUG) System.out.println(MESSAGE + "delete:" 
                    + " - sql=" + sql
                    + " - delCnt=" + delCnt
                    );
            return delCnt;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static int delete(
            Connection connection,
            String deleteCmd,
            LoggerInf logger)
        throws TException
    {
        if (StringUtil.isEmpty(deleteCmd)) {
            throw new TException.INVALID_OR_MISSING_PARM("deleteCmd not supplied");
        }
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM("connection not supplied");
        }
        
        try {

            Statement statement = connection.createStatement();
            int delCnt = statement.executeUpdate(deleteCmd);
            
            return delCnt;

        } catch(Exception e) {
            String msg = "Exception"
                + " - sql=" + deleteCmd
                + " - exception:" + e;

            logger.logError(MESSAGE + "exec - " + msg, 0);
            System.out.println(msg);
            throw new TException.SQL_EXCEPTION(msg, e);
        }
    }

}


