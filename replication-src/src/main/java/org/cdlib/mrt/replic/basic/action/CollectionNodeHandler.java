/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.replic.basic.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.cdlib.mrt.core.DateState;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;

import org.cdlib.mrt.inv.action.InvActionAbs;
import org.cdlib.mrt.inv.content.InvCollection;
import org.cdlib.mrt.inv.content.InvCollectionNode;
import org.cdlib.mrt.inv.content.InvNode;
import org.cdlib.mrt.inv.utility.DBAdd;
import org.cdlib.mrt.inv.utility.InvDBUtil;

/**
 * Add new CollectionMap
 * @author loy
 */
public class CollectionNodeHandler
        extends ReplicActionAbs
{
    private static final String NAME = "CollectionNodeHandler";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
    protected DBAdd dbAdd = null;
    protected InvCollectionNode invCollectionNode = null;
    
    public static CollectionNodeHandler getCollectionNodeHandler(Connection connection, LoggerInf logger)
        throws TException
    {
        return new CollectionNodeHandler(connection, logger);
    }
    
    public CollectionNodeHandler() { }
    
    protected CollectionNodeHandler(Connection connection, LoggerInf logger)
        throws TException
    {
        super(connection, logger);
        try {
            validate();
            this.dbAdd = new DBAdd(connection, logger);
    
            
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
    
    private void validate()
        throws TException
    {
        if (connection == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "connection missing");
        }
        if (logger == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger missing");
        }
        try {
            connection.setAutoCommit(false);
        } catch (Exception ex) {
            throw new TException.SQL_EXCEPTION(ex);
        }
    }
    
    public long process(Identifier collectionArk, int toNodeNumber)
        throws TException
    {
        try {
            invCollectionNode = getCreate(collectionArk, toNodeNumber);
            long cnt = initializeCollection(invCollectionNode.getId());
            connection.commit();
            return cnt;
                   
         } catch (Exception ex) {
            ex.printStackTrace();
            try {
                connection.rollback();
            } catch (Exception rex) {
                System.out.println(MESSAGE + "Rollback exception:" + rex);
            }
            if (ex instanceof TException) {
                throw (TException) ex;
            }
            else throw new TException(ex);
            
        } finally {
             try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception ex2) { }
        }
    }
    
    public InvCollectionNode getCreate(Identifier collectionArk, int toNodeNumber)
        throws TException
    {
        try {
            InvNode invNode = InvDBUtil.getNode(toNodeNumber, connection, logger);
            if (invNode == null) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND(MESSAGE + "add - node number not found:" + toNodeNumber);
            }
            String arkS = collectionArk.getValue();
            InvCollection invCollection = InvDBUtil.getCollection(arkS, connection, logger);
            
            if (invCollection == null) {
                throw new TException.REQUEST_INVALID(MESSAGE + "add - collection ark not found:" + arkS);
            }
            long collectionSeq = invCollection.getId();
            long nodeSeq = invNode.getId();
            return getCreate(collectionSeq, nodeSeq);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public InvCollectionNode getCreate(long collectionSeq, long nodeSeq)
        throws TException
    {
        InvCollectionNode collectionMap = null;        
        try {
            collectionMap = InvDBUtil.getCollectionNode(collectionSeq, nodeSeq, connection, logger);
            if (collectionMap == null) {
                collectionMap = new InvCollectionNode(logger);
                collectionMap.setCollectionid(collectionSeq);
                collectionMap.setNodeid(nodeSeq);
                collectionMap.setCreated((DateState)null);
                long id = dbAdd.replace(collectionMap);
                collectionMap.setId(id);
            }
            this.invCollectionNode = collectionMap;
            return collectionMap;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected int initializeCollection(long nodeseq)
        throws TException
    {
        try {
            String sql =
                "update inv_nodes_inv_objects set replicated=null where inv_object_id in "
                + "(select inv_collections_inv_objects.inv_object_id "
                + "from inv_collections_inv_objects, inv_collections_inv_nodes "
                + "where inv_collections_inv_objects.inv_collection_id = inv_collections_inv_nodes.inv_collection_id "
                + "and inv_collections_inv_nodes.id = " + nodeseq + ");";
        
            Statement createStmt = connection.createStatement();
 
            // execute the java preparedstatement
            int resultCnt = createStmt.executeUpdate(sql);          
            System.out.println("Initialize:"
                    + " - sql:" + sql + "\n"
                    + " - resultCnt:" + resultCnt + "\n"
                    );
            System.out.println("Initialize val:" +  resultCnt);
            return resultCnt;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected long initializeCollectionOld(long nodeseq)
        throws TException
    {
        try {
            String sql =
                "update inv_nodes_inv_objects set replicated=null where inv_object_id in "
                + "(select inv_collections_inv_objects.inv_object_id "
                + "from inv_collections_inv_objects, inv_collections_inv_nodes "
                + "where inv_collections_inv_objects.inv_collection_id = inv_collections_inv_nodes.inv_collection_id "
                + "and inv_collections_inv_nodes.id = " + nodeseq + ");";
            System.out.println("Initialize:" + sql);
            long val = DBAdd.exec(connection, sql, logger);
            System.out.println("Initialize val:" +  val);
            return val;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public InvCollectionNode getInvCollectionNode() {
        return invCollectionNode;
    }
    
}
