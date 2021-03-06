/*
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

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
*********************************************************************/
package org.cdlib.mrt.replic.basic.app.jersey.replic;

import org.cdlib.mrt.replic.basic.app.ReplicationServiceInit;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceInf;


import javax.servlet.ServletConfig;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.server.CloseableService;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.QueryParam;
import java.io.File;
import org.cdlib.mrt.core.FileContent;


import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.replic.basic.app.jersey.KeyNameHttpInf;
import org.cdlib.mrt.replic.basic.app.jersey.JerseyBase;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.replic.basic.service.ReplicationService;
import static org.cdlib.mrt.replic.basic.service.ReplicationService.getFileResponseFileName;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Thin Jersey layer for fixity handling
 * @author  David Loy
 */
@Path ("/")
public class JerseyReplication
        extends JerseyBase
        implements KeyNameHttpInf
{

    protected static final String NAME = "JerseyReplic";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.xml;
    protected static final boolean DEBUG = false;
    protected static final String NL = System.getProperty("line.separator");

    /**
     * Get state information about a specific node
     * @param nodeID node identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted service information
     * @throws TException
     */
    @GET
    @Path("/state")
    public Response callGetServiceState(
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return getServiceState(formatType, cs, sc);
    }

    /**
     * Get state information about a specific node
     * @param nodeID node identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted service information
     * @throws TException
     */
    @GET
    @Path("/status")
    public Response callGetServiceStatus(
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return getServiceStatus(formatType, cs, sc);
    }

    @GET
    @Path("content/{objectid}/{versionid}/{fileid}")
    public Response getFile(
            @PathParam("objectid") String objectIDS,
            @PathParam("versionid") String versionIDS,
            @PathParam("fileid") String fileID,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        Identifier objectID = new Identifier(objectIDS) ;
        int versionID = -1 ;
        try {
            versionID = Integer.parseInt(versionIDS) ;
        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID("versionID invalid");
        }
        return getFile(objectID, versionID, fileID, cs, sc);
    }

    @GET
    @Path("manifest/{objectid}")
    public Response getManifest(
            @PathParam("objectid") String objectIDS,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        Identifier objectID = new Identifier(objectIDS) ;
        return getManifest(objectID, cs, sc);
    }

    @POST
    @Path("service/{setType}")
    public Response callService(
            @PathParam("setType") String setType,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (StringUtil.isEmpty(setType)) {
            throw new TException.REQUEST_INVALID("Set fixity status requires 'S' query element");
        }
        setType = setType.toLowerCase();
        if (setType.equals("start")) {
            return startup(formatType, cs, sc);

        } else if (setType.equals("shutdown")) {
            return shutdown(formatType, cs, sc);
            
        } else if (setType.equals("pause")) {
            return pause(formatType, cs, sc);

        } else  {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Set fixity state value not recognized:" + setType);
        }
    }
    
    /* Removed because providing node is too dangerous and not db based
    Use deletesecondary instead
    Not supported on lower routines
    @DELETE
    @Path("delete/{nodeS}/{objectIDS}")
    public Response callDelete(
            @PathParam("nodeS") String nodeS,
            @PathParam("objectIDS") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return delete(nodeS, objectIDS, formatType, cs, sc);
    }
    */
    
    @DELETE
    @Path("invdelete/{nodeS}/{objectIDS}")
    public Response callDeleteInv(
            @PathParam("nodeS") String nodeS,
            @PathParam("objectIDS") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return deleteInv(nodeS, objectIDS, formatType, cs, sc);
    }
    
    @DELETE
    @Path("deletesecondary/{objectIDS}")
    public Response callDeleteSecondary(
            @PathParam("objectIDS") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return deleteSecondary(objectIDS, formatType, cs, sc);
    }
    
    @POST
    @Path("add/{objectIDS}")
    public Response callAdd(
            @PathParam("objectIDS") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return add(objectIDS, formatType, cs, sc);
    }
    
    @POST
    @Path("addinv/{objectIDS}")
    public Response callAddInv(
            @PathParam("objectIDS") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return addInv(objectIDS, formatType, cs, sc);
    }

    
    @POST
    @Path("replace/{nodeS}/{objectIDS}")
    public Response callReplaceBackground(
            @PathParam("nodeS") String nodeS,
            @PathParam("objectIDS") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return replaceBackground(nodeS, objectIDS, formatType, cs, sc);
    }
    
    @POST
    @Path("cleanup")
    public Response callCleanup(
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return cleanupReplic(formatType, cs, sc);
    }  
    
    @POST
    @Path("addmap/{collectionIDS}/{nodeS}")
    public Response callAddMap(
            @PathParam("collectionIDS") String collectionIDS,
            @PathParam("nodeS") String nodeS,
            @DefaultValue("xhtml") @QueryParam(org.cdlib.mrt.inv.app.jersey.KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        return addMap(collectionIDS, nodeS, formatType, cs, sc);
    }
    
    @GET
    @Path("match/{sourceNode}/{targetNode}/{objectid}")
    public Response callMatchObjects(
            @PathParam("sourceNode") String sourceNodeS,
            @PathParam("targetNode") String targetNodeS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "backupObject entered"
                    + " - sourceNodeS=" + sourceNodeS + NL
                    + " - targetNodeS=" + targetNodeS + NL
                    + " - objectIDS=" + objectIDS + NL
                    + " - formatType=" + formatType + NL
                    );
        return matchObjects(sourceNodeS, targetNodeS, objectIDS, formatType, cs, sc);
    }
    
    @GET
    @Path("match/{sourceNode}/{objectid}")
    public Response callMatchObject(
            @PathParam("sourceNode") String sourceNodeS,
            @PathParam("targetNode") String targetNodeS,
            @PathParam("objectid") String objectIDS,
            @DefaultValue("xhtml") @QueryParam(KeyNameHttpInf.RESPONSEFORM) String formatType,
            @Context CloseableService cs,
            @Context ServletConfig sc)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE + "backupObject entered"
                    + " - sourceNodeS=" + sourceNodeS + NL
                    + " - targetNodeS=" + targetNodeS + NL
                    + " - objectIDS=" + objectIDS + NL
                    + " - formatType=" + formatType + NL
                    );
        return matchObjects(sourceNodeS, null, objectIDS, formatType, cs, sc);
    }

    /**
     * Get state information about a specific node
     * @param nodeID node identifier
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted service information
     * @throws TException
     */
    public Response getServiceState(
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            StateInf responseState = replicationService.getReplicationServiceState();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    /**
     * Get state information about system status without db
     * @param formatType user provided format type
     * @param cs on close actions
     * @param sc ServletConfig used to get system configuration
     * @return formatted service information
     * @throws TException
     */
    public Response getServiceStatus(
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            StateInf responseState = replicationService.getReplicationServiceStatus();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response startup(
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            StateInf responseState = replicationService.startup();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response shutdown(
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            StateInf responseState = replicationService.shutdown();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response pause(
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            StateInf responseState = replicationService.pause();
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response delete(
            String nodeNumberS,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            int nodeNumber = Integer.parseInt(nodeNumberS);
            StateInf responseState = replicationService.delete(false, nodeNumber, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response getFile(
            Identifier objectID,
            int versionID,
            String fileID,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        String formatType = "octet";
        String fileResponseName = getFileResponseFileName(fileID);
        try {
            log("getContent entered:"
                    + " - objectIDS=" + objectID.getValue()
                    + " - version=" + versionID
                    + " - fileID=" + fileID
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();
            File content = replicationService.getFile(objectID, versionID, fileID);
            return getFileResponse(content, formatType, fileResponseName, cs, logger);
            
        } catch (TException tex) {
            return getExceptionResponse(tex, "xml", logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            TException tex = new TException(ex);
            return getExceptionResponse(tex, "xml", logger);
        }
    }
    
    public Response getManifest(
            Identifier objectID,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        String formatType = "octet";
        String fileResponseName = getFileResponseFileName(objectID.getValue() + "manifest");
        try {
            log("getManifest entered:"
                    + " - objectIDS=" + objectID.getValue()
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();
            File content = replicationService.getManifest(objectID);
            return getFileResponse(content, formatType, fileResponseName, cs, logger);
            
        } catch (TException tex) {
            return getExceptionResponse(tex, "xml", logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            TException tex = new TException(ex);
            return getExceptionResponse(tex, "xml", logger);
        }
    }
    
    // matchObject(sourceNodeS, targetNodeS, objectIDS, formatType, cs, sc);
    public Response matchObjects(
            String sourceNodeS,
            String targetNodeS,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            int sourceNode = getNumber("sourceNode", sourceNodeS);
            Integer targetNode = null;
            if (!StringUtil.isAllBlank(targetNodeS)) {
                targetNode = getNumber("targetNode", targetNodeS);
            }
            StateInf responseState = replicationService.matchObjects(sourceNode, targetNode, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response deleteInv(
            String nodeNumberS,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            int nodeNumber = Integer.parseInt(nodeNumberS);
            StateInf responseState = replicationService.deleteInv(nodeNumber, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response deleteSecondary(
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            StateInf responseState = replicationService.deleteSecondary(objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response addMap(
            String collectionIDS,
            String nodeNumberS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier collectionID = new Identifier(collectionIDS);
            int nodeNumber = Integer.parseInt(nodeNumberS);
            StateInf responseState = replicationService.addMap(collectionID, nodeNumber);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response add(
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            StateInf responseState = replicationService.add(objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response addInv(
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            StateInf responseState = replicationService.addReplicInv(objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

    /**
     * Update multiple entries
     * @param formatType
     * @param cs
     * @param sc
     * @return
     * @throws TException 
     */
    public Response cleanupReplic(
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("getServiceState entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();
            StateInf responseState = replicationService.doCleanup();
            return getStateResponse(responseState, formatType, logger, cs, sc);


        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public Response replaceBackground(
            String nodeNumberS,
            String objectIDS,
            String formatType,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        LoggerInf logger = defaultLogger;
        try {
            log("replace entered:"
                    + " - formatType=" + formatType
                    );
            ReplicationServiceInit replicServiceInit = ReplicationServiceInit.getReplicationServiceInit(sc);
            ReplicationServiceInf replicationService = replicServiceInit.getReplicationService();
            logger = replicationService.getLogger();

            Identifier objectID = new Identifier(objectIDS);
            int nodeNumber = Integer.parseInt(nodeNumberS);
            StateInf responseState = replicationService.replaceBackground(nodeNumber, objectID);
            return getStateResponse(responseState, formatType, logger, cs, sc);
            

        } catch (TException tex) {
            return getExceptionResponse(tex, formatType, logger);

        } catch (Exception ex) {
            System.out.println("TRACE:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }

}
