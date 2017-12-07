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

package org.cdlib.mrt.replic.basic.app;


import org.cdlib.mrt.utility.TFrameInit;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;

import org.cdlib.mrt.replic.basic.service.ReplicationService;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceInf;
import org.cdlib.mrt.replic.basic.service.ReplicationServiceHandler;
import org.cdlib.mrt.utility.TException;
/**
 * Initialize handling for storage service in servlet
 * @author dloy
 */
public class ReplicationServiceInit
        extends TFrameInit
{
    private static final String serviceName = "replicService";

    private enum Type {
        Regular, Default
    }
    protected volatile ReplicationServiceHandler replicationServiceHandler = null;

    /**
     * Get resolved storage service
     * @return
     */
    public ReplicationServiceInf getReplicationService()
        throws TException
    {
        return ReplicationService.getReplicationService(replicationServiceHandler);
    }

    /**
     * Factory: StorageServiceInit
     * @param servletConfig servlet configuration object
     * @return StorageServiceInit
     * @throws TException
     */
    public static synchronized ReplicationServiceInit getReplicationServiceInit(
            ServletConfig servletConfig)
            throws TException
    {
        ServletContext servletContext = servletConfig.getServletContext();
        ReplicationServiceInit replicServiceInit  = (ReplicationServiceInit)servletContext.getAttribute(serviceName);
        if (replicServiceInit == null) {
            replicServiceInit = new ReplicationServiceInit(Type.Regular, servletConfig, serviceName);
            servletContext.setAttribute(serviceName, replicServiceInit);
        }

        return replicServiceInit;
    }

    /**
     * Factory: StorageServiceInit
     * @param servletConfig servlet configuration object
     * @return StorageServiceInit
     * @throws TException
     */
    public static synchronized ReplicationServiceInit getReplicationServiceInitDefault(
            ServletConfig servletConfig)
            throws TException
    {
        ServletContext servletContext = servletConfig.getServletContext();
        ReplicationServiceInit replicServiceInit  = (ReplicationServiceInit)servletContext.getAttribute(serviceName);
        if (replicServiceInit == null) {
            replicServiceInit = new ReplicationServiceInit(Type.Regular, servletConfig, serviceName);
            servletContext.setAttribute(serviceName, replicServiceInit);
        }

        return replicServiceInit;
    }

    /**
     * Constructor
     * @param servletConfig servlet configuration
     * @param serviceName service name for logging and for persistence
     * @throws TException
     */
    protected ReplicationServiceInit(Type type, ServletConfig servletConfig, String serviceName)
            throws TException
    {
        super(servletConfig, serviceName);
        if (type == Type.Regular) {
            replicationServiceHandler =
                    ReplicationServiceHandler.getReplicationServiceHandler(tFrame.getProperties());
        }
        if (type == Type.Default) {
            replicationServiceHandler =
                    ReplicationServiceHandler.getReplicationServiceHandler(tFrame.getProperties());
        }
    }
}
