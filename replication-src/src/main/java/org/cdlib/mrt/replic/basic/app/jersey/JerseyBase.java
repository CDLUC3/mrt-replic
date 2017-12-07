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
package org.cdlib.mrt.replic.basic.app.jersey;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URL;


import org.glassfish.jersey.server.CloseableService;
import javax.servlet.ServletConfig;
import javax.ws.rs.core.Response;

import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.core.FileContent;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.SerializeUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Base Jersey handling for both Storage and CAN services
 * The attempt is to keep the Jersey layer as thin as possible.
 * Jersey provides the servlet layer for storage RESTful interface
 * <pre>
 * The Jersey routines typically perform the following functions:
 * - get System configuration
 * - get StorageManager
 * - call appropriate StorageManager method
 * - return file or create formatted file
 * - encapsolate formatted file in Jersey Response - setting appropriate return codes
 * </pre>
 * @author dloy
 */
public class JerseyBase
{

    protected static final String NAME = "JerseyBase";
    protected static final String MESSAGE = NAME + ": ";
    protected static final FormatterInf.Format DEFAULT_OUTPUT_FORMAT
            = FormatterInf.Format.xml;
    protected static final boolean DEBUG = false;
    protected static final String NL = System.getProperty("line.separator");

    protected LoggerInf defaultLogger = new TFileLogger("Jersey", 10, 10);
    protected JerseyCleanup jerseyCleanup = new JerseyCleanup();



    /**
     * Shortcut enum for format types for both State display and Archive response
     */
    public enum FormatType
    {
        anvl("state", "txt", "text/x-anvl", null),
        json("state", "json", "application/json", null),
        serial("state", "ser", "application/x-java-serialized-object", null),
        octet("file", "txt", "application/octet-stream", null),
        tar("archive", "tar", "application/x-tar", null),
        targz("archive", "tar.gz", "application/x-tar-gz", "gzip"),
        txt("file", "txt", "plain/text", null),
        xml("state", "xml", "text/xml", null),
        rdf("state", "xml", "application/rdf+xml", null),
        turtle("state", "ttl", "text/turtle", null),
        xhtml("state", "xhtml", "application/xhtml+xml", null),
        zip("archive", "zip", "application/zip", null);

        protected final String form;
        protected final String extension;
        protected final String mimeType;
        protected final String encoding;

        FormatType(String form, String extension, String mimeType, String encoding) {
            this.form = form;
            this.extension = extension;
            this.mimeType = mimeType;
            this.encoding = encoding;
        }

        /**
         * Extension for this format
         * @return
         */
        public String getExtension() {
            return extension;
        }

        /**
         * return MIME type of this format response
         * @param t
         * @return MIME type
         */
        public String getMimeType() {
            return mimeType;
        }

        /**
         * return form of this format
         * @param t
         * @return MIME type
         */
        public String getForm() {
            return form;
        }

        /**
         * return encoding of this format
         * @return encoding
         */
        public String getEncoding() {
            return encoding;
        }

        public static FormatType valueOfExtension(String t)
        {
            if (StringUtil.isEmpty(t)) return null;
            for (FormatType p : FormatType.values()) {
                if (p.getExtension().equals(t)) {
                    return p;
                }
            }
            return null;
        }

        /**
         * return MIME type of this format response
         * @param t
         * @return MIME type
         */
        public static FormatType valueOfMimeType(String t)
        {
            if (StringUtil.isEmpty(t)) return null;
            for (FormatType p : FormatType.values()) {
                if (p.getMimeType().equals(t)) {
                    return p;
                }
            }
            return null;
        }
    }

    /**
     * Format file from input State file
     *  - identify format type
     *  - format the State object (locally or remote)
     *  - save formatted object to file
     *  - return file
     * @param responseState object to be formatted
     * @param formatType user requested format type
     * @param logger file logger
     * @return formatted data with MimeType
     * @throws TException
     */
    protected TypeFile getStateFile(StateInf responseState, FormatType outputFormat, LoggerInf logger)
            throws TException
    {
        if (responseState == null) return null;
        PrintStream stream = null;
        TypeFile typeFile = new TypeFile();
        //System.out.println("!!!!" + MESSAGE + "getStateFile localFormat=" + localFormat.toString());
        try {
            if (outputFormat == FormatType.serial) {
                typeFile.formatType = outputFormat;
                if (responseState instanceof Serializable) {
                    Serializable serial = (Serializable)responseState;
                    typeFile.file = FileUtil.getTempFile("state", ".ser");
                    SerializeUtil.serialize(serial, typeFile.file);
                }
            }

            if (typeFile.file == null) {
                FormatterInf formatter = getFormatter(outputFormat, logger);
                FormatterInf.Format formatterType = formatter.getFormatterType();
                String foundFormatType = formatterType.toString();
                typeFile.formatType = FormatType.valueOf(foundFormatType);
                String ext = typeFile.formatType.getExtension();
                typeFile.file = FileUtil.getTempFile("state", "." + ext);
                FileOutputStream outStream = new FileOutputStream(typeFile.file);
                stream = new PrintStream(outStream, true, "utf-8");
                formatter.format(responseState, stream);
            }
            return typeFile;

        } catch (TException tex) {
            System.out.println("Stack:" + StringUtil.stackTrace(tex));
            throw tex;

        } catch (Exception ex) {
            System.out.println("Stack:" + StringUtil.stackTrace(ex));
            throw new TException.GENERAL_EXCEPTION(MESSAGE + " Exception:" + ex);

        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception ex) { }
            }
        }

    }

    /**
     * Validate that the user passed format legit
     * @param formatType user passed format
     * @param form type of format: "state", "archive", "file"
     * @return FormatType form of user format
     * @throws TException
     */
    protected FormatType getFormatType(String formatType, String form)
            throws TException
    {
        try {
            if (StringUtil.isEmpty(formatType)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
            }
            formatType = formatType.toLowerCase();
            FormatType format = FormatType.valueOf(formatType);
            if (!format.getForm().equals(form)) {
                throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType);
            }

        return format;
        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("Format not supported:" + formatType, ex);
        }
    }


    protected void validateManifest(
            File manifest,
            String sizeS,
            String type,
            String value)
        throws TException
    {
        long size = 0;
        if (StringUtil.isNotEmpty(sizeS)) {
            try {
                size = Long.parseLong(sizeS);
            } catch (Exception ex) {
                throw new TException.REQUEST_INVALID(
                        "validateManifest - manifest size not numeric:"
                        + sizeS);
            }
            if (manifest.length() != size) {
                throw new TException.INVALID_DATA_FORMAT(
                        "validateManifest - size argument does not match passed manifest"
                        + " - passed size=" + size
                        + " - manifest size=" + manifest.length()
                        );
            }
        }
        if (size == 0) size = manifest.length();
        if (StringUtil.isNotEmpty(type)
                && StringUtil.isNotEmpty(value)) {
            LoggerInf logger = new TFileLogger("JerseyStorage", 10, 10);
            FixityTests fixity = new FixityTests(manifest, type, logger);
            FixityTests.FixityResult fixityResult = fixity.validateSizeChecksum(value, type, size);
            if (!fixityResult.fileSizeMatch || !fixityResult.checksumMatch) {
                String msg = MESSAGE + "validateManifest - Fixity fails."
                        + fixityResult.dump("");
                throw new TException.INVALID_DATA_FORMAT(msg);
            }
        }
    }


    /**
     * Get Response to a formatted State object
     * @param responseState State object to format
     * @param formatType user specified format type
     * @param logger system logging
     * @return Jersey Response referencing formatted State object (as File)
     * @throws TException process exceptions
     */
    protected Response getStateResponse(
            StateInf responseState,
            String formatType,
            LoggerInf logger,
            CloseableService cs,
            ServletConfig sc)
        throws TException
    {
        TypeFile typeFile = null;
        FormatType format = null;
        try {
            format = getFormatType(formatType, "state");

        } catch (TException tex) {
            responseState = tex;
            format = FormatType.xml;
        }

        try {
            typeFile = getStateFile(responseState, format, logger);
            jerseyCleanup.addTempFile(typeFile.file);
            cs.add(jerseyCleanup);

        } catch (TException tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType);
        }
        log("getStateResponse:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());

        return Response.ok(typeFile.file, typeFile.formatType.getMimeType()).build();
    }

    /**
     * Return a Jersey Response object after formatting an exception
     * @param exception process exception to format
     * @param formatType format to use on exception (default xml)
     * @param logger system logger
     * @return Jerse Response referencing formatted Exception output
     * @throws TException process exceptions
     */
    protected Response getExceptionResponse(TException exception, String formatType, LoggerInf logger)
        throws TException
    {
        if (DEBUG)
            System.out.println("TRACE:" + exception.getTrace());
        int httpStatus = exception.getStatus().getHttpResponse();
        TypeFile typeFile = null;
        FormatType format = null;
        try {
            format = getFormatType(formatType, "state");

        } catch (TException dtex) {
            format = FormatType.xml;
        }
        try {
            typeFile = getStateFile(exception, format, logger);
            jerseyCleanup.addTempFile(typeFile.file);

        } catch (TException dtex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType + "Exception:" + dtex);
        }
        log("getStateResponse:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        return Response.ok(typeFile.file, typeFile.formatType.getMimeType()).status(httpStatus).build();
    }

    /**
     * Set the Jersey Response for a returned file.
     * Note that a Content-Disposition is set to force the setting of a file name on the returned file
     * @param content File data and metadata used for the response
     * @param formatType Enumerated type of format for this file
     * @param fileName name of file to be returned
     * @param logger process logging
     * @return Jersey Response with headers and content set
     * @throws TException  process exceptions
     */
    protected Response getFileResponse(
            FileContent content,
            String formatType,
            String fileName,
            CloseableService cs,
            LoggerInf logger)
        throws TException
    {
        TypeFile typeFile = new TypeFile();
        try {
            typeFile.file = content.getFile();
            typeFile.formatType = FormatType.valueOf(formatType);
            jerseyCleanup.addTempFile(typeFile.file);
            cs.add(jerseyCleanup);

        } catch (Exception tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType);
        }
        log("formatType:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        /*
        InputStream mapStream = StringUtil.stringToStream(typeFile.formatType.getMimeMap(), "UTF-8");
        MimetypesFileTypeMap mimeMap = new MimetypesFileTypeMap(mapStream);
        String mt = mimeMap.getContentType(typeFile.file);
         */
        String encoding = typeFile.formatType.getEncoding();
        if (encoding != null) {
            return Response.ok(typeFile.file, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    //.header("Content-Encoding", encoding)
                    .build();
        } else {
            return Response.ok(typeFile.file, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .build();
        }
    }
    
       /**
     * Set the Jersey Response for a returned file.
     * Note that a Content-Disposition is set to force the setting of a file name on the returned file
     * @param content File data and metadata used for the response
     * @param formatType Enumerated type of format for this file
     * @param fileName name of file to be returned
     * @param logger process logging
     * @return Jersey Response with headers and content set
     * @throws TException  process exceptions
     */
    protected Response getFileResponse(
            File content,
            String formatType,
            String fileName,
            CloseableService cs,
            LoggerInf logger)
        throws TException
    {
        TypeFile typeFile = new TypeFile();
        try {
            typeFile.file = content;
            typeFile.formatType = FormatType.valueOf(formatType);
            jerseyCleanup.addTempFile(typeFile.file);
            cs.add(jerseyCleanup);

        } catch (Exception tex) {
            throw new JerseyException.INTERNAL_SERVER_ERROR("Could not process this format:" + formatType);
        }
        log("formatType:" + formatType
                + " - tformatType=" + typeFile.formatType
                + " - mimeType=" + typeFile.formatType.getMimeType());
        /*
        InputStream mapStream = StringUtil.stringToStream(typeFile.formatType.getMimeMap(), "UTF-8");
        MimetypesFileTypeMap mimeMap = new MimetypesFileTypeMap(mapStream);
        String mt = mimeMap.getContentType(typeFile.file);
         */
        String encoding = typeFile.formatType.getEncoding();
        if (encoding != null) {
            return Response.ok(typeFile.file, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    //.header("Content-Encoding", encoding)
                    .build();
        } else {
            return Response.ok(typeFile.file, typeFile.formatType.getMimeType())
                    .header("Content-Disposition", "attachment; filename=" + fileName)
                    .build();
        }
    }


    /**
     * build a file containg the addVersion manifest
     * The manifest is sent to this method either as a String value or as a url reference
     * @param manifestRequest (Alternate)String containing addVersion manifest
     * @param url (Alternate) URL reference to addVersion manifest
     * @param logger process logger
     * @return file containing addVersion manifest
     * @throws TException process exception
     */
    protected File getManifest(
            String manifestRequest,
            String url,
            LoggerInf logger)
        throws TException
    {

        try {
        File tempFile = FileUtil.getTempFile("manifest", ".txt");
        URL remoteManifest = null;
        if (StringUtil.isNotEmpty(manifestRequest) && !manifestRequest.equals("none")) {
            InputStream manifest = StringUtil.stringToStream(manifestRequest, "utf-8");
            FileUtil.stream2File(manifest, tempFile);
            return tempFile;
        }
        if (StringUtil.isNotEmpty(url) && !url.equals("none")) {
            try {
                    remoteManifest = new URL(url);
                } catch (Exception ex) {
                    throw new TException.REQUEST_INVALID(
                        "Manifest URL is invalid:" + url);
                }
                FileUtil.url2File(logger, remoteManifest, tempFile);
                return tempFile;
        }
        throw new TException.REQUEST_INVALID(
                 "Neither manifest.txt nor url provided");

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            throw new TException.REQUEST_INVALID(
                        "Unable to obtain manifest");
        }

    }

    /**
     * return integer if valid or exception if not
     * @param header exception header
     * @param parm String value of parm
     * @return parsed int value
     * @throws TException
     */
    protected int getNumber(String header, String parm)
        throws TException
    {
        try {
            return Integer.parseInt(parm);

        } catch (Exception ex) {
            throw new JerseyException.BAD_REQUEST(header + ": Number required, found " + parm);
        }
    }


    /**
     * Get StateInf formatter using Jersey FormatType
     * Involves mapping Jersey FormatType to FormatterInf.Format type
     * @param outputFormat  Jersey formattype
     * @param logger process logger
     * @return Formatter
     * @throws TException process exception
     */
    protected FormatterInf getFormatter(FormatType outputFormat, LoggerInf logger)
        throws TException
    {
        String formatS = null;
        try {
            formatS = outputFormat.toString();
            FormatterInf.Format formatterType = FormatterInf.Format.valueOf(formatS);
            return FormatterAbs.getFormatter(formatterType, logger);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            System.out.println("getFormatter: stack:" + StringUtil.stackTrace(ex));
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("State formatter type not supported:" + formatS);
        }
    }


    /**
     * Normalize name of a file response file
     * @param fileResponseName non-normalized name
     * @return normalized name
     */
    protected String getFileResponseFileName(String fileResponseName)
        throws TException
    {
        if (StringUtil.isEmpty(fileResponseName)) return "";
        fileResponseName = fileResponseName.replace('/', '=');
        fileResponseName = fileResponseName.replace('\\', '=');
        return fileResponseName;
    }


    /**
     * return JerseyCleanup
     * @return JerseyCleanup
     */
    protected JerseyCleanup getJerseyCleanup()
    {
        return jerseyCleanup;
    }

    /**
     * Set boolean flag based on a passed string
     * @param valueS string to evaluate
     * @param ifNull if string is null return this
     * @param ifNone if string has not null and zero length return this
     * @param ifInvalid if string is not matched return this
     * @return true or false
     */
    public static boolean setBool(String valueS, boolean ifNull, boolean ifNone, boolean ifInvalid)
    {
        if (valueS == null) return ifNull;
        if (valueS.length() == 0) return ifNone;
        if (valueS.equals("true")) return true;
        if (valueS.equals("t")) return true;
        if (valueS.equals("yes")) return true;
        if (valueS.equals("false")) return false;
        if (valueS.equals("f")) return false;
        if (valueS.equals("no")) return false;
        return ifInvalid;
    }

    /**
     * If debug flag on then sysout this message
     * @param msg message to sysout
     */
    protected void log(String msg)
    {
        if (DEBUG) System.out.println("[JerseyStorage]>" + msg);
        //logger.logMessage(msg, 0, true);
    }

    /**
     * Container class for file and Jersey FormatType enum
     */
    public class TypeFile
    {
        public FormatType formatType = null;
        public File file = null;
    }

}