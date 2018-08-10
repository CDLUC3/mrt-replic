/*
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
*********************************************************************/
package org.cdlib.mrt.replic.utility;

import java.util.*;
import java.io.*;
//import javax.mail.*;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeMessage;
import org.cdlib.mrt.formatter.FormatInfo;
import org.cdlib.mrt.utility.TException;

/**
* Simple demonstration of using the javax.mail API.
*
* Run from the command line. Please edit the implementation
* to use correct email addresses and host name.
*/
public final class ReplicEmail
{
    protected Session session = null;
    public ReplicEmail(Properties emailProp)
    {
        setSession(emailProp, true);
    }

    public static void main( String... aArguments )
    {
        try {
            Properties props = new Properties();
            props.put("mail.smtp.host", "exchangemail.ad.ucop.edu");
            ReplicEmail emailer = new ReplicEmail(props);
            String[] to = {"dloy@ucop.edu"};
            emailer.sendEmail(
                to,
                "dloy@ucop.edu",
                "Testing 1-2-3",
                "this is a test - this is only a test"
            );
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
        }
    }

    /**
    * Send a single email.
    */
    public void sendEmail(
            String recipients[ ],
            String from,
            String subject,
            String body)
        throws TException
    {
        MimeMessage message = new MimeMessage( session );
        try {

            // set the from and to address
            InternetAddress addressFrom = new InternetAddress(from);
            message.setFrom(addressFrom);

            System.out.println("length=" + recipients.length);
            InternetAddress[] addressTo = new InternetAddress[recipients.length];
            for (int i = 0; i < recipients.length; i++)
            {
                addressTo[i] = new InternetAddress(recipients[i]);
            }
            message.setRecipients(Message.RecipientType.TO, addressTo);
            message.setSubject( subject );
            message.setSentDate(new Date());
            message.setText( body );
            Transport.send( message );

        } catch (Exception ex){
            System.out.println("sendEmail Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    /**
    * Send a single email.
    */
    public void sendEmail(
            String name,
            String formatType,
            String recipients[ ],
            String from,
            String subject,
            String msg,
            String body)
        throws TException
    {
        MimeMessage message = new MimeMessage( session );
        try {
            formatType = formatType.toLowerCase();
            FormatInfo info = FormatInfo.valueOf(formatType);
            String mime = info.getMimeType();
            if (mime.contains("html")) mime = "text/html";
            String bodyContentType = mime + "; charset=\"utf-8\"";

            String ext = info.getExtension();
            if (mime.contains("html")) ext = "html";
            String bodyFileName = name + "." + ext;
            
            // set the from and to address
            InternetAddress addressFrom = new InternetAddress(from);
            message.setFrom(addressFrom);

            System.out.println("length=" + recipients.length);
            InternetAddress[] addressTo = new InternetAddress[recipients.length];
            for (int i = 0; i < recipients.length; i++)
            {
                addressTo[i] = new InternetAddress(recipients[i]);
            }

            MimeBodyPart messagePart = new MimeBodyPart();
            MimeMultipart multipart = new MimeMultipart();
            multipart.addBodyPart(messagePart);  // adding message part

            //Setting the Email Encoding
            messagePart.setText(msg,"utf-8");
            messagePart.setHeader("Content-Type","text/plain; charset=\"utf-8\"");
            messagePart.setHeader("Content-Transfer-Encoding", "quoted-printable");

            MimeBodyPart bodyPart = new MimeBodyPart();
            multipart.addBodyPart(bodyPart);  // adding message part

            //Setting the Email Encoding
            bodyPart.setFileName(bodyFileName);
            bodyPart.setText(body,"utf-8");
            bodyPart.setHeader("Content-Type", bodyContentType);
            bodyPart.setHeader("Content-Transfer-Encoding", "quoted-printable");

            message.setContent(multipart);
            message.setSentDate(new Date());
            message.setRecipients(Message.RecipientType.TO, addressTo);
            message.setSubject( subject );
            Transport.send( message );

        } catch (Exception ex){
            System.out.println("sendEmail Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    protected void setSession(Properties emailProp, boolean debug)
    {
        // create some properties and get the default Session
        session = Session.getDefaultInstance(emailProp, null);
        session.setDebug(debug);
    }
}


