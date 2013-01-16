package notifiers.export;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import models.export.Export;

import play.Logger;
import play.mvc.Http.Request;
import play.mvc.Mailer;
import play.templates.BaseTemplate;
import play.templates.TemplateLoader;
import play.vfs.VirtualFile;

public class MailerNotification extends Mailer {
  private static final String YALA_NOREPLY_MAIL = "noreply@yala.fm";

  public static void sendExportNotifiation(Export export) {
    String tempfile = "";
    String resetLink = Request.current().getBase() + "/export/download/" + export.token;
    System.out.println(resetLink);
    try {
      tempfile = readTemplate("/app/views/MailerNotification/export.html");
    } catch (Exception e) {
      Logger.error(e, e.getMessage());
    }
    String subject = "Export of: SELECT "+ export.properties + " FROM "+ export.entity + "WHERE "+export.filters+" WITH CONDITIONS " + export.conditions;
    Map<String, Object> objs = new HashMap<String, Object>();
    objs.put("messageTitle", subject);
    objs.put("resetLink", resetLink);
    BaseTemplate template = TemplateLoader.loadString(tempfile);
    String rendered = template.render(objs);
//    System.out.println(rendered);
    sendEmail(YALA_NOREPLY_MAIL, export.email, subject, rendered);
  }

  private static String readTemplate(String filePath) throws java.io.IOException {
    VirtualFile vf = VirtualFile.fromRelativePath(filePath);
    File realFile = vf.getRealFile();
    byte[] buffer = new byte[(int) realFile.length()];
    BufferedReader reader = new BufferedReader(new FileReader(realFile));

    StringBuilder stringBuilder = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      stringBuilder.append(line);
    }
    return stringBuilder.toString();
  }

  private static void sendEmail(String from, String to, String subject, String body) {
    Properties props = new Properties();
    Session session = Session.getDefaultInstance(props, null);
    try {
      Message msg = new MimeMessage(session);

      msg.setFrom(new InternetAddress(from));
      String[] tos = to.split(",");
      for (int i = 0; i < tos.length; i++) {
        msg.addRecipient(Message.RecipientType.TO, new InternetAddress(tos[i]));
      }
      msg.setSubject(subject);
      Multipart mp = new MimeMultipart();
      MimeBodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(body, "text/html");
      mp.addBodyPart(htmlPart);
      msg.setContent(mp);
      Transport.send(msg);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

}
