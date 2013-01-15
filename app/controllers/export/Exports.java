/**
 * 
 */
package controllers.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import models.Variable;
import models.export.Export;

import org.apache.commons.lang.StringUtils;

import play.Logger;
import play.data.binding.As;
import play.mvc.Controller;
import play.server.ServletWrapper;
import siena.Json;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

/**
 * @author elhajdi
 * 
 */
public class Exports extends Controller {

  /**
   * checks if an export of the entity given is runing else launchs the export
   * 
   * @param entity
   * @param token
   * @param properties
   * @param filters
   * @param email
   */
  public static void launchExport(String entity, String token, @As(",") List<String> properties, String filters, String email) {
    String cursor = Variable.get(entity + "_cursor");

    if (StringUtils.isNotBlank(cursor) && !cursor.equals(" ")) {
      renderJSON(Json.map().put("error", "An export of the table " + entity + " is runing !").toString());
    } else {
      Export.launchNextExport(entity, token, filters, properties, email);
    }
  }

  /**
   * registers an export request
   * 
   * @param entity
   * @param properties
   * @param filters
   * @param email
   */
  public static void requestExport(String entity, @As(",") List<String> properties, String filters, String email, String conditions) {
    if(entity == null){
      renderJSON(Json.map().put("error", "you should specify the entity"));
    }
    if(properties == null){
      renderJSON(Json.map().put("error", "you should specify some properties"));
    }
    if(filters == null){
      filters = Json.map().toString();
    }else{
      try{
        Json j = Json.loads(filters);
      }catch(Exception e){
        renderJSON(Json.map().put("error", "filters should be a json map"));
      }
    }
    
    if(conditions == null){
      conditions = Json.map().toString();
    }else{
      try{
        Json j = Json.loads(conditions);
      }catch(Exception e){
        renderJSON(Json.map().put("error", "conditions should be a json map"));
      }
    }
    Export export = Export.requestExport(entity, filters, properties, email, conditions);
    Logger.info("requestExport : \n" + export);
    renderJSON(Json.map().put("success",true).put("message", "Your request export is queued").toString());
  }

  /**
   * starts the export given by token
   * 
   * @param entity
   * @param token
   * @param properties
   * @param filters
   * @param email
   */
  public static void exports(String entity, String token, @As(",") List<String> properties, String filters, String email) {
    try {
      Json jfiltes = Json.map();
      if (filters != null)
        jfiltes = Json.loads(filters);
      Export.export(entity, token, jfiltes, properties, email);
      renderJSON(Json.map().put("success", true).put("message", "export is launched").toString());
    } catch (Exception e) {
      // if we catch an error so we change export status to pending then we close the blob and delete it
      Logger.error(e, e.getMessage());
      
      Export export = Export.all().filter("token", token).get();
      export.status = Export.STATUS_PENDING;
      export.date = new Date();
      AppEngineFile file = new AppEngineFile(export.blobFullPath);
      try {
        FileWriteChannel writeChannel = FileServiceFactory.getFileService().openWriteChannel(file, true);
        writeChannel.closeFinally();

        BlobKey blobKey =  FileServiceFactory.getFileService().getBlobKey(file);
        if(blobKey != null) {
          BlobstoreServiceFactory.getBlobstoreService().delete(blobKey);
        }
        Variable.set(entity + "_cursor", "");
        Variable.set(entity + "_blob_full_path", "");
      } catch (Exception ex) {
        Logger.error(ex, ex.getMessage());
      }
      renderJSON(Json.map().put("error", e.getMessage()).toString());
    }
  }

  public static void readBlob(String entity) {
    try {
      Export export = Export.all().filter("entity", entity).fetch().get(1);
      AppEngineFile file = new AppEngineFile(export.blobFullPath);

      FileReadChannel readChannel = FileServiceFactory.getFileService().openReadChannel(file, false);
      String line = "";
      // Again, different standard Java ways of reading from the channel.
      BufferedReader reader = new BufferedReader(Channels.newReader(readChannel, "UTF8"));
      int character = -1;
      while ((character = reader.read()) != -1) {
        line += (char) character + reader.readLine() + "\n";
      }
      readChannel.close();

      renderText(line);
    } catch (Exception e) {
      Logger.error(e, e.getMessage());
      renderText(e.getMessage());
    }
  }

  public static void downloadBlob(String token) {
    try {
      Export export = Export.all().filter("token", token).get();
      AppEngineFile file = new AppEngineFile(export.blobFullPath);
      BlobKey blobKey = FileServiceFactory.getFileService().getBlobKey(file);
      
      response.setHeader("Content-type", " text/csv");
      response.setHeader("Cache-Control", " no-store, no-cache");
      response.setHeader("Content-Disposition", "attachment;filename=" + token + ".csv");

      BlobstoreService blobstoreService = BlobstoreServiceFactory.getBlobstoreService();  
      blobstoreService.serve(blobKey, (HttpServletResponse) request.args.get(ServletWrapper.SERVLET_RES));
      
    } catch (Exception e) {
      Logger.error(e, e.getMessage());
      renderText(e.getMessage());
    }
  }
  
  public static void localDownloadBlob(String token) {
    try {
      Export export = Export.all().filter("token", token).get();
      AppEngineFile file = new AppEngineFile(export.blobFullPath);
      
      response.setHeader("Content-type", " text/csv");
      response.setHeader("Cache-Control", " no-store, no-cache");
      response.setHeader("Content-Disposition", "attachment;filename=" + token + ".csv");

      FileReadChannel readChannel = FileServiceFactory.getFileService().openReadChannel(file, false);
      String line = "";
      // Again, different standard Java ways of reading from the channel.
      BufferedReader reader = new BufferedReader(Channels.newReader(readChannel, "UTF8"));
      int character = -1;
      while ((character = reader.read()) != -1) {
        line += (char) character + reader.readLine() + "\n";
      }
      readChannel.close();
      renderText(line);

    } catch (Exception e) {
      Logger.error(e, e.getMessage());
      renderText(e.getMessage());
    }
  }

  /*public static void cleanBlob(String entity) {
    String path = Variable.get(entity + "_blob_full_path");
    AppEngineFile file = new AppEngineFile(path);
    try {

      BlobKey blobKey =  FileServiceFactory.getFileService().getBlobKey(file);
      if(blobKey != null) {
        BlobstoreServiceFactory.getBlobstoreService().delete(blobKey);
      }
      Variable.set(entity + "_cursor", "");
      Variable.set(entity + "_blob_full_path", "");
      renderText(file.getFullPath() + " deleted");

    } catch (UnsupportedOperationException fe) {
      try {
        FileWriteChannel writeChannel = FileServiceFactory.getFileService().openWriteChannel(file, true);
        writeChannel.closeFinally();
        BlobKey blobKey =  FileServiceFactory.getFileService().getBlobKey(file);
        if(blobKey != null) {
          BlobstoreServiceFactory.getBlobstoreService().delete(blobKey);
        }
        renderText(file.getFullPath() + " deleted");
        Variable.set(entity + "_cursor", "");
        Variable.set(entity + "_blob_full_path", "");
      } catch (IOException ioe) {
        Logger.error(ioe, ioe.getMessage());
        renderText("file not found");
      }
    } catch (Exception e) {
      Logger.error(e, e.getMessage());
      renderText(e.getStackTrace());
    }
  }*/

  public static void refreshExport(String entity) {
    String properties = params.get("properties");
    String filters = params.get("filters");
    Queue queue = QueueFactory.getQueue("export-queue");
    queue.add(Builder.withUrl("/export/" + entity).method(Method.POST).param("offset", "0").param("properties", properties).param("filters", filters));
  }

  public static void show() {
    String status = params.get("status");
    List<Export> exports = Export.all().filter("status", status==null?Export.STATUS_DONE:status).fetch();
    renderText(exports);
  }

  /**
   * launch the first export request
   */
  public static void startExport() {
    Export export = Export.all().filter("status", Export.STATUS_GENERATING).get();
    if (export != null) {
      renderJSON(Json.map().put("error", "An export is runing !").toString());
    }
    export = Export.all().filter("status", Export.STATUS_PENDING).order("date").get();
    if (export == null) {
      Logger.info("The queue export is empty");
      renderJSON(Json.map().put("success",true).put("message","The queue of export request is empty").toString());
    } else {
      Logger.info("start export of "+ export );
      export.status = Export.STATUS_GENERATING;
      export.update();
      String[] arrayProps = StringUtils.split(export.properties, ",");
      Export.launchNextExport(export.entity, export.token, export.filters, Arrays.asList(arrayProps), export.email);
      renderJSON(Json.map().put("success",true).put("message", "The queue").toString());
    }
  }
  
  @SuppressWarnings("deprecation")
  public static void showRequest(@As(",") List<String> properties, String entity) throws Exception {
    String strProp = "";
    if(entity == null) {
      strProp = StringUtils.join(properties, ",");
      renderArgs.put("properties", strProp);
      renderTemplate("Exports/show.html", renderArgs);
    }
    StringBuffer out = new StringBuffer();
    String where = params.get("where");
    if(StringUtils.isNotBlank(where)) {
//      where = StringUtils.deleteWhitespace(where);
//      where =  where.replaceAll(" (AND|And|ANd|aND|anD) ", "and");
//      String[] wheres  = where.split("and");
    } else {
      where = Json.map().toString();
    }
    Json filters = Json.loads(where);
    Query q = new Query(entity);
    Class clazz = Class.forName("models."+entity);
    // add filters to query
    if (filters != null) {
      for (Iterator<String> it = filters.keys().iterator(); it.hasNext();) {
        String key = it.next();
        List<String> s = Arrays.asList(StringUtils.stripToEmpty(key).split("\\s+"));
        if (s.size() == 1) {
          s.add("=");
        } else if (s.size() > 2) {
          throw new Exception("Syntax filter " + key + " is not correct");
        }
        String aFilter= StringUtils.stripToEmpty(s.get(0));
        if(!"".equals(aFilter)) {
          Field afield = clazz.getField(aFilter);
          String className = afield.getType().getName();//Double.class.getName()
          if(className.equals("boolean") || className.equals("java.lang.Boolean")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Boolean.parseBoolean(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("int") || className.equals("java.lang.Integer")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Integer.parseInt(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("long") || className.equals("java.lang.Long")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Long.parseLong(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("double") || className.equals("java.lang.Double")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Double.parseDouble(StringUtils.stripToEmpty(filters.get(key).asString())));
          }  else if (className.equals("float") || className.equals("java.lang.Float")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Float.parseFloat(StringUtils.stripToEmpty(filters.get(key).asString())));
          } 
          else {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), StringUtils.stripToEmpty(filters.get(key).asString()));
          }
        }
      }
    }
    Json conditions = Json.map();
    try {
      conditions = Json.loads(params.get("conditions"));
    }catch(Exception e) {
    }
    
    int limit = 30;
    try{
      limit = Integer.parseInt(params.get("limit"));
    }catch (NumberFormatException nfe) {
    }
    int offset = 0;
    try{
      offset = Integer.parseInt(params.get("offset"));
    }catch (NumberFormatException nfe) {
    }
    
    // we specify limit and offset wich respectivly has 30 and 0 as default value
    FetchOptions fetchOptions = FetchOptions.Builder.withLimit(limit).offset(offset);
    
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    PreparedQuery pq = datastore.prepare(q);
    QueryResultList<Entity> results = pq.asQueryResultList(fetchOptions);
    if(!results.isEmpty()) {
      out.append("<table border='1' style='margin:0 5px;'>");
      
      out.append("<tr>");
      for (String property : properties) {
        if (StringUtils.isBlank(property)) {
          continue;
        }
        out.append("<th>");
        out.append(property);
        out.append("</th>");
      }
      out.append("</tr>");
      for (Entity table : results) {
        writeEntity(table, out, properties, conditions);
      }
      out.append("</table>");
    }
    if(properties != null && !properties.isEmpty()) {
      strProp = StringUtils.join(properties, ",");
    }
    
    renderArgs.put("properties", strProp);
    renderArgs.put("entity", entity);
    renderArgs.put("where", where);
    renderArgs.put("conditions", conditions);
    renderArgs.put("results", out.toString());
    renderArgs.put("limit", limit);
    renderArgs.put("offset", offset);
    
    render("Exports/show.html", renderArgs);
  }
  
  private static void writeEntity(Entity entity, StringBuffer out, List<String> properties, Json conditions) {
    out.append("<tr>");
    if( !conditions.isEmpty()){
      for(String key : conditions.keys()){
        String value = (String) entity.getProperty(key);
        if(!value.matches(conditions.get(key).asString())) {
          return;
        }
      }
    }
    Map<String, Object> mapProperties = entity.getProperties();
    if (properties == null) {
      for (Iterator<String> itProp = mapProperties.keySet().iterator(); itProp.hasNext();) {
        String property = itProp.next();
        out.append("<td>");
        // write on the blob
        out.append(mapProperties.get(property));
        //check if we reach the last element
        out.append("</td>");
      }
    } else {
      for (Iterator<String> itProp = properties.iterator(); itProp.hasNext();) {
        String property = itProp.next();
        property = StringUtils.stripToEmpty(property);
        // write on the blob
        out.append("<td>");
        if("id".equalsIgnoreCase(property) && entity.getKey() != null) {
          out.append(entity.getKey().getId() );
        } else {
          Object o = mapProperties.get(property);
          out.append(String.valueOf(o));
        }
        out.append("</td>");
      }
    }
    out.append("</tr>");
  }

}
