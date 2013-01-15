/**
 * 
 */
package models.export;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.channels.Channels;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.SimpleFormatter;

import models.BaseModel;
import models.Variable;
import notifiers.MailerNotification;

import org.apache.commons.lang.StringUtils;

import siena.Model;
import play.libs.Codec;
import siena.Id;
import siena.Json;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.LockException;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.gson.Gson;

/**
 * @author elhajdi
 * 
 */
public class Export extends Model {
  @Id
  public Long id;

  public String filters = "";
  public String entity = "";
  public String properties = "";
  public Date date;
  public String token = "";
  public String blobFullPath = "";
  public String cursor = "";
  public String email = "";
  public String status = STATUS_PENDING;
  public String conditions = "";
  public final static Map<String, Query.FilterOperator> OPERATORS = initOperators();

  public static final String EMAIL_ADMIN = "ouziel@gmail.com,hajmoh@gmail.com";
  public static final String CSV_SEPARATOR = ",";
  public static final String STATUS_PENDING="pending";
  public static final String STATUS_GENERATING="generating";
  public static final String STATUS_DONE="done";
  
  private static Map<String, Query.FilterOperator> initOperators() {
    Map<String, Query.FilterOperator> ops = new HashMap<String, Query.FilterOperator>();
    ops.put("=", Query.FilterOperator.EQUAL);
    ops.put("!=", Query.FilterOperator.NOT_EQUAL);
    ops.put("<", Query.FilterOperator.LESS_THAN);
    ops.put("<=", Query.FilterOperator.LESS_THAN_OR_EQUAL);
    ops.put(">", Query.FilterOperator.GREATER_THAN);
    ops.put(">=", Query.FilterOperator.GREATER_THAN_OR_EQUAL);
    ops.put("IN", Query.FilterOperator.IN);

    return ops;
  }

  protected static String createNewBlobFile(String entityName, List<String> properties) throws IOException, ClassNotFoundException {
    // Get a file service
    FileService fileService = FileServiceFactory.getFileService();
    Date date= new Date();
    String dateString = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
    dateString += "_" + (int) (Math.random() * 9000);
    // Create a new Blob file with mime-type "text/csv"
    AppEngineFile file = fileService.createNewBlobFile("text/csv", entityName+"_"+dateString);

    // Open a channel to write to it
    boolean lock = false;
    FileWriteChannel writeChannel = fileService.openWriteChannel(file, lock);
    PrintWriter out = new PrintWriter(Channels.newWriter(writeChannel, "UTF8"));
    // write properties name as header of csv file
    if (properties == null || properties.isEmpty()) {
      Class clazz = Class.forName("models." + entityName);
      Field[] fields = clazz.getFields();
      int i = 0;
      for (Field field : fields) {
        if (Modifier.STATIC == field.getModifiers()) {
          continue;
        }
        out.print(field.getName());
        if (++i < fields.length) {
          out.print(CSV_SEPARATOR);
        }
      }
    } else {
      int i = 0;
      for (String property : properties) {
        if (StringUtils.isBlank(property)) {
          continue;
        }
        out.print(property);
        if (++i < properties.size()) {
          out.print(CSV_SEPARATOR);
        }
      }
    }
    out.println();

    out.close();

    Variable.set(entityName + "_blob_full_path", file.getFullPath());
    return file.getFullPath();
  }

  /**
   * Exports entity respecting constraints filters
   * 
   * @param filters
   *          (prop, value) or (prop opertator, value)
   * @throws Exception
   */
  public static void export(String entityName, String token, Json filters, List<String> properties, String email) throws Exception {
    Export export =  Export.all().filter("token", token).get();
    int elementCount = 300;
    FetchOptions fetchOptions = FetchOptions.Builder.withLimit(elementCount);
    String startCursor = export.cursor;// Variable.get(entityName + "_cursor");

    // If we have a cursor in Variable, let's use it
    if (StringUtils.isNotBlank(startCursor)) {
      fetchOptions.startCursor(Cursor.fromWebSafeString(startCursor));
    }

    Query q = new Query(entityName);
    Class clazz = Class.forName("models."+entityName);
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
        if (!"".equals(aFilter)) {
          Field afield = null;
          try {
            afield = clazz.getField(aFilter);
          } catch (Exception e) {
            break;
          }
          String className = afield.getType().getName();
          if (className.equals("boolean") || className.equals("java.lang.Boolean")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Boolean.parseBoolean(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("int") || className.equals("java.lang.Integer")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Integer.parseInt(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("long") || className.equals("java.lang.Long")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Long.parseLong(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("double") || className.equals("java.lang.Double")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Double.parseDouble(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("float") || className.equals("java.lang.Float")) {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), Float.parseFloat(StringUtils.stripToEmpty(filters.get(key).asString())));
          } else if (className.equals("java.util.Date")) {
            String filterDate = StringUtils.stripToEmpty(filters.get(key).asString());
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date date = format.parse(filterDate);
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), date );
          } else {
            q.addFilter(aFilter, Export.OPERATORS.get(s.get(1).toUpperCase()), StringUtils.stripToEmpty(filters.get(key).asString()));
          }
        }
      }
    }

    // open the blob file to begin writing
    String path = export.blobFullPath;// Variable.get(entityName + "_blob_full_path");//Variable.set(entityName + "_blob_full_path"," ");
    if (StringUtils.isBlank(path) || " ".equals(path)) {
      path = createNewBlobFile(entityName, properties);
//      path = Variable.get(entityName + "_blob_full_path");
    }
    Json conditions = Json.map();
    try{
      conditions = Json.loads(export.conditions);
    } catch(Exception e) {
      
    }
    AppEngineFile file = new AppEngineFile(path);

    // preapre query to fetch results
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    PreparedQuery pq = datastore.prepare(q);
    QueryResultList<Entity> results = pq.asQueryResultList(fetchOptions);

    if (!results.isEmpty()) {
      //initiate a writer and let the blob not close to write within in the future
      FileWriteChannel writeChannel = FileServiceFactory.getFileService().openWriteChannel(file, false);// false mean the file will not be finalize to write again
      PrintWriter out = new PrintWriter(Channels.newWriter(writeChannel, "UTF8"));
      // iterate results and write on blob
      for (Entity entity : results) {
        writeEntity(entity, out, properties, conditions);
      }
      // close file 
      out.close();

      // get last cursor and set within variable
      if (results.getCursor() != null) {
        String cursorString = results.getCursor().toWebSafeString();
//        Variable.set(entityName + "_cursor", cursorString);
        
        export.blobFullPath = file.getFullPath();//Variable.get(entityName + "_blob_full_path");
        export.cursor = cursorString;
        export.update();
        
        launchNextExport(entityName,token, filters.toString(), properties, email);
      } else {
        closeBlob(file, entityName, token, filters, properties, email);
      }
    } else {
      closeBlob(file, entityName, token, filters, properties, email);

    }

  }

  private static void closeBlob(AppEngineFile file, String entityName, String token, Json filters, List<String> properties, String email) throws FileNotFoundException, FinalizationException, LockException, IOException {
    // This time lock because we intend to finalize
    FileWriteChannel writeChannel = FileServiceFactory.getFileService().openWriteChannel(file, true);
    writeChannel.closeFinally();
    stockExportInformation(file.getFullPath(), "__done__", entityName, token, filters.toString(), properties, email);
    Variable.set(entityName + "_cursor", " ");
    Variable.set(entityName + "_blob_full_path", " ");
  }

  private static Export stockExportInformation(String blobFullPath, String cursor, String entityName,String token,  String filters, List<String> properties, String email) {
    Export export =  Export.all().filter("token", token).get();
    export.blobFullPath = blobFullPath;
    export.status = Export.STATUS_DONE;
    export.cursor = cursor;
    export.update();
    export.sendExportMail();
    
    return export;
  }

  public static void launchNextExport(String entityName, String token, String filters, List<String> properties, String email) {
    String strProperties = StringUtils.join(properties, ",");
    Queue queue = QueueFactory.getQueue("export-queue");
    TaskOptions to = Builder.withUrl("/export/" + entityName+"/"+token).method(Method.POST).param("properties", strProperties).param("filters", filters).param("email", email==null?"":email);
    queue.add(to);
  }

  private static void writeEntity(Entity entity, PrintWriter out, List<String> properties, Json conditions) {
    if( !conditions.isEmpty()){
      for(String key : conditions.keys()){
        String value = (String) entity.getProperty(key);
        if(value == null || !value.matches(conditions.get(key).asString())) {
          return;
        }
      }
    }
    Map<String, Object> mapProperties = entity.getProperties();
    if (properties == null) {
      for (Iterator<String> itProp = mapProperties.keySet().iterator(); itProp.hasNext();) {
        String property = itProp.next();
        // write on the blob
        out.print(mapProperties.get(property));
        //check if we reach the last element
        if (itProp.hasNext()) {
          out.print(Export.CSV_SEPARATOR);
        }
      }
      out.println();
    } else {
      for (Iterator<String> itProp = properties.iterator(); itProp.hasNext();) {
        String property = itProp.next();
        property = StringUtils.stripToEmpty(property);
        // write on the blob
        String value = "\"";
        if("id".equalsIgnoreCase(property) && entity.getKey() != null) {
          value += entity.getKey().getId() ;
        } else {
          Object o = mapProperties.get(property);
          
          if(o instanceof java.util.Date) {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");
            value += format.format(o);
          }else{
            value += String.valueOf(o).replaceAll("\"", "\\\"") ;
          }
        }
        value+= "\"";
        out.print(value);
        //check if we reach the last element
        if (itProp.hasNext()) {
          out.print(Export.CSV_SEPARATOR);
        }
      }
      out.println();
    }
  }

  public static siena.Query<Export> all() {
    return Model.all(Export.class);
  }

  public void sendExportMail(){
    notifiers.export.MailerNotification.sendExportNotifiation(this.email, this.entity, this.token);
  }

  public String toString() {
    return new Gson().toJson(this) + "\n";
  }

  public static Export requestExport(String entity, String filters, List<String> properties, String email, String conditions) {
    Export export = new Export();
    export.entity = entity;
    export.email = StringUtils.trimToEmpty(email);
    if (StringUtils.isBlank(export.email)) {
      export.email = Export.EMAIL_ADMIN;
    } 
    export.date = new Date();
    export.filters = filters == null?Json.map().toString():filters;
    export.properties = StringUtils.join(properties, ",");
    export.conditions = conditions== null?Json.map().toString():conditions;
    export.token = Codec.hexSHA1(new Date().getTime() + entity + export.email + (Math.random() * 9999999));
    export.status = STATUS_PENDING;
    export.insert();
    
    return export;
  }

}
