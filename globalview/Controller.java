package edu.umich.sdc.controller;


import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import edu.umich.sdc.controller.dt.*;
import edu.umich.sdc.south.DataConsumerLocal;
import edu.umich.sdc.south.IDataConsumer;
import edu.umich.sdc.south.Tag;
import org.bson.Document;

import static java.lang.Thread.sleep;

/**
 * Each app is interested in a list of keys. This class binds an app and the sets of interested keys.
 */
class RequestTag {
  private String source;
  private List<String> keys;

  public RequestTag(String source, List<String> keys) {
    this.source = source;
    keys = keys;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public List<String> getKeys() {
    return keys;
  }

  public void setKeys(List<String> keys) {
    this.keys = keys;
  }

  @Override
  public String toString() {
    return "RequestTag [source=" + source + ", keys=" + keys.toString() + "]";
  }
}

public class Controller {

  public static int count = 0;

  public static void main(String[] args) {
    try {
      File file = new File("./resources/config/config.json");
      Gson gson = new Gson();
      JsonReader reader = new JsonReader(new FileReader(file));
      Type type = new TypeToken<List<String>>() {}.getType();

      List<String> apps = gson.fromJson(reader, type);

      List<RequestTag> tags = new ArrayList<RequestTag>();
      for (String app : apps) {
        file = new File("./resources/config/" + app + ".config.json");
        gson = new Gson();
        reader = new JsonReader(new FileReader(file));
        type = new TypeToken<List<RequestTag>>() {}.getType();
        List<RequestTag> interests = gson.fromJson(reader, type);
        tags.addAll(interests);
      }
      Controller controller = new Controller();

      controller.run(tags);

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("SDC Controller exiting...");
    }
  }

  public void run(List<RequestTag> tags) {
    IDataConsumer c = new DataConsumerLocal();

    try {
      TopoDigitalTwin dt = new TopoDigitalTwin();
      dt.run();
      MongoClient mongoClient = new MongoClient("localhost", 27017);
      MongoDatabase old_mongoDatabase = mongoClient.getDatabase("sdc");
      old_mongoDatabase.drop();
      MongoDatabase mongoDatabase = mongoClient.getDatabase("sdc");
      mongoDatabase.createCollection("CNC1");
      mongoDatabase.createCollection("CNC2");
      mongoDatabase.createCollection("CNC3");
      mongoDatabase.createCollection("CNC4");

      mongoDatabase.createCollection("STOPPER1");
      mongoDatabase.createCollection("STOPPER2");
      mongoDatabase.createCollection("STOPPER3");
      mongoDatabase.createCollection("STOPPER4");
      mongoDatabase.createCollection("QC1");
      mongoDatabase.createCollection("QC2");
      mongoDatabase.createCollection("ROBOT1");
      mongoDatabase.createCollection("ROBOT2");
<<<<<<< HEAD
     /* generateData(mongoDatabase,dt,"CNC1");
      generateData(mongoDatabase,dt,"CNC2");
      generateData(mongoDatabase,dt,"CNC3");
      generateData(mongoDatabase,dt,"CNC4");*/
=======

      /*
      generateData(mongoDatabase,dt,"CNC1");
      generateData(mongoDatabase,dt,"CNC2");
      generateData(mongoDatabase,dt,"CNC3");
      generateData(mongoDatabase,dt,"CNC4");
      */
>>>>>>> 0e00b2d87137db668416a2f4445db3baaa173248
      generateData(mongoDatabase,dt,"STOPPER1");
      generateData(mongoDatabase,dt,"STOPPER2");
      generateData(mongoDatabase,dt,"STOPPER3");
      generateData(mongoDatabase,dt,"STOPPER4");
      generateData(mongoDatabase,dt,"QC1");
      generateData(mongoDatabase,dt,"QC2");
      generateData(mongoDatabase,dt,"ROBOT1");
      generateData(mongoDatabase,dt,"ROBOT2");

      /*
       * read data from southbound interface and store them in database
       */
<<<<<<< HEAD

      DataConsumer c = new DataConsumer("141.212.133.36", 50051);
=======
//      IDataConsumer c = new DataConsumerRemote("141.212.133.36", 50051);

>>>>>>> 0e00b2d87137db668416a2f4445db3baaa173248
      while (true) {
        for (RequestTag t : tags) {
          System.out.println(t.getSource());
          System.out.println(t.getKeys());

          List<Tag> response = c.requestTagList(t.getSource(), t.getKeys());
          parseJsonIntoCollection(mongoDatabase, response, dt);
        }
        Thread.sleep(1000);
      }
<<<<<<< HEAD

=======
>>>>>>> 0e00b2d87137db668416a2f4445db3baaa173248
    } catch (Exception e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    } finally {
      c.disconnect();
    }

  }

  /**
   * Write data into DB
   *
   * @param mongoDatabase
   * @param tags
   * @param dt
   * @throws IOException
   */
  public void parseJsonIntoCollection(MongoDatabase mongoDatabase, List<Tag> tags, TopoDigitalTwin dt) throws IOException {
    count++;
    for (int i = 0; i < tags.size(); i++) {
      String source = tags.get(i).getPath();
      JsonObject dataObj = new JsonObject();
      //dataObj.addProperty("time", new Timestamp(new Date().getTime()).toString());

      String tagname = tags.get(i).getName();
      dataObj.addProperty("tag_name", tagname);
      switch (tags.get(i).getTagvalueCase()) {
        case INT_VALUE:
          dataObj.addProperty("value", tags.get(i).getIntValue());
          dataObj.addProperty("type", "int");
          break;

        case STR_VALUE:
          dataObj.addProperty("value", tags.get(i).getStrValue());
          dataObj.addProperty("type", "str");

          break;

        case FLOAT_VALUE:
          dataObj.addProperty("value", tags.get(i).getFloatValue());
          dataObj.addProperty("type","float");
          break;

        case DOUBLE_VALUE:
          double value = tags.get(i).getDoubleValue();
          dataObj.addProperty("type","double");
          if (tagname.indexOf("DCBusVoltage") != -1 && value == 0) {
            dt.connectionMap.remove(source);
            for (String key : dt.connectionMap.keySet()) {
              dt.connectionMap.get(key).remove(source);
            }
          }
          break;
      }
      dataObj.addProperty("link_relationship", TopoDigitalTwin.connectionMap.get(source).toString());
      Document doc = Document.parse(dataObj.toString());
      doc.append("time", new Date());

      mongoDatabase.getCollection(source).insertOne(doc);
    }
  }

  /**
   * Generates data for test
   *
   * @param mongoDatabase
   * @param dt
   * @param source
   * @throws IOException
   */
  public void generateData(MongoDatabase mongoDatabase, TopoDigitalTwin dt, String source) throws InterruptedException {
    if(source.contains("CNC")) {
      List<String> tagnames = new ArrayList<String>();
      tagnames.add("PROGRAM_NUMBER");
      tagnames.add("ipinfo.IP_Address");
      tagnames.add("ipinfo.Subnet");
      tagnames.add("ipinfo.Gateway");
      tagnames.add("ipinfo.DNS_Pri");
      tagnames.add("ipinfo.DNS_Sec");
      tagnames.add("ipinfo.DomainName");
      tagnames.add("Spindle_Drive:SI.STATUS");
      tagnames.add("Spindle_Drive:SI.ConnectionStatus");
      tagnames.add("Spindle_Drive:SI.RunMode");
      tagnames.add("Spindle_Drive:SI.ConnectionFaulted");
      tagnames.add("Spindle_Drive:SI.ResetRequired");
      tagnames.add("Spindle_Drive:SI.SafetyFault");


      for (int i = 0; i < 3; i++) {
        JsonObject dataObj = new JsonObject();
        for (int j = 0; j < 13; j++) {
          dataObj.addProperty("tag_name", tagnames.get(j));
          if (j == 0 || (7 <= j && j <= 11)) {
            dataObj.addProperty("value", j + i);
            dataObj.addProperty("type", "int");
          } else {
            dataObj.addProperty("value", "GoBlue");
            dataObj.addProperty("type", "str");
          }

          dataObj.addProperty("link_relationship", dt.connectionMap.get(source).toString());
          dataObj.addProperty("capability", dt.capMap.get(source).toString());
          Document doc = Document.parse(dataObj.toString());
          doc.append("time", new Date());
          mongoDatabase.getCollection(source).insertOne(doc);
          sleep(500);
        }
      }
    }else{
      JsonObject dataObj = new JsonObject();
      dataObj.addProperty("link_relationship", dt.connectionMap.get(source).toString());
      Document doc = Document.parse(dataObj.toString());
      doc.append("time", new Date());
      mongoDatabase.getCollection(source).insertOne(doc);
      sleep(500);
    }

  }

}
