package edu.umich.sdc.controller.dt;

import org.apache.commons.io.FileUtils;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract digital twin
 */
public class TopoDigitalTwin extends DigitalTwin {
  public static Map<String, List<String>> connectionMap = new HashMap<String, List<String>>();
  public static Map<String,List<Integer>> capMap= new HashMap<String, List<Integer>>();

  public void run() {
    try {
      getTopo();
      getCap();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean isConnected(String s1, String s2){
    Map<String, Integer> helpMap = new HashMap<String, Integer>();
     int i=0;
    for (String key : connectionMap.keySet()) {
      helpMap.put(key, i);
      i++;
    }
    QuickUnion quickUnion = new QuickUnion(i);
    for (String key : connectionMap.keySet()) {
      List<String> neigh = connectionMap.get(key);
      for (int j = 0; j < neigh.size(); j++) {
       quickUnion.union(helpMap.get(key),helpMap.get(neigh.get(j)));
      }
    }
    return quickUnion.find(helpMap.get(s1),helpMap.get(s2));
  }


  private void getCap() {
    List cap1 =new ArrayList<Integer>();
    cap1.add(1);
    cap1.add(3);
    List cap2 =new ArrayList<Integer>();
    cap2.add(2);
    capMap.put("CNC1",cap1);
    capMap.put("CNC2",cap1);
    capMap.put("CNC3",cap2);
    capMap.put("CNC4",cap2);
  }

  public void update() {
    System.out.println("Update topo digital twin");
  }

  public Object query() {
    System.out.println("Query topo digital twin");
    return null;
  }

  public void getTopo() throws IOException {
    String fileName = "./resources/config/layout.json";
    try {
      File file = new File(fileName);
      String json = FileUtils.readFileToString(file);
      Gson gson = new Gson();
      JsonParser parser = new JsonParser();
      JsonObject jsonObject = parser.parse(json).getAsJsonObject();

      JsonArray jsonArray = jsonObject.getAsJsonArray("Link");
      for (int i = 0; i < jsonArray.size(); i++) {
        JsonElement el = jsonArray.get(i);
        Layout data = gson.fromJson(el, Layout.class);
        connectionMap.put(data.getNodeName(), data.getconnectedNode());
      }
    } catch (IOException e) {
      System.out.println(e);
    }
    System.out.println(connectionMap);
  }
}

abstract class Node {
}

class SensorNode extends Node {

}

/**
 * Nodes that can process a part
 */
abstract class MachineNode extends Node {
  protected int capacity;

  public abstract void process();
}

class CncNode extends MachineNode {
  public void process() {

  }
}

class RobotNode extends MachineNode {
  public void process() {

  }
}

abstract class Link {
  protected Node source;
  protected Node target;
  protected boolean mAlive;
}

class NetworkLink extends Link {

}

class PhysicalLink extends Link {

}

class Layout {
  private String nodeName;
  private List<String> connectedNode;

  public String getNodeName() {
    return nodeName;
  }

  public List<String> getconnectedNode() {
    return connectedNode;
  }

  public void setNodeName(String name) {
    this.nodeName = name;
  }

  public void setConnectedNode(List<String> list) {
    this.connectedNode = list;
  }
}

class QuickUnion{
  int[] id;

  public QuickUnion(int n) {
    this.id = new int[n];
    for (int i = 0; i < n; i++) {
      id[i] = i;
    }
  }

  public void union(int p, int q) {
    int rootP = getRoot(p);
    int rootQ = getRoot(q);
    id[rootQ] = rootP;
  }

  public boolean find(int p, int q) {
    return getRoot(p) == getRoot(q);
  }

  private int getRoot(int i) {
    while (i != id[i]) {
      i = id[i];
    }
    return i;
  }
}

