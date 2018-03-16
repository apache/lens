package org.apache.lens.server.api.authorization;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;

public class ADGroupService {

  public static String getGroups(String serverUrl) throws IOException, JSONException {
    String adgroupList = null;
    JSONObject jsonObject = readJsonFromUrl(serverUrl);
    System.out.println("HERE IN AD SERVER");
    System.out.println(jsonObject.getJSONArray("memberOf"));
    return adgroupList;

  }

  private static String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
    InputStream is = new URL(url).openStream();
    try {
      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
      String jsonText = readAll(rd);
      JSONObject json = new JSONObject(jsonText);
      return json;
    } finally {
      is.close();
    }
  }

  public static void main(String[] args) throws IOException, JSONException {
    JSONObject json = readJsonFromUrl("http://whobot:Na9JrqpmC7gWMoacJr5Iv6AMIULZrQSU@who.corp.inmobi.com/api/v1/user/rajitha.r");
    System.out.println(json.toString());
    //System.out.println(json.get("id"));
  }
}
