package org.apache.lens.server.api.authorization;

import java.io.*;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ADGroupService {

  public static Map<String, String> getAttributes(String serverUrl, String[] lookupFields, String userName, String pwd) throws IOException, JSONException {

    Map<String , String> res = new HashMap<>();
    JSONArray jsonObject = readJsonFromUrl(serverUrl ,userName, pwd);

    for(int i = 0; i < lookupFields.length; i ++) {
      if(lookupFields[i].equals("memberOf")) {
        List<String> adGroupList = new ArrayList<>();
        JSONArray jsonStringArray =  (JSONArray) ((JSONObject)jsonObject.get(0)).get("memberOf");
        for(int j = 0 ; j < jsonStringArray.length() ; j++){
          adGroupList.add(jsonStringArray.get(j).toString());
        }
        res.put(lookupFields[i],adGroupList.toString());
      }else
        res.put(lookupFields[i],null);
    }
    return res;
  }

  private static JSONArray readJsonFromUrl(String url, String username, String pwd) throws IOException, JSONException {
    String authString = username + ":" + pwd;
    byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
    String authStringEnc = new String(authEncBytes);
    URLConnection urlConnection = new URL(url).openConnection();
    urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
    urlConnection.setRequestProperty("Content-Type", "application/json");
    urlConnection.setRequestProperty("Accept", "application/json");
    InputStream is = urlConnection.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);

    int numCharsRead;
    char[] charArray = new char[1024];
    StringBuilder sb = new StringBuilder();
    while ((numCharsRead = isr.read(charArray)) > 0) {
      sb.append(charArray, 0, numCharsRead);
    }
    return new JSONArray(sb.toString());
  }

  public static void main(String[] args) throws IOException, JSONException {
    //JSONObject json = readJsonFromUrl("http://whobot:Na9JrqpmC7gWMoacJr5Iv6AMIULZrQSU@who.corp.inmobi.com/api/v1/user/rajitha.r");
   // System.out.println(json.toString());
    //System.out.println(json.get("id"));
  }
}
