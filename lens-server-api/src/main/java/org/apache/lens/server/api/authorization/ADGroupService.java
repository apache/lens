/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

  private ADGroupService() {

  }

  public static Map<String, String> getAttributes(String serverUrl, String[] lookupFields, String userName, String pwd)
    throws IOException, JSONException {

    Map<String, String> res = new HashMap<>();
    JSONArray jsonObject = readJsonFromUrl(serverUrl, userName, pwd);

    for(int i = 0; i < lookupFields.length; i++) {
      if (lookupFields[i].equals("memberOf")) {
        List<String> adGroupList = new ArrayList<>();
        JSONArray jsonStringArray =  (JSONArray) ((JSONObject)jsonObject.get(0)).get("memberOf");
        for(int j = 0; j < jsonStringArray.length(); j++){
          adGroupList.add(jsonStringArray.get(j).toString());
        }
        res.put(lookupFields[i], adGroupList.toString().replace("[", "").replace("]", ""));
      }else {
        res.put(lookupFields[i],  null);
      }
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
}
