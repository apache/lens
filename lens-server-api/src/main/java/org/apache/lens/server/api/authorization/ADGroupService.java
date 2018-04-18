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

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

    if (jsonObject != null) {
      for (String lookupField : lookupFields) {
        if (lookupField.equals("memberOf")) {
          List<String> adGroupList = new ArrayList<>();
          JSONArray jsonStringArray = (JSONArray) ((JSONObject) jsonObject.get(0)).get("memberOf");
          for (int j = 0; j < jsonStringArray.length(); j++) {
            adGroupList.add(jsonStringArray.get(j).toString());
          }
          res.put(lookupField, adGroupList.toString().replace("[", "").replace("]", ""));
        } else {
          res.put(lookupField, null);
        }
      }
    }
    return res;
  }

  private static JSONArray readJsonFromUrl(String url, String username, String pwd) throws IOException, JSONException {
    String authString = username + ":" + pwd;
    String authStringEnc = Base64.getEncoder().encodeToString(authString.getBytes(StandardCharsets.UTF_8));

    HttpURLConnection urlConnection = (HttpURLConnection) new URL(url).openConnection();
    urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
    urlConnection.setRequestProperty("Content-Type", "application/json");
    urlConnection.setRequestProperty("Accept", "application/json");
    if (urlConnection.getResponseCode() == 200) {
      InputStream is = urlConnection.getInputStream();
      InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
      int numCharsRead;
      char[] charArray = new char[2048];
      StringBuilder sb = new StringBuilder();
      while ((numCharsRead = isr.read(charArray)) > 0) {
        sb.append(charArray, 0, numCharsRead);
      }
      return new JSONArray(sb.toString());
    }

    return null;
  }
}
