/*
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

package org.apache.druid.data.input.parquet.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestUtil 
{
  public static boolean compareJsonObjects(JsonNode node1, JsonNode node2) 
  {
    if (node1.equals(node2)) {
      return true;
    }

    if (node1.isObject() && node2.isObject()) {
      Map<String, JsonNode> map1 = new ObjectMapper().convertValue(node1, Map.class);
      Map<String, JsonNode> map2 = new ObjectMapper().convertValue(node2, Map.class);

      return compareMaps(map1, map2);
    }

    if (node1.isArray() && node2.isArray()) {
      List<JsonNode> list1 = new ObjectMapper().convertValue(node1, ArrayList.class);
      List<JsonNode> list2 = new ObjectMapper().convertValue(node2, ArrayList.class);

      return compareLists(list1, list2);
    }

    return false;
  }

  public static boolean compareMaps(Map<String, JsonNode> map1, Map<String, JsonNode> map2) 
  {
    if (map1.size() != map2.size()) {
      return false;
    }

    for (Map.Entry<String, JsonNode> entry : map1.entrySet()) {
      if (!map2.containsKey(entry.getKey())) {
        return false;
      }

      if (!compareJsonObjects(entry.getValue(), map2.get(entry.getKey()))) {
        return false;
      }
    }

    return true;
  }

  public static boolean compareLists(List<JsonNode> list1, List<JsonNode> list2) 
  {
    if (list1.size() != list2.size()) {
      return false;
    }

    List<JsonNode> sortedList1 = new ArrayList<>(list1);
    List<JsonNode> sortedList2 = new ArrayList<>(list2);

    Collections.sort(sortedList1, (x, y) -> compareJsonObjects(x, y) ? 0 : -1);
    Collections.sort(sortedList2, (x, y) -> compareJsonObjects(x, y) ? 0 : -1);

    for (int i = 0; i < sortedList1.size(); i++) {
      if (!compareJsonObjects(sortedList1.get(i), sortedList2.get(i))) {
        return false;
      }
    }

    return true;
  }

} 
