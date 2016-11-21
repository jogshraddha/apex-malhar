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
package com.datatorrent.contrib.parser;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LogSchemaDetails {

  /**
   * This holds the list of field names in the same order as in the schema
   */
  private List<String> fieldNames = new LinkedList();

  private List<Field> fields = new LinkedList();

  /**
   * This holds regex pattern for the schema
   */
  private String pattern;

  public LogSchemaDetails(String json) throws JSONException, IOException
  {
    try {
      initialize(json);
      createPattern();
    } catch (JSONException | IOException e) {
      logger.error("{}", e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * For a given json string, this method sets the field members
   *
   * @param json
   * @throws JSONException
   * @throws IOException
   */
  private void initialize(String json) throws JSONException, IOException
  {
    JSONObject jo = new JSONObject(json);
    JSONArray fieldArray = jo.getJSONArray("fields");

    for(int i = 0; i < fieldArray.length(); ++i) {
      JSONObject obj = fieldArray.getJSONObject(i);
      Field field = new Field(obj.getString("field"), obj.getString("regex"));
      this.fields.add(field);
      this.fieldNames.add(field.name);
    }
  }

  /**
   * creates regex group pattern from the regex given for each field
   *
   */
  public void createPattern()
  {
    String pattern = "";
    for(Field field: this.getFields()) {
      pattern = pattern + field.getRegex() + " ";
    }
    logger.info("Created pattern for parsing the log {}", pattern.trim());
    this.setPattern(pattern.trim());
  }

  /**
   * Get the list of fieldNames mentioned in schema
   *
   * @return fieldNames
   */
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  /**
   * Get the list of fields (field, regex) mentioned in schema
   *
   * @return fields
   */
  public List<Field> getFields()
  {
    return fields;
  }

  /**
   *
   * Get the regex pattern for the schema
   * @return pattern
   */
  public String getPattern()
  {
    return pattern;
  }

  /**
   * Set the regex pattern for schema
   *
   * @param pattern
   */
  public void setPattern(String pattern)
  {
    this.pattern = pattern;
  }

  public class Field
  {
    /**
     * name of the field
     */
    String name;
    /**
     * regular expression for the field
     */
    String regex;


    public Field(String name, String regex)
    {
      this.name = name;
      this.regex = regex;
    }

    /**
     * Get the name of the field
     *
     * @return name
     */
    public String getName()
    {
      return name;
    }

    /**
     * Set the name of the field
     *
     * @param name
     */
    public void setName(String name)
    {
      this.name = name;
    }

    /**
     * Get the regular expression of the field
     *
     * @return
     */
    public String getRegex()
    {
      return regex;
    }

    /**
     * Set the regular expression of the field
     *
     * @param regex
     */
    public void setRegex(String regex)
    {
      this.regex = regex;
    }

    @Override
    public String toString()
    {
      return "Fields [name=" + name + ", regex=" + regex +"]";
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(LogSchemaDetails.class);
}
