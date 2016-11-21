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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.parser.Parser;
import com.datatorrent.lib.util.KeyValPair;

public class LogParser extends Parser<byte[], KeyValPair<String, String>>
{
  protected transient Class<?> clazz;

  private String logFileFormat;

  private LogSchemaDetails logSchemaDetails;

  Log log;

  private transient ObjectMapper objMapper;

  @Override
  public Object convert(byte[] tuple)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public KeyValPair<String, String> processErrorTuple(byte[] bytes)
  {
    return null;
  }

  /**
   * output port to emit validate records as POJO
   */
  public transient DefaultOutputPort<Object> parsedOutput = new DefaultOutputPort<Object>()
  {
    public void setup(Context.PortContext context)
    {
      clazz = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  /**
   * metric to keep count of number of tuples emitted on {@link #parsedOutput}
   * port
   */
  @AutoMetric
  long parsedOutputCount;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    parsedOutputCount = 0;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    objMapper = new ObjectMapper();
    String logFormat = this.geLogFileFormat();
    logger.info("Received logFileFormat as : " + logFormat);
    if(DefaultLogs.logTypes.containsKey(logFormat)) {
      logger.info("Parsing logs from default log formats");
      log = DefaultLogs.logTypes.get(logFormat);
    } else {
      logger.info("Parsing logs from custom log formats");
      try {
        //Set log schema details(fields and regex) according to the given logFormat
        this.logSchemaDetails = new LogSchemaDetails(logFormat);
      } catch (Exception e) {
        logger.error("Error while initializing the custom format " + e.getMessage());
      }
    }
  }

  @Override
  public void processTuple(byte[] inputTuple)
  {
    if (inputTuple == null) {
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(null, "null tuple"));
      }
      errorTupleCount++;
      return;
    }

    String incomingString = new String(inputTuple);
    logger.info("Input string {} ", incomingString);

    try {
      if(this.logSchemaDetails != null) {
        logger.info("Parsing with CUSTOM log format has been started {}", this.geLogFileFormat());
        String pattern = createPattern();
        if (parsedOutput.isConnected()) {
          parsedOutput.emit(objMapper.readValue(createJsonFromLog(incomingString, pattern).toString().getBytes(), clazz));
          parsedOutputCount++;
        }
      } else {
        logger.info("Parsing with DEFAULT log format " + this.geLogFileFormat());
        Log parsedLog = log.getLog(incomingString);
        if(parsedLog != null && parsedOutput.isConnected()) {
          parsedOutput.emit(parsedLog);
          logger.info("Emitting parsed object ");
          parsedOutputCount++;
        } else {
          throw new NullPointerException("Could not parse the log");
        }
      }
    } catch (Exception e) {
      logger.error("Error while parsing the logs " + e.getMessage());
      errorTupleCount++;
      if (err.isConnected()) {
        err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
      }
    }
  }

  /**
   * creates regex group pattern from the regex given for each field
   *
   * @return pattern
   */
  public String createPattern()
  {
    String pattern = "";
    for(LogSchemaDetails.Field field: this.logSchemaDetails.getFields()) {
      pattern = pattern + field.getRegex() + " ";
    }
    logger.info("Created pattern for parsing the log {}", pattern.trim());
    return pattern.trim();
  }

  /**
   * creates json object by matching the log with given pattern
   *
   * @param log
   * @param pattern
   * @return logObject
   * @throws Exception
   */
  public JSONObject createJsonFromLog(String log, String pattern) throws Exception
  {
    Pattern compile = Pattern.compile(pattern);
    Matcher m = compile.matcher(log);
    int count = m.groupCount();
    int i = 1;
    JSONObject logObject = new JSONObject();
    if(m.find()) {
      for(String field: this.logSchemaDetails.getFieldNames()) {
        if(i == count) {
          break;
        }
        logObject.put(field, m.group(i));
        i++;
      }
    } else {
      throw new Exception("No match found for log : " + log);
    }
    logger.info("Json created {}", logObject);
    return logObject;
  }

  /**
   * Set log file format required for parsing the log
   *
   * @param logFileFormat
   */
  public void setLogFileFormat(String logFileFormat)
  {
    this.logFileFormat = logFileFormat;

  }

  /**
   * Get log file format required for parsing the log
   *
   * @return logFileFormat
   */
  public String geLogFileFormat()
  {
    return logFileFormat;
  }

  /**
   * Get log schema details (field, regex etc)
   *
   * @return logSchemaDetails
   */
  public LogSchemaDetails getLogSchemaDetails() {
    return logSchemaDetails;
  }

  /**
   * Set log schema details like (fields and regex)
   *
   * @param logSchemaDetails
   */
  public void setLogSchemaDetails(LogSchemaDetails logSchemaDetails) {
    this.logSchemaDetails = logSchemaDetails;
  }

  /**
   * Get the class that needs to be formatted
   *
   * @return Class<?>
   */
  public Class<?> getClazz()
  {
    return clazz;
  }

  /**
   * Set the class of tuple that needs to be formatted
   *
   * @param clazz
   */
  public void setClazz(Class<?> clazz)
  {
    this.clazz = clazz;
  }

  private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
}