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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.parser.Parser;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that parses a log string tuple against the
 * default log formats or a specified json schema and emits POJO on a parsed port and tuples that could not be
 * parsed on error port.<br>
 * <b>Properties</b><br>
 * <b>jsonSchema</b>:schema as a string<br>
 * <b>clazz</b>:Pojo class in case of user specified schema<br>
 * <b>Ports</b> <br>
 * <b>in</b>:input tuple as a String. Each tuple represents a log<br>
 * <b>parsedOutput</b>:tuples that are validated against the default or user specified schema are emitted
 * as POJO on this port<br>
 * <b>err</b>:tuples that do not confine to log format are emitted on this port as
 * KeyValPair<String,String><br>
 * Key being the tuple and Val being the reason.
 *
 *
 * @displayName LogParser
 * @category Parsers
 * @tags log pojo parser
 * @since 3.6.0
 */
public class LogParser extends Parser<byte[], KeyValPair<String, String>>
{
  private transient Class<?> clazz;

  private String extendedFieldsSeq;

  private String logFileFormat;

  private LogSchemaDetails logSchemaDetails;

  private Log log;

  public void setObjMapper(ObjectMapper objMapper)
  {
    this.objMapper = objMapper;
  }

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
    logger.info("Received logFileFormat as {} ", logFileFormat);
    setupLog();
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
        if (parsedOutput.isConnected()) {
          parsedOutput.emit(objMapper.readValue(this.logSchemaDetails.createJsonFromLog(incomingString).toString().getBytes(), clazz));
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
   *  Setup for the logs according to the logFileFormat
   *
   */
  public void setupLog()
  {
    DefaultLogs defaultLog;
    if(org.apache.commons.lang3.EnumUtils.isValidEnum(DefaultLogs.class, logFileFormat.toUpperCase())){
      defaultLog = DefaultLogs.valueOf(logFileFormat.toUpperCase());
    } else {
      defaultLog = DefaultLogs.CUSTOM;
    }
    switch (defaultLog) {
      case COMMON:
        log = new CommonLog();
        break;
      case COMBINED:
        log = new CombinedLog();
        break;
      case EXTENDED:
        String[] fields = extendedFieldsSeq.split(" ");
        log = new ExtendedLog(fields);
        break;
      case SYS:
        log = new SysLog();
        break;
      default:
        logger.info("Parsing logs from custom log formats");
        try {
          //parse the schema in logFileFormat string
          this.logSchemaDetails = new LogSchemaDetails(logFileFormat);
        } catch (JSONException | IOException e) {
          logger.error("Error while initializing the custom log format " + e.getMessage());
          DTThrowable.wrapIfChecked(e);
        }
        break;
    }
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

  /**
   * Get field sequence of extended log
   *
   * @return extendedFieldsSeq
   */
  public String getExtendedFieldsSeq()
  {
    return extendedFieldsSeq;
  }

  /**
   * Set field sequence of extended log
   *
   * @param extendedFieldsSeq
   */
  public void setExtendedFieldsSeq(String extendedFieldsSeq)
  {
    this.extendedFieldsSeq = extendedFieldsSeq;
  }

  private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
}