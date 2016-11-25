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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtendedLog implements Log
{

  private String date = "-";
  private String time = "-";
  private String clientIP = "-";
  private String userName = "-";
  private String siteName = "-";
  private String computerName = "-";
  private String serverIP = "-";
  private String serverPort = "-";
  private String csMethod = "-";
  private String URIStem = "-";
  private String URIQuery = "-";
  private String status = "-";
  private String win32Status = "-";
  private String bytesSent = "-";
  private String bytesReceived = "-";
  private String timeTaken = "-";
  private String protocolVersion = "-";
  private String host = "-";
  private String userAgent = "-";
  private String cookie = "-";
  private String referrer = "-";
  private String subStatus = "-";

  private String pattern;

  private static Map<String,String> fieldRegex = new HashMap<String,String>();
  static
  {
    fieldRegex.put("date", "(\\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2]\\d|3[0-1]))");
    fieldRegex.put("time", "((?:[0-1]\\d|2[0-3]):[0-5]\\d:[0-5]\\d)");
    fieldRegex.put("c-ip", "([0-9.]+)");
    fieldRegex.put("cs-username", "([w. -]+)");
    fieldRegex.put("s-sitename", "(^[a-z0-9_-]{3,16}$)");
    fieldRegex.put("s-computername", "(^[a-z0-9_-]{3,16}$)");
    fieldRegex.put("s-ip", "([0-9.]+)");
    fieldRegex.put("s-port", "(\\d{2,4})");
    fieldRegex.put("cs-method", "");
    fieldRegex.put("cs-uri-stem", "");
    fieldRegex.put("cs-uri-query", "");
    fieldRegex.put("sc-status", "(\\d{3})");
    fieldRegex.put("sc-win32-status", "(\\d+|-)");
    fieldRegex.put("sc-bytes", "(\\d+|-)");
    fieldRegex.put("cs-bytes", "(\\d+|-)");
    fieldRegex.put("time-taken", "(\\s*(\\d*)\\s*ms)");
    fieldRegex.put("cs-version", "");
    fieldRegex.put("cs-host", "");
    fieldRegex.put("cs(User-Agent)", "(\"(.*?)\")");
    fieldRegex.put("cs(Cookie)", "(\\w+=.+)");
    fieldRegex.put("cs(Referrer)", "((http|https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$)");
    fieldRegex.put("sc-substatus", "(\\d+|-)");
  }

  private static Map<String,String> fieldNames = new HashMap<String,String>();
  static
  {
    fieldNames.put("date", "date");
    fieldNames.put("time", "time");
    fieldNames.put("c-ip", "clientIP");
    fieldNames.put("cs-username", "userName");
    fieldNames.put("s-sitename", "siteName");
    fieldNames.put("s-computername", "computerName");
    fieldNames.put("s-ip", "serverIP");
    fieldNames.put("s-port", "serverPort");
    fieldNames.put("cs-method", "csMethod");
    fieldNames.put("cs-uri-stem", "URIStem");
    fieldNames.put("cs-uri-query", "URIQuery");
    fieldNames.put("sc-status", "status");
    fieldNames.put("sc-win32-status", "win32Status");
    fieldNames.put("sc-bytes", "bytesSent");
    fieldNames.put("cs-bytes", "bytesReceived");
    fieldNames.put("time-taken", "timeTaken");
    fieldNames.put("cs-version", "protocolVersion");
    fieldNames.put("cs-host", "host");
    fieldNames.put("cs(User-Agent)", "userAgent");
    fieldNames.put("cs(Cookie)", "cookie");
    fieldNames.put("cs(Referrer)", "referrer");
    fieldNames.put("sc-substatus", "subStatus");
  }

  public ExtendedLog()
  {

  }

  public ExtendedLog(String[] fields)
  {
    this.fieldSequence = fields;
    this.createPattern();
  }

  /**
   * Create regex pattern according to field sequence
   */
  public void createPattern()
  {
    String patternGroup = "";
    for(String field: this.fieldSequence){
      if(fieldRegex.containsKey(field)){
        patternGroup = patternGroup + fieldRegex.get(field) + " ";
      }
    }
    this.pattern = patternGroup.trim();
  }

  @Override
  public Log getLog(String log) throws Exception
  {
    Pattern compile = Pattern.compile(this.pattern);
    Matcher m = compile.matcher(log);
    Class<?> cls = Class.forName("com.datatorrent.contrib.parser.ExtendedLog");
    int i = 1;
    if (m.find()) {
      for (String fieldName: this.fieldSequence){
        String field = org.apache.commons.lang.StringUtils.capitalize(fieldNames.get(fieldName));
        String methodName = "set" + field;
        Method method = cls.getMethod(methodName, String.class);
        method.invoke(this, m.group(i));
        i++;
      }
    }
    return this;
  }

  @Override
  public String toString()
  {
    return "ExtendedLog [ date : " + this.getDate() +
      ", time : " + this.getTime() +
      ", clientIP : " + this.getClientIP() +
      ", userName : " + this.getUserName() +
      ", siteName : " + this.getSiteName() +
      ", computerName : " + this.getComputerName() +
      ", serverIP : " + this.getServerIP() +
      ", serverPort : " + this.getServerPort() +
      ", csMethod : " + this.getCsMethod() +
      ", URIStem : " + this.getURIStem() +
      ", URIQuery : " + this.getURIQuery() +
      ", status : " + this.getStatus() +
      ", win32Status : " + this.getWin32Status() +
      ", bytesSent : " + this.getBytesSent() +
      ", bytesReceived : " + this.getBytesReceived() +
      ", timeTaken : " + this.getTimeTaken() +
      ", protocolVersion : " + this.getProtocolVersion() +
      ", host : " + this.getHost() +
      ", userAgent : " + this.getUserAgent() +
      ", cookie : " + this.getCookie() +
      ", referrer : " + this.getReferrer() +
      ", subStatus : " + this.getSubStatus() + "]";
  }

  public String[] getFieldSequence()
  {
    return fieldSequence;
  }

  public void setFieldSequence(String[] fieldSequence)
  {
    this.fieldSequence = fieldSequence;
  }

  private String[] fieldSequence;

  public String getDate()
  {
    return date;
  }

  public void setDate(String date)
  {
    this.date = date;
  }

  public String getTime()
  {
    return time;
  }

  public void setTime(String time)
  {
    this.time = time;
  }

  public String getClientIP()
  {
    return clientIP;
  }

  public void setClientIP(String clientIP)
  {
    this.clientIP = clientIP;
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getSiteName()
  {
    return siteName;
  }

  public void setSiteName(String siteName)
  {
    this.siteName = siteName;
  }

  public String getComputerName()
  {
    return computerName;
  }

  public void setComputerName(String computerName)
  {
    this.computerName = computerName;
  }

  public String getServerIP()
  {
    return serverIP;
  }

  public void setServerIP(String serverIP)
  {
    this.serverIP = serverIP;
  }

  public String getServerPort()
  {
    return serverPort;
  }

  public void setServerPort(String serverPort)
  {
    this.serverPort = serverPort;
  }

  public String getCsMethod()
  {
    return csMethod;
  }

  public void setCsMethod(String csMethod)
  {
    this.csMethod = csMethod;
  }

  public String getURIStem()
  {
    return URIStem;
  }

  public void setURIStem(String URIStem)
  {
    this.URIStem = URIStem;
  }

  public String getURIQuery()
  {
    return URIQuery;
  }

  public void setURIQuery(String URIQuery)
  {
    this.URIQuery = URIQuery;
  }

  public String getStatus()
  {
    return status;
  }

  public void setStatus(String status)
  {
    this.status = status;
  }

  public String getWin32Status()
  {
    return win32Status;
  }

  public void setWin32Status(String win32Status)
  {
    this.win32Status = win32Status;
  }

  public String getBytesSent()
  {
    return bytesSent;
  }

  public void setBytesSent(String bytesSent)
  {
    this.bytesSent = bytesSent;
  }

  public String getBytesReceived()
  {
    return bytesReceived;
  }

  public void setBytesReceived(String bytesReceived)
  {
    this.bytesReceived = bytesReceived;
  }

  public String getTimeTaken()
  {
    return timeTaken;
  }

  public void setTimeTaken(String timeTaken)
  {
    this.timeTaken = timeTaken;
  }

  public String getProtocolVersion()
  {
    return protocolVersion;
  }

  public void setProtocolVersion(String protocolVersion)
  {
    this.protocolVersion = protocolVersion;
  }

  public String getHost()
  {
    return host;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public String getUserAgent()
  {
    return userAgent;
  }

  public void setUserAgent(String userAgent)
  {
    this.userAgent = userAgent;
  }

  public String getCookie()
  {
    return cookie;
  }

  public void setCookie(String cookie)
  {
    this.cookie = cookie;
  }

  public String getReferrer()
  {
    return referrer;
  }

  public void setReferrer(String referrer)
  {
    this.referrer = referrer;
  }

  public String getSubStatus()
  {
    return subStatus;
  }

  public void setSubStatus(String subStatus)
  {
    this.subStatus = subStatus;
  }

  public String getPattern()
  {
    return pattern;
  }

  public void setPattern(String pattern)
  {
    this.pattern = pattern;
  }

  public static Map<String, String> getFieldRegex()
  {
    return fieldRegex;
  }

  public static Map<String, String> getFieldNames()
  {
    return fieldNames;
  }

}
