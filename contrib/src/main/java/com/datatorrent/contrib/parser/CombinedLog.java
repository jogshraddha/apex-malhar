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

public class CombinedLog implements Log {

  private String host;
  private String rfc931;
  private String userName;
  private String datetime;
  private String request;
  private String statusCode;
  private String bytes;
  private String referrer;
  private String user_agent;
  private String cookie;

  @Override
  public Log getLog(String log){
    String pattern="^([0-9.]+) ([w. -]+) (.*?) \\[(.*?)\\] \"((?:[^\"]|\")+)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\"(.*?)\"";
    Pattern compile = Pattern.compile(pattern);
    Matcher m = compile.matcher(log);

    if (m.find()) {
      this.setHost(m.group(1));
      this.setRfc931(m.group(2));
      this.setUserName(m.group(3));
      this.setDatetime(m.group(4));
      this.setRequest(m.group(5));
      this.setStatusCode(m.group(6));
      this.setBytes(m.group(7));
      this.setReferrer(m.group(8));
      this.setUser_agent(m.group(9));
    }
    return this;
  }

  @Override
  public String toString(){
    return "CombinedLog [ Host : " + this.getHost() +
      ", rfc931 : " + this.getRfc931() +
      ", userName : " + this.getUserName() +
      ", dateTime : " + this.getDatetime() +
      ", request : " + this.getRequest() +
      ", statusCode : " + this.getStatusCode() +
      ", bytes : " + this.getBytes() +
      ", referrer : " + this.getReferrer() +
      ", user_agent : " + this.getUser_agent() +" ]";
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getRfc931() {
    return rfc931;
  }

  public void setRfc931(String rfc931) {
    this.rfc931 = rfc931;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getDatetime() {
    return datetime;
  }

  public void setDatetime(String datetime) {
    this.datetime = datetime;
  }

  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }

  public String getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(String statusCode) {
    this.statusCode = statusCode;
  }

  public String getBytes() {
    return bytes;
  }

  public void setBytes(String bytes) {
    this.bytes = bytes;
  }

  public String getReferrer() {
    return referrer;
  }

  public void setReferrer(String referrer) {
    this.referrer = referrer;
  }

  public String getUser_agent() {
    return user_agent;
  }

  public void setUser_agent(String user_agent) {
    this.user_agent = user_agent;
  }

  public String getCookie() {
    return cookie;
  }

  public void setCookie(String cookie) {
    this.cookie = cookie;
  }
}
