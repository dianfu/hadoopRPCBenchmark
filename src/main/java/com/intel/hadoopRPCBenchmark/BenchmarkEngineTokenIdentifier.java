/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hadoopRPCBenchmark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BenchmarkEngineTokenIdentifier extends TokenIdentifier {
  private Text tokenid;
  private Text realUser;
  final static Text KIND_NAME = new Text("benchmarkengine.token");

  public BenchmarkEngineTokenIdentifier() {
    this(new Text(), new Text());
  }
  public BenchmarkEngineTokenIdentifier(Text tokenid) {
    this(tokenid, new Text());
  }
  public BenchmarkEngineTokenIdentifier(Text tokenid, Text realUser) {
    this.tokenid = tokenid == null ? new Text() : tokenid;
    this.realUser = realUser == null ? new Text() : realUser;
  }
  @Override
  public Text getKind() {
    return KIND_NAME;
  }
  @Override
  public UserGroupInformation getUser() {
    if (realUser.toString().isEmpty()) {
      return UserGroupInformation.createRemoteUser(tokenid.toString());
    } else {
      UserGroupInformation realUgi = UserGroupInformation
        .createRemoteUser(realUser.toString());
      return UserGroupInformation
        .createProxyUser(tokenid.toString(), realUgi);
    }
  }

  public void readFields(DataInput in) throws IOException {
    tokenid.readFields(in);
    realUser.readFields(in);
  }
  public void write(DataOutput out) throws IOException {
    tokenid.write(out);
    realUser.write(out);
  }
}
