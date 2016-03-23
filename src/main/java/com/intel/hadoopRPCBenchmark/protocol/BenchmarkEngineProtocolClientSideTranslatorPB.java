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
package com.intel.hadoopRPCBenchmark.protocol;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.intel.hadoopRPCBenchmark.protocol.proto.BenchmarkEngineProtocolProtos.PingRequestProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.InetSocketAddress;

public class BenchmarkEngineProtocolClientSideTranslatorPB implements
  BenchmarkEngineProtocol, Closeable {

  /** RpcController is not used and hence is set to null */
  private final BenchmarkEngineProtocolPB rpcProxy;
  private final static RpcController NULL_CONTROLLER = null;

  public BenchmarkEngineProtocolClientSideTranslatorPB(InetSocketAddress nameNodeAddr,
                                                Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, BenchmarkEngineProtocolPB.class,
      ProtobufRpcEngine.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxy = RPC.getProtocolProxy(BenchmarkEngineProtocolPB.class,
      RPC.getProtocolVersion(BenchmarkEngineProtocolPB.class), nameNodeAddr, ugi,
      conf, NetUtils.getSocketFactory(conf, BenchmarkEngineProtocolPB.class),
      org.apache.hadoop.ipc.Client.getPingInterval(conf), null).getProxy();
  }

  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  public void ping(byte[] payload) throws IOException {
    PingRequestProto.Builder builder = PingRequestProto.newBuilder()
      .setPayload(ByteString.copyFrom(payload));
    try {
      rpcProxy.ping(NULL_CONTROLLER, builder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }
}
