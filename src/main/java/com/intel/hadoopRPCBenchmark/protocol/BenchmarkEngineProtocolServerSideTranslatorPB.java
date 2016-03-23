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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.intel.hadoopRPCBenchmark.protocol.proto.BenchmarkEngineProtocolProtos.PingRequestProto;
import com.intel.hadoopRPCBenchmark.protocol.proto.BenchmarkEngineProtocolProtos.PingResponseProto;

import java.io.IOException;

public class BenchmarkEngineProtocolServerSideTranslatorPB implements
  BenchmarkEngineProtocolPB {
  private static final PingResponseProto VOID_PING_RESPONSE =
    PingResponseProto.newBuilder().build();

  private final BenchmarkEngineProtocol impl;

  public BenchmarkEngineProtocolServerSideTranslatorPB(BenchmarkEngineProtocol impl) {
    this.impl = impl;
  }

  public PingResponseProto ping(RpcController controller, PingRequestProto request)
    throws ServiceException {
    byte[] payload = request.getPayload().toByteArray();
    try {
      impl.ping(payload);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_PING_RESPONSE;
  }
}
