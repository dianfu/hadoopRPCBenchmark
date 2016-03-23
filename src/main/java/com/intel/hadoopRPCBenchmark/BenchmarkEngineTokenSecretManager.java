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

import org.apache.hadoop.security.token.SecretManager;

public class BenchmarkEngineTokenSecretManager extends SecretManager<BenchmarkEngineTokenIdentifier> {
  @Override
  public byte[] createPassword(BenchmarkEngineTokenIdentifier id) {
    return id.getBytes();
  }

  @Override
  public byte[] retrievePassword(BenchmarkEngineTokenIdentifier id)
    throws InvalidToken {
    return id.getBytes();
  }

  @Override
  public BenchmarkEngineTokenIdentifier createIdentifier() {
    return new BenchmarkEngineTokenIdentifier();
  }
}
