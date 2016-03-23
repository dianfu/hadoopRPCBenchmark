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

import com.google.protobuf.BlockingService;
import com.intel.hadoopRPCBenchmark.protocol.*;
import com.intel.hadoopRPCBenchmark.protocol.proto.BenchmarkEngineProtocolProtos;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Random;

public class HadoopRPCBenchmarkEngine {
  private static final String ADDRESS = "0.0.0.0";
  public static final String SERVER_PRINCIPAL_KEY =
    "benchmarkengine.ipc.server.principal";
  public static final String SERVER_KEYTAB_KEY =
    "benchmarkengine.ipc.server.keytab";

  private Configuration conf = new Configuration();
  private RPC.Server server;
  private BenchmarkEngineTokenSecretManager sm;
  private EnginePreparer enginePreparer;

  private UserGroupInformation clientUgi;
  private String mode;
  private int packetSize;
  private long packetNum;

  public HadoopRPCBenchmarkEngine(String mode, int packetSize, long packetNum)
    throws IOException {
    if (mode.equalsIgnoreCase("simple")) {
      if (SecurityUtil.getAuthenticationMethod(conf) !=
        UserGroupInformation.AuthenticationMethod.SIMPLE) {
        throw new IOException("Mode is simple, but configuration " +
          "hadoop.security.authentication is kerberos");
      }
      this.enginePreparer = new SimpleBasedEnginePreparer();
    } else if (mode.equalsIgnoreCase("token")) {
      this.enginePreparer = new TokenBasedEnginePreparer();
    } else if (mode.equalsIgnoreCase("kerberos")) {
      if (SecurityUtil.getAuthenticationMethod(conf) ==
        UserGroupInformation.AuthenticationMethod.SIMPLE) {
        throw new IOException("Mode is kerberos, but configuration " +
          "hadoop.security.authentication is simple");
      }
      this.enginePreparer = new KerberosBasedEnginePreparer();
    } else {
      throw new IOException("Mode: " + mode + " not supported!");
    }
    this.mode = mode;
    this.packetSize = packetSize;
    this.packetNum = packetNum;
  }

  interface EnginePreparer {
    void prepareClient() throws Exception;
    void prepareServer() throws Exception;
    void cleanup();
  }

  public class TokenBasedEnginePreparer implements EnginePreparer {
    public void prepareClient() throws IOException {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
      setToken(addr);
    }

    public void prepareServer() {
      sm = new BenchmarkEngineTokenSecretManager();
    }

    public void cleanup() {
      // Do nothing
    }

    private void setToken(InetSocketAddress addr) throws IOException {
      // Set token
      if (sm != null) {
        final UserGroupInformation current = UserGroupInformation.getCurrentUser();
        BenchmarkEngineTokenIdentifier tokenId = new BenchmarkEngineTokenIdentifier(new Text(current.getUserName()));
        Token<BenchmarkEngineTokenIdentifier> token = new Token<BenchmarkEngineTokenIdentifier>(tokenId, sm);
        SecurityUtil.setTokenService(token, addr);
        current.addToken(token);
      }
    }
  }

  public class KerberosBasedEnginePreparer implements EnginePreparer {
    private MiniKdc kdc;
    private String serverPrincipal;
    private String clientPrincipal;
    private String serverKeytab;
    private String clientKeytab;

    public void prepareClient() throws IOException {
      clientUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        clientPrincipal, clientKeytab);
    }

    public void prepareServer() throws Exception {
      prepareKdc();
      SecurityUtil.login(conf, SERVER_KEYTAB_KEY, SERVER_PRINCIPAL_KEY);
    }

    public void cleanup() {
      if (kdc != null) {
        kdc.stop();
      }
    }

    private void prepareKdc() throws Exception {
      File baseDir = new File("target", "kdc");
      FileUtils.deleteDirectory(baseDir);
      FileUtils.forceMkdir(baseDir);

      Properties kdcConf = MiniKdc.createConf();
      kdc = new MiniKdc(kdcConf, baseDir);
      kdc.start();

      File serverKeytabFile = new File(baseDir, "server.keytab");
      File clientKeytabFile = new File(baseDir, "client.keytab");

      kdc.createPrincipal(serverKeytabFile, "server/localhost");
      kdc.createPrincipal(clientKeytabFile, "client/localhost");

      serverPrincipal = "server/localhost@" + kdc.getRealm();
      clientPrincipal = "client/localhost@" + kdc.getRealm();
      serverKeytab = serverKeytabFile.getAbsolutePath();
      clientKeytab = clientKeytabFile.getAbsolutePath();

      conf.set(SERVER_PRINCIPAL_KEY, serverPrincipal);
      conf.set(SERVER_KEYTAB_KEY, serverKeytab);
    }
  }

  public class SimpleBasedEnginePreparer implements EnginePreparer {
    public void prepareClient() {
      // Do nothing
    }

    public void prepareServer() {
      // Do nothing
    }

    public void cleanup() {
      // Do nothing
    }
  }

  private void startRPCServer() throws IOException {
    RPC.setProtocolEngine(conf, BenchmarkEngineProtocolPB.class, ProtobufRpcEngine.class);

    BenchmarkEngineProtocolServerSideTranslatorPB benchmarkEngineProtocolServerTranslator =
      new BenchmarkEngineProtocolServerSideTranslatorPB(new BenchmarkEngineProtocolImpl());
    BlockingService clientNNPbService = BenchmarkEngineProtocolProtos.BenchmarkEngineProtocolService.
      newReflectiveBlockingService(benchmarkEngineProtocolServerTranslator);

    server = new RPC.Builder(conf)
      .setProtocol(BenchmarkEngineProtocolPB.class)
      .setInstance(clientNNPbService)
      .setBindAddress(ADDRESS)
      .setPort(0)
      .setSecretManager(sm)
      .build();
    server.start();
  }

  private void stopRPCServer() {
    if (server != null) {
      server.stop();
    }
  }

  private void doTest() throws Exception {
    BenchmarkEngineProtocol proxy = null;

    try {
      // create a client
      proxy = getRPCProxy();

      byte[] payload = new byte[packetSize];
      new Random().nextBytes(payload);

      System.out.println("Mode: " + mode +", Packet size: " + packetSize +
        ", Packet number: " + packetNum);

      long start = System.currentTimeMillis();
      for (long iter = 0; iter < packetNum; iter++) {
        proxy.ping(payload);
      }
      long end = System.currentTimeMillis();

      System.out.println("time used (ms): " + (end - start));
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
  }

  private BenchmarkEngineProtocol getRPCProxy() throws Exception {
    final BenchmarkEngineProtocol proxy;
    final InetSocketAddress addr = NetUtils.getConnectAddress(server);

    if (clientUgi != null) {
      proxy = clientUgi.doAs(new PrivilegedExceptionAction<BenchmarkEngineProtocol>() {
        public BenchmarkEngineProtocol run() throws IOException {
          return new BenchmarkEngineProtocolClientSideTranslatorPB(addr, conf);
        }
      });
    } else {
      proxy = new BenchmarkEngineProtocolClientSideTranslatorPB(addr, conf);
    }

    return proxy;
  }

  public void testRPCPerformance() throws Exception {
    // Start RPC engine
    enginePreparer.prepareServer();
    startRPCServer();

    enginePreparer.prepareClient();
    doTest();

    // Stop RPC engine
    enginePreparer.cleanup();
    stopRPCServer();
  }

  private static final String USAGE = "Usage: run "
    + " [-mode simple|kerberos|token]\n\t"
    + " [-packetsize Size[B|KB|MB]\n\t"
    + " [-packetnum N]";

  enum ByteMultiple {
    B(1),
    KB(0x400),
    MB(0x100000);

    private int multiplier;

    ByteMultiple(int mult) {
      multiplier = mult;
    }

    int value() {
      return multiplier;
    }

    static ByteMultiple parseString(String sMultiple) {
      if(sMultiple == null || sMultiple.isEmpty()) // MB by default
        return MB;
      String sMU = StringUtils.toUpperCase(sMultiple);
      if(StringUtils.toUpperCase(B.name()).endsWith(sMU))
        return B;
      if(StringUtils.toUpperCase(KB.name()).endsWith(sMU))
        return KB;
      if(StringUtils.toUpperCase(MB.name()).endsWith(sMU))
        return MB;
      throw new IllegalArgumentException("Unsupported ByteMultiple "+sMultiple);
    }
  }

  private static int parseSize(String arg) {
    String[] args = arg.split("\\D", 2);  // get digits
    assert args.length <= 2;
    int nrBytes = Integer.parseInt(args[0]);
    String bytesMult = arg.substring(args[0].length()); // get byte multiple
    return nrBytes * ByteMultiple.parseString(bytesMult).value();
  }

  public static void main(String[] args) throws Exception {
    String mode = "simple";
    int packetSize = 1024; // default 1KB
    long packetNum = 1024 * 1024; // default 1M packets

    for (int i = 0; i < args.length; i++) { // parse command line
      if (StringUtils.toLowerCase(args[i]).startsWith("-mode")) {
        mode = args[++i];
      } else if (StringUtils.toLowerCase(args[i]).startsWith("-packetsize")) {
        packetSize = parseSize(args[++i]);
      } else if (StringUtils.toLowerCase(args[i]).startsWith("-packetnum")) {
        packetNum = Long.parseLong(args[++i]);
      } else {
        System.out.println(USAGE);
      }
    }
    HadoopRPCBenchmarkEngine engine = new HadoopRPCBenchmarkEngine(mode,
      packetSize, packetNum);
    engine.testRPCPerformance();
  }
}
