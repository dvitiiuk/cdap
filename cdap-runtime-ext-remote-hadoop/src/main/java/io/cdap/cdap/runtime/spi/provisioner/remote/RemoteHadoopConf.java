/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.runtime.spi.provisioner.remote;

import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configuration for the Remote Hadoop provisioner.
 */
public class RemoteHadoopConf {
  private final SSHKeyPair sshKeyPair;
  private final String host;
  private final String initializationAction;
  private final String kerberosKeytabPath;
  private final String kerberosPrincipal;
  private final EdgeNodeCheckType edgeNodeCheckType;
  private final int timeout;

  private static final int DEFAULT_CHECK_TIMEOUT = 5000;

  private RemoteHadoopConf(SSHKeyPair sshKeyPair, String host, @Nullable String initializationAction,
                           @Nullable String kerberosPrincipal, @Nullable String kerberosKeytabPath,
                           EdgeNodeCheckType edgeNodeCheckType, Integer timeout) {
    this.sshKeyPair = sshKeyPair;
    this.host = host;
    this.initializationAction = initializationAction;
    this.kerberosKeytabPath = kerberosKeytabPath;
    this.kerberosPrincipal = kerberosPrincipal;
    this.edgeNodeCheckType = edgeNodeCheckType;
    this.timeout = timeout;
  }

  public SSHKeyPair getKeyPair() {
    return sshKeyPair;
  }

  public String getHost() {
    return host;
  }

  @Nullable
  public String getInitializationAction() {
    return initializationAction;
  }

  @Nullable
  public String getKerberosKeytabPath() {
    return kerberosKeytabPath;
  }

  @Nullable
  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  public EdgeNodeCheckType getEdgeNodeCheck() {
    return edgeNodeCheckType;
  }

  public int getTimeout() {
    return timeout;
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  public static RemoteHadoopConf fromProperties(Map<String, String> properties) {
    String host = getString(properties, "host");
    String user = getString(properties, "user");
    String privateKey = getString(properties, "sshKey");

    SSHKeyPair keyPair = new SSHKeyPair(new SSHPublicKey(user, ""),
                                        () -> privateKey.getBytes(StandardCharsets.UTF_8));

    EdgeNodeCheckType edgeNodeCheckType = EdgeNodeCheckType.getEdgeNodeCheck(properties.get("edgeNodeCheckMethod"));
    int checkTimeout = parseCheckTimeout(properties.get("checkTimeout"));

    return new RemoteHadoopConf(keyPair, host, properties.get("initializationAction"),
                                properties.get("kerberosPrincipal"),
                                properties.get("kerberosKeytabPath"),
      edgeNodeCheckType, checkTimeout);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
    }
    return val;
  }

  private static int parseCheckTimeout(String value) {
    try {
      return Integer.parseInt(value);
    } catch (Exception e) {
      return DEFAULT_CHECK_TIMEOUT;
    }
  }
}
