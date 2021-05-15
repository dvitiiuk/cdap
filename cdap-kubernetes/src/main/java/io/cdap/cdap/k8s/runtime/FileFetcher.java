/*
 * Copyright © 2021 Cask Data, Inc.
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
 */

package io.cdap.cdap.k8s.runtime;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Download file from AppFabric via internal REST API calls.
 * <p>
 * The target file could be either residing in AppFabric's local file system or a distributed file system
 * that AppFabric is configured to access.
 */
class FileFetcher {
  static final String SSL_KEYSTORE_TYPE = "JKS";
  static final String CERT_ALIAS = "cert";
  static final String APP_FABRIC_HTTP = "appfabric";
  // Default days of validity for certificates generated by this class.
  static final int VALIDITY = 999;
  private static final Logger LOG = LoggerFactory.getLogger(FileFetcher.class);
  DiscoveryServiceClient discoveryServiceClient;

  FileFetcher(DiscoveryServiceClient discoveryClient) {
    this.discoveryServiceClient = discoveryClient;
  }

  public static KeyStore createKeyStore(Path certificatePath, String password) {
    PrivateKey privateKey = null;
    List<Certificate> certificates = new ArrayList<>();

    BouncyCastleProvider provider = new BouncyCastleProvider();
    JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter().setProvider(provider);
    JcaX509CertificateConverter certConverter = new JcaX509CertificateConverter().setProvider(provider);
    char[] passPhase = password.toCharArray();

    try (PEMParser parser = new PEMParser(Files.newBufferedReader(certificatePath, StandardCharsets.ISO_8859_1))) {
      Object obj = parser.readObject();
      while (obj != null) {
        // Decrypt the key block if it is encrypted
        if (obj instanceof PEMEncryptedKeyPair) {
          obj = ((PEMEncryptedKeyPair) obj).decryptKeyPair(new BcPEMDecryptorProvider(passPhase));
        }

        // Set the private key if it is not set
        if (obj instanceof PEMKeyPair && privateKey == null) {
          privateKey = keyConverter.getKeyPair((PEMKeyPair) obj).getPrivate();
        } else if (obj instanceof X509CertificateHolder) {
          // Add the cert to the cert chain
          certificates.add(certConverter.getCertificate((X509CertificateHolder) obj));
        }

        obj = parser.readObject();
      }

      if (privateKey == null) {
        throw new RuntimeException("Missing private key from file " + certificatePath);
      }

      KeyStore keyStore = KeyStore.getInstance(SSL_KEYSTORE_TYPE);
      keyStore.load(null, passPhase);
      keyStore.setKeyEntry(CERT_ALIAS, privateKey, passPhase, certificates.toArray(new Certificate[0]));
      return keyStore;
    } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to create keystore from PEM file " + certificatePath, e);
    }
  }

  Discoverable pickRandom(ServiceDiscovered serviceDiscovered) {
    LOG.warn("wyzhang: serviceDiscovered = " + serviceDiscovered.toString());
    Discoverable result = null;
    Iterator<Discoverable> iter = serviceDiscovered.iterator();
    int count = 0;
    LOG.warn("wyzhang: serviceDiscovered iter start");
    while (iter.hasNext()) {
      LOG.warn("wyzhang: serviceDiscovered iter one iter");
      Discoverable next = iter.next();
      LOG.warn("wyzhang: discoverable = " + next.toString());
      if (ThreadLocalRandom.current().nextInt(++count) == 0) {
        result = next;
      }
    }
    LOG.warn("wyzhang: serviceDiscovered iter end");
    return result;
  }

  void downloadWithRetry(URI sourceURI, Location targetLocation)
    throws IOException, IllegalArgumentException, InterruptedException, NoSuchAlgorithmException,
    UnrecoverableKeyException, KeyStoreException, KeyManagementException {
    long initDelaySec = 5;
    long maxDeplySec = 30;
    long maxRetries = 5;
    int retries = 0;
    while (true) {
      try {
        LOG.warn("wyzhang: download start");
        download(sourceURI, targetLocation);
        LOG.warn("wyzhang: download succeeds");
        return;
      } catch (IOException e) {
        retries++;
        LOG.warn("wyzhang: download failed retries=" + retries + e.getMessage());
        e.printStackTrace();
        if (retries >= maxRetries) {
          throw e;
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(Math.min(initDelaySec * (1L << retries), maxDeplySec)));
      } catch (Exception e) {
        LOG.warn("wyzhang: download failed exception");
        throw e;
      }
    }
  }

  KeyManagerFactory createKeyManagerFactory()
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
    String path = "/etc/cdap/security";
    String password = "dbf4e70628f52b5e747aace1f59619ed";
    KeyStore keyStore = createKeyStore(Paths.get(path), password);

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, password.toCharArray());
    LOG.warn("wyzhang: keyManagerFactory created");
    return kmf;
  }

  SSLSocketFactory getSSLSocketFactory()
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException {
    SSLContext sslContext = SSLContext.getInstance("SSL");
    KeyManagerFactory kmf = createKeyManagerFactory();
    TrustManagerFactory tmf = InsecureTrustManagerFactory.INSTANCE;
    sslContext.init(kmf == null ? null : kmf.getKeyManagers(),
                    tmf == null ? null : tmf.getTrustManagers(),
                    new SecureRandom());
    LOG.warn("wyzhang: ssl socket factory created");
    return sslContext.getSocketFactory();
  }

  /**
   * Download a file from AppFabric and store it in the target file.
   *
   * @param sourceURI uri to identity the file to download. This URI should exist in AppFabric.
   * @param targetLocation target location to store the downloaded file
   * @throws IOException if file downloading or writing to target location fails.
   */
  void download(URI sourceURI, Location targetLocation)
    throws IOException, IllegalArgumentException, NoSuchAlgorithmException, UnrecoverableKeyException,
    KeyStoreException, KeyManagementException {
    Discoverable discoverable = pickRandom(discoveryServiceClient.discover(APP_FABRIC_HTTP));
    if (discoverable == null) {
      throw new IOException(String.format("service %s not found by discoveryService", APP_FABRIC_HTTP));
    }
    String scheme = URIScheme.getScheme(discoverable).scheme;
    LOG.warn("wyzhang: scheme " + scheme);
    InetSocketAddress address = discoverable.getSocketAddress();
    LOG.warn("wyzhang: address " + address.toString());
    URI uri = URI.create(String.format("%s://%s:%d/%s/%s",
                                       scheme, address.getHostName(), address.getPort(),
                                       "v3Internal/location", sourceURI.getPath()));
    URL url = uri.toURL();
    LOG.warn("wyzhang: url " + url.toString());
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setReadTimeout(15000);
    conn.setConnectTimeout(15000);
    conn.setChunkedStreamingMode(0);

    HttpsURLConnection httpsConn = ((HttpsURLConnection) conn);

    httpsConn.setSSLSocketFactory(getSSLSocketFactory());
    httpsConn.setHostnameVerifier((s, sslSession) -> true);
    LOG.warn("wyzhang: conn " + conn.toString());
    conn.connect();
    int responseCode = conn.getResponseCode();
    LOG.warn("wyzhang: resp code " + responseCode);
    if (responseCode != 200) {
      if (responseCode == 404) {
        throw new FileNotFoundException(conn.getResponseMessage());
      }
      if (responseCode == 400) {
        throw new IllegalArgumentException(conn.getResponseMessage());
      }
      throw new IOException(conn.getResponseMessage());
    }

    InputStream inputStream = conn.getInputStream();
    OutputStream outputStream = targetLocation.getOutputStream();

    byte[] buf = new byte[64 * 1024];
    int length;
    while ((length = inputStream.read(buf)) > 0) {
      outputStream.write(buf, 0, length);
    }
    outputStream.close();
  }

  enum URIScheme {
    HTTP("http", new byte[0], 80),
    HTTPS("https", "https://".getBytes(StandardCharsets.UTF_8), 443);

    private final String scheme;
    private final byte[] discoverablePayload;
    private final int defaultPort;


    URIScheme(String scheme, byte[] discoverablePayload, int defaultPort) {
      this.scheme = scheme;
      this.discoverablePayload = discoverablePayload;
      this.defaultPort = defaultPort;
    }

    public static URIScheme getScheme(Discoverable discoverable) {
      for (URIScheme scheme : values()) {
        if (scheme.isMatch(discoverable)) {
          return scheme;
        }
      }
      return HTTP;
    }

    public boolean isMatch(Discoverable discoverable) {
      return Arrays.equals(discoverablePayload, discoverable.getPayload());
    }
  }
}
