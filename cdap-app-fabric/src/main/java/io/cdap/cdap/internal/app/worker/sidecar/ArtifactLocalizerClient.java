/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.inject.Inject;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

/**
 * ArtifactLocalizerClient is used by tasks that extend {@link io.cdap.cdap.api.service.worker.RunnableTask} to fetch,
 * cache and unpack artifacts locally.
 */
public class ArtifactLocalizerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizerClient.class);
  private final String sidecarBaseURL;
  private HashMap<ArtifactId, File> artifactCache = new HashMap<>();
  private HashMap<ArtifactId, File> unpackedArtifactCache = new HashMap<>();

  @Inject
  ArtifactLocalizerClient(CConfiguration cConf) {
    this.sidecarBaseURL = String
      .format("http://localhost:%d/%s/worker", cConf.getInt(Constants.ArtifactLocalizer.PORT),
              Constants.Gateway.INTERNAL_API_VERSION_3_TOKEN);
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching the artifact as well
   * as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @return The Local Location for this artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching or caching the artifact
   */
  public File getArtifactLocation(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    if (artifactCache.containsKey(artifactId)) {
      File file = artifactCache.get(artifactId);
      LOG.debug("Path for {} was already fetched during this session, returning path from in-memory cache: {}",
                artifactId, file.getPath());
      return file;
    }
    File file = sendRequest(artifactId, false);
    artifactCache.put(artifactId, file);
    return file;
  }

  /**
   * Gets the location on the local filesystem for the directory that contains the unpacked artifact. This method
   * handles fetching, caching and unpacking the artifact.
   *
   * @param artifactId The ArtifactId of the artifact to fetch and unpack
   * @return The Local Location of the directory that contains the unpacked artifact files
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching, caching or unpacking the artifact
   */
  public File getUnpackedArtifactLocation(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
    if (unpackedArtifactCache.containsKey(artifactId)) {
      File file = unpackedArtifactCache.get(artifactId);
      LOG.debug("Unpacked path for {} was already fetched during this session, returning path from in-memory cache: {}",
                artifactId, file.getPath());
    }
    File file = sendRequest(artifactId, true);
    unpackedArtifactCache.put(artifactId, file);
    return file;
  }

  private File sendRequest(ArtifactId artifactId, boolean unpack) throws IOException, ArtifactNotFoundException {
    String urlPath = String
      .format("/artifact/namespaces/%s/artifacts/%s/versions/%s?unpack=%b", artifactId.getNamespace(),
              artifactId.getArtifact(),
              artifactId.getVersion(), unpack);
    URL url;
    try {
      url = new URI(sidecarBaseURL + urlPath).toURL();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    LOG.debug("Sending request to {}", url);
    HttpRequest httpRequest = HttpRequest.builder(HttpMethod.GET, url).build();
    HttpResponse httpResponse = HttpRequests.execute(httpRequest);

    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        throw new ArtifactNotFoundException(artifactId);
      }
      String errorMsg = httpResponse.getResponseBodyAsString();
      throw new IOException(errorMsg);
    }

    String path = httpResponse.getResponseBodyAsString();
    LOG.debug("ArtifactLocalizer request returned path {}", path);

    return new File(path);
  }
}
