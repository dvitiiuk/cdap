/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.spi.events;

import java.util.Map;
import javax.annotation.Nullable;

public class ReceivedEventDetails {
  private final String data;
  private final String ackId;
  @Nullable
  private final Map<String, String> attributes;

  private ReceivedEventDetails(String data, String ackId, @Nullable Map<String, String> attributes) {
    this.data = data;
    this.ackId = ackId;
    this.attributes = attributes;
  }

  public static Builder getBuilder(String data, String ackId) {
    return new Builder(data, ackId);
  }

  public String getData() {
    return data;
  }

  public String getAckId() {
    return ackId;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    String result = "Event data: " + data + ", ACK ID: " + ackId;

    if (attributes == null) {
      return result;
    }

    StringBuilder attributeBuilder = new StringBuilder();
    for (Map.Entry<String, String> e : attributes.entrySet()) {
      attributeBuilder.append(e.getKey() + ":" + e.getValue());
    }
    return result + "\n" + String.join("\n", attributeBuilder);
  }

  public static class Builder {
    private final String data;
    private final String ackId;
    private Map<String, String> attributes;

    Builder(String data, String ackId) {
      this.data = data;
      this.ackId = ackId;
    }

    public Builder withAttributes(Map<String, String> attributes) {
      this.attributes = attributes;
      return this;
    }

    public ReceivedEventDetails build() {
      return new ReceivedEventDetails(data, ackId, attributes);
    }
  }


}
