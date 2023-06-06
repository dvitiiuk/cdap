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

import javax.annotation.Nullable;

/**
 * {@link Event} implementation for generic incoming PubSub message.
 */
public class ReceivedEvent implements Event<ReceivedEventDetails> {

  private final long publishTime;
  private final String version;
  private final ReceivedEventDetails eventDetails;
  @Nullable
  private final String projectName;

  public ReceivedEvent(long publishTime, String version,
                       @Nullable String projectName, ReceivedEventDetails eventDetails) {
    this.publishTime = publishTime;
    this.version = version;
    this.projectName = projectName;
    this.eventDetails = eventDetails;
  }

  @Override
  public EventType getType() {
    return EventType.RECEIVED_EVENT;
  }

  @Override
  public long getPublishTime() {
    return publishTime;
  }

  @Override
  public String getVersion() {
    return version;
  }

  @Nullable
  @Override
  public String getInstanceName() {
    return null;
  }

  @Nullable
  @Override
  public String getProjectName() {
    return projectName;
  }


  @Override
  public ReceivedEventDetails getEventDetails() {
    return eventDetails;
  }

  @Override
  public String toString() {
    return "ReceivedEvent{"
            + "publishTime=" + publishTime
            + ", version='" + version + '\''
            + ", projectName='" + projectName + '\''
            + ", eventDetails=" + eventDetails
            + '}';
  }
}
