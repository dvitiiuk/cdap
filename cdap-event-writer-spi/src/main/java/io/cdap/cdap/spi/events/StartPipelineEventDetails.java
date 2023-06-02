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

package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class StartPipelineEventDetails {

  private final String appId;
  private final String namespaceId;
  private final String programType;
  private final String programId;
  @Nullable
  private final Map<String, String> userArgs;
  @Nullable
  private final Map<String, String> systemArgs;

  private StartPipelineEventDetails(String appId, String namespaceId, String programId,
                                    String programType,
                                    @Nullable Map<String, String> userArgs, @Nullable Map<String, String> systemArgs) {
    this.appId = appId;
    this.namespaceId = namespaceId;
    this.programId = programId;
    this.userArgs = userArgs;
    this.systemArgs = systemArgs;
    this.programType = programType;
  }


  public String getAppId() {
    return appId;
  }

  @Nullable
  public Map<String, String> getUserArgs() {
    return userArgs;
  }

  @Nullable
  public Map<String, String> getSystemArgs() {
    return systemArgs;
  }

  @Override
  public String toString() {
    return "ProgramStatusEventDetails{"
            + "appId='" + appId + '\''
            + ", namespaceId='" + namespaceId + '\''
            + ", programType='" + programType + '\''
            + ", programId='" + programId + '\''
            + ", userArgs=" + userArgs
            + ", systemArgs=" + systemArgs
            + '}';
  }

  public String getProgramId() {
    return programId;
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public String getProgramType() {
    return programType;
  }

}
