/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline argument to a triggered pipeline argument.
 */
public class ArgumentMapping {
  @Nullable
  private final String source;
  @Nullable
  private final String target;
  @Nullable
  private final TriggeringPipelineId pipelineId;

  public ArgumentMapping(@Nullable String source, @Nullable String target) {
    this(source, target, null);
  }

  public ArgumentMapping(@Nullable String source,
                         @Nullable String target,
                         @Nullable TriggeringPipelineId pipelineId) {
    this.source = source;
    this.target = target;
    this.pipelineId = pipelineId;
  }

  /**
   * @return The name of triggering pipeline argument
   */
  @Nullable
  public String getSource() {
    return source;
  }

  /**
   * @return The name of triggered pipeline argument
   */
  @Nullable
  public String getTarget() {
    return target;
  }

  /**
   * @return The identifiers of properties from the triggering pipeline.
   */
  @Nullable
  public TriggeringPipelineId getTriggeringPipelineId() {
    return pipelineId;
  }

  @Override
  public String toString() {
    return "ArgumentMapping{" +
      "source='" + getSource() + '\'' +
      ", target='" + getTarget() + '\'' +
      ", triggeringPipelineId='" + getTriggeringPipelineId() + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArgumentMapping)) {
      return false;
    }
    ArgumentMapping that = (ArgumentMapping) o;
    return Objects.equals(getSource(), that.getSource())
      && Objects.equals(getTarget(), that.getTarget())
      && Objects.equals(getTriggeringPipelineId(), that.getTriggeringPipelineId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSource(), getTarget(), getTriggeringPipelineId());
  }
}
