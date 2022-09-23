/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.common.base.Throwables;
import io.cdap.cdap.error.api.ErrorTagProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link RuntimeException} that wraps exceptions from Dataproc operation and provide a {@link #toString()}
 * implementation that doesn't include this exception class name and with the root cause error message.
 */
public class DataprocRuntimeException extends RuntimeException implements ErrorTagProvider {
  private final Set<ErrorTag> errorTags = new HashSet<>();

  public DataprocRuntimeException(String message, ErrorTag... tags) {
    super(message);
    errorTags.addAll(Arrays.asList(tags));
    errorTags.add(ErrorTag.DEPENDENCY);
  }

  public DataprocRuntimeException(Throwable cause, String message, ErrorTag... tags) {
    super(message, cause);
    errorTags.addAll(Arrays.asList(tags));
    errorTags.add(ErrorTag.DEPENDENCY);
  }

  public DataprocRuntimeException(Throwable cause, ErrorTag... tags) {
    this(cause, "", tags);
  }

  public DataprocRuntimeException(@Nullable String operationId, Throwable cause) {
    super(createMessage(operationId, cause), cause);
  }

  @Override
  public String toString() {
    return String.format("ErrorTags: %s,  Msg: %s", errorTags, getMessage() != null ? getMessage() : "");
  }

  private static String createMessage(Throwable cause) {
    return createMessage(null, cause);
  }

  private static String createMessage(@Nullable String operationId, Throwable cause) {
    if (operationId != null) {
      return String.format("Dataproc operation %s failure: %s",
                           operationId, Throwables.getRootCause(cause).getMessage());
    } else {
      return String.format("Dataproc operation failure: %s", Throwables.getRootCause(cause).getMessage());
    }
  }

  @Override
  public Set<ErrorTag> getErrorTags() {
    return Collections.unmodifiableSet(errorTags);
  }
}
