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

package io.cdap.cdap.sourcecontrol;

import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

/**
 * RepositoryManagerFactory interface which is used by Guice during runtime to create a RepositoryManager.
 */
public interface RepositoryManagerFactory {
  // TODO: RepositoryConfig is currently only accessible from the service layer
  //  Need to fix it and avoid passing it in RepositoryManagerFactory
  RepositoryManager create(NamespaceId namespace, RepositoryConfig repoConfig);
}
