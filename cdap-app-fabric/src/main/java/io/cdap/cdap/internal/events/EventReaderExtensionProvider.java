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

package io.cdap.cdap.internal.events;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.spi.events.PubSubEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link EventWriterProvider} which provides Event writer extension classes
 * extending from {@link AbstractExtensionLoader}
 */
public class EventReaderExtensionProvider extends AbstractExtensionLoader<String, PubSubEventReader>{

  private static final Logger LOG = LoggerFactory.getLogger(EventReaderExtensionProvider.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);
  private Collection<String> enabledEventReaders;

  @Inject
  public EventReaderExtensionProvider(CConfiguration cConf) {
    super(cConf.get(Constants.Event.EVENTS_READER_EXTENSIONS_DIR) != null
        ? cConf.get(Constants.Event.EVENTS_READER_EXTENSIONS_DIR) : "");
    this.enabledEventReaders = cConf.getStringCollection(
        Constants.Event.EVENTS_READER_EXTENSIONS_ENABLED_LIST);
    if (this.enabledEventReaders == null || this.enabledEventReaders.isEmpty()) {
      LOG.debug("No event readers enabled.");
      return;
    }
    LOG.debug("Enabled event readers are {} .", enabledEventReaders);
  }

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(PubSubEventReader.class.getClassLoader(),
          PubSubEventReader.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for writer extension. "
          + "Usage of events writer might fail.", e);
    }
  }

  public Map<String, PubSubEventReader> loadEventReaders() {
    return getAll();
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(PubSubEventReader pubSubEventReader) {
    if (enabledEventReaders == null || !enabledEventReaders.contains(pubSubEventReader.getID())) {
      LOG.debug("{} is not present in the enabled list of event writers.", pubSubEventReader.getID());
      return Collections.emptySet();
    }

    return Collections.singleton(pubSubEventReader.getID());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only allow spi classes.
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return ALLOWED_RESOURCES.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return ALLOWED_PACKAGES.contains(packageName);
      }
    };
  }
}
