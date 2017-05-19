/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.elasticsearch.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.elasticsearch.ElasticSearchConstants;
import org.apache.drill.exec.elasticsearch.ElasticSearchStoragePlugin;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;

import com.google.common.cache.CacheLoader;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

public class ElasticSearchTypeMappingLoader extends CacheLoader<String, List<String>> {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchTypeMappingLoader.class);
    private final ElasticSearchStoragePlugin plugin;

    public ElasticSearchTypeMappingLoader(ElasticSearchStoragePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public List<String> load(String key) throws Exception {
        if (!ElasticSearchConstants.INDEXES.equals(key)) {
            throw new UnsupportedOperationException();
        }
        try {
            //TODO: Is there a better way to only ask for type mapping for a single index?
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = plugin.getClient().admin().indices().getIndex(new GetIndexRequest()).actionGet().getMappings();
            ImmutableOpenMap<String, MappingMetaData> objectObjectCursors = mappings.get(key);
            if (objectObjectCursors != null) {
                return Arrays.asList(objectObjectCursors.keys().toArray(String.class));
            } else {
                throw new IllegalArgumentException("Index '"+key+"' type mappings not found.");
            }
        } catch (RuntimeException me) {
            logger.warn("Failure while loading type mapping from index {} from ElasticSearch. {}",
                    key, me.getMessage());
            return Collections.emptyList();
        } catch (Exception e) {
            throw new DrillRuntimeException(e.getMessage(), e);
        }
    }
}
