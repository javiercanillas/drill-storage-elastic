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

package org.apache.drill.exec.store.elasticsearch.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Sets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchConstants;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchStoragePlugin;
import org.apache.drill.exec.store.elasticsearch.JsonHelper;
import org.elasticsearch.client.Response;

import java.util.*;

public class ElasticSearchTypeMappingLoader extends CacheLoader<String, Collection<String>> {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchTypeMappingLoader.class);
    private final ElasticSearchStoragePlugin plugin;

    public ElasticSearchTypeMappingLoader(ElasticSearchStoragePlugin plugin)  {
        this.plugin = plugin;
    }

    @Override
    public Collection<String> load(String idxName) throws Exception {
        Set<String> typeMappings = Sets.newHashSet();
        try {
            Response response = this.plugin.getClient().performRequest("GET", "/" + idxName);
            JsonNode jsonNode = this.plugin.getObjectMapper().readTree(response.getEntity().getContent());
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                JsonNode mappings = JsonHelper.getPath(entry.getValue(), "mappings");
                if (!mappings.isMissingNode()) {
                    Iterator<String> aliasesIterator = mappings.fieldNames();
                    while (aliasesIterator.hasNext()) {
                        typeMappings.add(aliasesIterator.next());
                    }
                } else {
                    logger.warn("No typeMappings on {}", idxName);
                    //typeMappings.add(entry.getKey());
                }
            }
            return typeMappings;
        } catch (RuntimeException me) {
            logger.warn("Failure while loading typeMappings from ElasticSearch. {}",
                    me.getMessage());
            return Collections.emptyList();
        } catch (Exception e) {
            throw new DrillRuntimeException(e.getMessage(), e);
        }
    }
}
