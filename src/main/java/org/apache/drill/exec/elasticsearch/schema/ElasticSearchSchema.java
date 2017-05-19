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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.elasticsearch.ElasticSearchConstants;
import org.apache.drill.exec.elasticsearch.ElasticSearchPluginConfig;
import org.apache.drill.exec.elasticsearch.ElasticSearchScanSpec;
import org.apache.drill.exec.elasticsearch.ElasticSearchStoragePlugin;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ElasticSearchSchema extends AbstractSchema {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchSchema.class);
    private final Map<String, ElasticSearchIndexSchema> schemaMap = Maps.newHashMap();
    private final ElasticSearchStoragePlugin plugin;

    public ElasticSearchSchema(String name, ElasticSearchStoragePlugin plugin)
    {
        super(ImmutableList.<String> of(), name);
        this.plugin = plugin;
    }

    @Override
    public String getTypeName() {
        return ElasticSearchPluginConfig.NAME;
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
        List<String> typeMappings;
        try {
            if ( !this.schemaMap.containsKey(name)){
                typeMappings = this.plugin.getSchemaFactory().getTypeMappingCache().get(name);
                this.schemaMap.put(name, new ElasticSearchIndexSchema(typeMappings, this, name));
            }

            return this.schemaMap.get(name);
        } catch (ExecutionException e) {
            logger.warn("Failure while attempting to access ElasticSearch Index '{}'.",
                    name, e.getCause());
            return null;
        }
    }

    void setHolder(SchemaPlus plusOfThis) {
        for (String s : getSubSchemaNames()) {
            plusOfThis.add(s, getSubSchema(s));
        }
    }

    @Override
    public boolean showInInformationSchema() {
        return false;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        try {
            List<String> dbs = this.plugin.getSchemaFactory().getIndexCache().get(ElasticSearchConstants.INDEXES);
            return Sets.newHashSet(dbs);
        } catch (ExecutionException e) {
            logger.warn("Failure while getting ElasticSearch index list.", e);
            return Collections.emptySet();
        }
    }

    public DrillTable getDrillTable(String name, String tableName) {
        ElasticSearchScanSpec elasticSearchScanSpec = new ElasticSearchScanSpec(name, tableName);
        return new DynamicDrillTable(this.plugin, this.plugin.getSchemaFactory().getSchemaName(), null, elasticSearchScanSpec);
    }
}